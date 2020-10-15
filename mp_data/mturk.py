from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List
from urllib.parse import urlparse

import boto3
import click
import dateutil.parser
import pandas as pd
import yaml
import regex
from bs4 import BeautifulSoup
from dateutil.tz import gettz

from mp_data import cli, repo_root
from mp_data.db import get_session, HIT, Assignment

REGION_NAME = "us-east-1"


_global_client = None


@cli.group()
def mturk():
    pass


@mturk.command()
def ls():
    """List batches in Mturk"""
    batch_names = set()
    for hit in _iter_hits():
        batch_name = hit["RequesterAnnotation"]
        batch_names.add(batch_name)
    for bn in sorted(batch_names):
        click.echo(bn)


@mturk.command()
@click.argument("batch_name")
def rm(batch_name: str):
    """Remove HITs for a batch"""
    client = _get_client()
    n = 0
    for hit in _iter_batch_hits(batch_name):
        client.delete_hit(HITId=hit["HITId"])
        n += 1
    click.echo(f"Deleted {n} HITs for batch: {batch_name}")


@mturk.command()
@click.option(
    "-f",
    "--force",
    is_flag=True,
    help="Delete all HITs regardless whether they've been imported or not.",
)
def prune(force: bool):
    """Remove HITs for batches from Mturk whose assignments
    have been already imported to the DB."""
    session = get_session()
    imported_batches = set()
    for a in session.query(Assignment):
        imported_batches.add(a.hit.batch_name)

    client = _get_client()
    n = 0
    ignore_statuses = {"Assignable", "Unassignable"}
    for hit in _iter_hits():
        hit_id = hit["HITId"]
        batch_name = hit["RequesterAnnotation"]
        hit_status = hit["HITStatus"]
        should_delete = force or batch_name in imported_batches
        if should_delete and hit_status not in ignore_statuses:
            client.delete_hit(HITId=hit_id)
            n += 1
    click.echo(f"Pruned {n} HITs.")


@mturk.command()
@click.argument("data_dir", type=Path)
def create(data_dir: Path):
    """Create MTurk batch."""

    mturk_dir = repo_root / "data" / "mturk"
    task_def = load_yaml(mturk_dir / "hit.yml")

    session = get_session()

    now = datetime.utcnow().strftime("%Y-%m-%d_%H_%M_%S")
    batch_name = f"batch_{now}"
    task_def["RequesterAnnotation"] = batch_name

    hits = []
    success_count = 0
    failed_urls = []
    for url in _load_urls(data_dir):
        try:
            hits.append(_create_hit(task_def, url))
            success_count += 1
        except Exception as e:
            click.echo(
                f"Creating HIT for url {url} failed with exception:\n{e}", err=True
            )
            failed_urls.append(url)

    session.add_all(hits)
    session.commit()

    click.echo(f"Created {success_count} HITs for batch: {batch_name}")
    if len(failed_urls) > 0:
        click.echo("Creating a HIT for the following urls failed:", err=True)
        for u in failed_urls:
            click.echo(u, err=True)


@mturk.command()
@click.argument("hit_type_def_path", type=Path)
def create_hit_type(hit_type_def_path: Path):
    """Create HIT type."""
    hit_type_def = load_yaml(hit_type_def_path)
    client = _get_client()
    res = client.create_hit_type(**hit_type_def)
    hit_type_id = res["HITTypeId"]
    click.echo(
        f"Created new hit type from '{hit_type_def_path}' with id: {hit_type_id}"
    )


@mturk.command()
@click.argument("batch_name")
def fetch(batch_name: str):
    """Fetch assignments from Mturk and save them to the db"""
    session = get_session()

    assignments = []
    for hit in _iter_batch_hits(batch_name):
        for assignment in _iter_assignments(hit["HITId"]):
            assignments.append(_create_assignment(assignment))

    session.add_all(assignments)
    session.commit()

    click.echo(f"Exported {len(assignments)} assignments to DB for batch: {batch_name}")


@mturk.command()
def approve_all():
    """Approve all assignements"""
    client = _get_client()
    n = 0
    for hit in _iter_hits():
        for a in _iter_assignments(hit["HITId"], assignment_statuses=["Submitted"]):
            client.approve_assignment(AssignmentId=a["AssignmentId"])
            n += 1
    click.echo(f"Approved {n} assignments.")


@mturk.command()
@click.argument("csv_path", type=Path)
def import_data(csv_path: Path):
    """Import HIts and assignments from CSV from MTurk GUI."""
    df = pd.read_csv(csv_path)

    hits = {}
    assignments = []
    for row in df.to_dict(orient="records"):
        url = row["Input.url"]
        domain_name = urlparse(url).netloc
        hit_id = row["HITId"]
        hit = HIT(
            hit_id=hit_id,
            creation_time=_parse_time(row["CreationTime"]),
            batch_name=csv_path.stem,
            url_param=url,
            domain_name=domain_name,
        )
        if hit_id not in hits:
            hits[hit_id] = hit
        try:
            in_stock = row["Answer.available.available"] == "true"
        except KeyError:
            in_stock = row["Answer.in-stock.in-stock"] == "true"
        assignment_id = row["AssignmentId"]
        raw_price = row["Answer.price"]
        quantity = _parse_quantity(row["Answer.quantity"])
        price, currency = _parse_price_currency(raw_price, in_stock, assignment_id)
        assignment = Assignment(
            hit_id=hit_id,
            assignment_id=row["AssignmentId"],
            accept_time=_parse_time(row["AcceptTime"]),
            submit_time=_parse_time(row["SubmitTime"]),
            in_stock=in_stock,
            price=price,
            currency=currency,
            quantity=quantity,
        )
        assignments.append(assignment)

    session = get_session()
    session.add_all(list(hits.values()) + assignments)
    session.commit()

    click.echo(f"Imported {len(assignments)} assignments to DB from: {csv_path}")


def _create_assignment(assignment_dict):
    assignment_id = assignment_dict["AssignmentId"]

    answer_xml = assignment_dict["Answer"]
    soup = BeautifulSoup(answer_xml, "lxml")
    answers = soup.html.body.questionformanswers.find_all("answer")
    results = {}
    for ans in answers:
        question_name = "-".join(ans.questionidentifier.contents)
        answer_text = "".join(ans.freetext.contents)
        results[question_name] = answer_text

    quantity = _parse_quantity(results.get("quantity"))
    in_stock = results["in-stock.in-stock"] == "true"
    raw_price = results.get("price")
    price, currency = _parse_price_currency(raw_price, in_stock, assignment_id)

    assignment = Assignment(
        hit_id=assignment_dict["HITId"],
        assignment_id=assignment_id,
        accept_time=assignment_dict["AcceptTime"],
        submit_time=assignment_dict["SubmitTime"],
        in_stock=in_stock,
        price=price,
        currency=currency,
        quantity=quantity,
    )

    return assignment


def _parse_quantity(raw_quantity: Optional[str]) -> Optional[int]:
    try:
        return int(raw_quantity)
    except (TypeError, ValueError):
        return None


def _parse_price_currency(raw_price: Optional[str], in_stock: bool, assignment_id: str):
    if raw_price is None:
        return None, None
    curr_match = regex.search(r"^\p{Sc}|\p{Sc}$", raw_price)
    if in_stock and not curr_match:
        raise ValueError(f"Invalid price for assignment {assignment_id}: '{raw_price}'")
    elif curr_match:
        if curr_match.start() == 0:
            price = _clean_price(raw_price[curr_match.end() :])
        else:
            price = _clean_price(raw_price[: curr_match.start()])
        currency = curr_match.group()
    else:
        price = None
        currency = None
    return price, currency


def _parse_time(times: str) -> datetime:
    tzinfos = {"PDT": gettz("America/Los Angeles")}
    return dateutil.parser.parse(times, tzinfos=tzinfos).astimezone(timezone.utc)


def _clean_price(num_str):
    # Strip thousand separators
    res = regex.sub(r"[\s,.](?=[0-9]{3,})", "", num_str)
    # Replace decimal comma with dot if present
    res = regex.sub(r",[0-9]{1,2}$", ".", res)
    try:
        return round(float(res) * 100)
    except ValueError:
        return None


def _get_client():
    global _global_client
    if _global_client is None:
        _global_client = boto3.client("mturk", region_name=REGION_NAME,)
    return _global_client


def _iter_hits():
    client = _get_client()
    paginator = client.get_paginator("list_hits")
    page_iterator = paginator.paginate()
    for page in page_iterator:
        yield from page["HITs"]


def _iter_assignments(hit_id: str, assignment_statuses: Optional[List[str]] = None):
    client = _get_client()
    paginator = client.get_paginator("list_assignments_for_hit")
    args = {"HITId": hit_id}
    if assignment_statuses:
        args["AssignmentStatuses"] = assignment_statuses
    page_iterator = paginator.paginate(**args)
    for page in page_iterator:
        yield from page["Assignments"]


def _iter_batch_hits(batch_name):
    yield from filter(
        lambda hit: hit["RequesterAnnotation"] == batch_name, _iter_hits()
    )


def _load_urls(urls_dir: Path):
    for fp in urls_dir.glob("*.csv"):
        df = pd.read_csv(fp)
        for _, row in df.iterrows():
            yield row[0]


def _create_hit(task_def, url):
    client = _get_client()
    hit_params = [{"Name": "url", "Value": url}]
    hit_args = {**task_def, "HITLayoutParameters": hit_params}
    response = client.create_hit_with_hit_type(**hit_args)
    h = response["HIT"]
    domain_name = urlparse(url).netloc
    return HIT(
        hit_id=h["HITId"],
        creation_time=h["CreationTime"],
        batch_name=h["RequesterAnnotation"],
        url_param=url,
        domain_name=domain_name,
    )


def load_yaml(path: Path):
    with open(path) as infile:
        return yaml.safe_load(infile)
