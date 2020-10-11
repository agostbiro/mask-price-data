import itertools
import shutil
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, date
from operator import attrgetter
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import click
import iso4217parse
import numpy as np
import pandas as pd
from currency_converter import CurrencyConverter
from scipy import stats

from mp_data import cli
from mp_data.db import get_session, Assignment


# Observations with unit prices this many standard
# deviations away from the mean are removed.
# We assume that unit prices in an online marketplace
# are close to uniform, hence the low value.
OUTLIER_THRESHOLD = 1
MIN_MATCHING_RESULT = 2


_currency_converter = None


def _get_currency_converter() -> CurrencyConverter:
    global _currency_converter
    if _currency_converter is None:
        # Fetch latest currency data
        _currency_converter = CurrencyConverter(
            "https://www.ecb.int/stats/eurofxref/eurofxref-hist.zip",
            fallback_on_missing_rate=True,
            fallback_on_missing_rate_method="last_known",
            fallback_on_wrong_date=True,
        )
    return _currency_converter


@dataclass()
class _Observation:
    datetime: datetime
    price_cents: int
    currency_symbol: str
    quantity: int
    in_stock: bool
    domain_name: str
    url: str
    uuid: str = field(init=False)

    def __post_init__(self):
        self.uuid = uuid.uuid4().hex

    @property
    def price(self):
        return self.price_cents / 100

    @property
    def date(self) -> date:
        return self.datetime.date()

    @property
    def date_str(self) -> str:
        return self.date.strftime("%Y-%m-%d")

    @property
    def iso_currency(self) -> str:
        if self.currency_symbol == "$":
            # Dollar sign is ambiguous, assume it's USD
            return "USD"
        else:
            return iso4217parse.parse(self.currency_symbol)[0].alpha3

    @property
    def price_usd(self) -> float:
        if self.iso_currency == "USD":
            return self.price
        else:
            cc = _get_currency_converter()
            return cc.convert(self.price, self.iso_currency, "USD", date=self.date)

    @property
    def unit_price_usd(self) -> float:
        return self.price_usd / self.quantity

    @property
    def marketplace(self) -> str:
        prefix = "www."
        if self.domain_name.startswith(prefix):
            name = self.domain_name[len(prefix) :]
        else:
            name = self.domain_name
        return name.replace(".", "_")


@cli.group()
def data():
    pass


@data.command()
@click.argument("out_dir", type=Path)
def export(out_dir: Path):
    assignments_dict = _fetch_assignments()
    all_observations = []
    for assignments in assignments_dict.values():
        res = _consolidate_assignments(assignments)
        if res is not None:
            all_observations.append(res)
    filtered_observations = _filter_outliers(all_observations)

    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)

    _export_timeseries(filtered_observations, out_dir)
    _export_latest_observations(filtered_observations, out_dir)

    click.echo(f"Exported data to {out_dir}")


def _fetch_assignments() -> Dict[str, List[Assignment]]:
    session = get_session()
    assignments = defaultdict(list)
    for a in session.query(Assignment):
        assignments[a.hit_id].append(a)
    return assignments


def _consolidate_assignments(
    assignments: Sequence[Assignment],
) -> Optional[_Observation]:
    if len(assignments) == 0:
        return None
    count = defaultdict(int)
    for a in assignments:
        t = (a.price, a.quantity, a.currency, a.in_stock)
        count[t] += 1
    max_count = max(count.values())
    if max_count >= MIN_MATCHING_RESULT:
        majority = next(k for k in count if count[k] == max_count)
        # Price is stored in db as int representing cents
        price, quantity, currency, in_stock = majority
        # Heuristic to filter out nonsense output from MTurk
        if not _check_legit_quantity(quantity) or price == 0:
            return None
        hit = assignments[0].hit
        return _Observation(
            datetime=hit.creation_time,
            price_cents=price,
            currency_symbol=currency,
            quantity=quantity,
            in_stock=in_stock,
            domain_name=hit.domain_name,
            url=hit.url_param,
        )
    else:
        return None


def _check_legit_quantity(quantity: int):
    return 5 <= quantity <= 500 and quantity % 5 == 0


def _export_timeseries(observations: Sequence[_Observation], out_dir: Path):
    daily_unit_prices = defaultdict(list)
    for o in observations:
        key = (o.date_str, o.marketplace)
        daily_unit_prices[key].append(o.unit_price_usd)
    daily_medians = defaultdict(list)
    for (date_str, marketplace), unit_prices in daily_unit_prices.items():
        daily_medians[marketplace].append(
            {"Date": date_str, "Median_Unit_Price_$": np.median(unit_prices)}
        )
    for k, v in daily_medians.items():
        df = pd.DataFrame(v).sort_values(by=["Date"])
        out_path = out_dir / f"{k}_timeseries.csv"
        df.to_csv(out_path, index=False)


def _export_latest_observations(observations: Sequence[_Observation], out_dir: Path):
    marketplaces = set(o.marketplace for o in observations)
    for m in marketplaces:
        _export_latest_marketplace(observations, out_dir, m)


def _export_latest_marketplace(
    all_observations: Sequence[_Observation], out_dir: Path, marketplace: str
):
    observations = list(
        filter(lambda o: o.marketplace == marketplace, all_observations)
    )
    latest_date = sorted(o.date_str for o in observations)[-1]
    items = []
    unit_price_key = f"Unit_Price_$"
    for o in observations:
        if o.date_str == latest_date:
            item = {"Url": o.url, unit_price_key: o.unit_price_usd}
            items.append(item)
    df = pd.DataFrame(items).sort_values(by=[unit_price_key])
    df.to_csv(out_dir / f"{marketplace}_latest.csv", index=False)


def _filter_outliers(observations: Sequence[_Observation]) -> Sequence[_Observation]:
    marketplaces = set(map(attrgetter("marketplace"), observations))
    dates = set(map(attrgetter("date_str"), observations))
    results = []
    for m, d in itertools.product(marketplaces, dates):
        curr_observations = list(filter(lambda o: o.marketplace == m and o.date_str == d, observations))
        indices = list(map(attrgetter("uuid"), curr_observations))
        unit_prices = list(map(attrgetter("unit_price_usd"), curr_observations))
        # Can't decide if there are outliers with fewer than 3 observations.
        if len(indices) < 3:
            results.extend(curr_observations)
            continue
        df = pd.DataFrame(unit_prices, index=indices)
        condition = (np.abs(stats.zscore(df)) < OUTLIER_THRESHOLD).all(axis=1)
        allowed_uuids = set(df[condition].index)
        for o in curr_observations:
            if o.uuid in allowed_uuids:
                results.append(o)
    return results
