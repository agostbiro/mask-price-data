import click
from pathlib import Path


repo_root = Path(__file__).parents[1]


@click.group()
def cli(**kwargs):
    pass
