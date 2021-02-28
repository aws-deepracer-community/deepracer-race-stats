import click

from deepracer_race_stats.util.boto3_util import get_boto_deepracer_client


@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)

    # Set context flags here (like env vars?)


@cli.command()
@click.pass_context
def test(ctx):
    client = get_boto_deepracer_client()

    response = client.list_leaderboards(MaxResults=100)

    print(response)