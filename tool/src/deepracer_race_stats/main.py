import click

from deepracer_race_stats.util.deepracer_service import list_leaderboards


@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)

    # Set context flags here (like env vars?)


@cli.command()
@click.pass_context
def test(ctx):
    print(list_leaderboards())
