import click

from deepracer_race_stats.util.csv_util import boto_response_to_csv
from deepracer_race_stats.util.deepracer_service import (
    list_leaderboard,
    list_leaderboards,
    list_tracks,
)


@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)

    # Set context flags here (like env vars?)


@cli.command()
@click.pass_context
def test_leaderboards(ctx):
    response = list_leaderboards()

    boto_response_to_csv(response, "leaderboards.csv")


@cli.command()
@click.pass_context
def test_tracks(ctx):
    response = list_tracks()

    boto_response_to_csv(response, "tracks.csv")


@cli.command()
@click.option("-l", "--leaderboard-arn", required=True)
@click.pass_context
def test_leaderboard(ctx, leaderboard_arn):
    response = list_leaderboard(leaderboard_arn)

    boto_response_to_csv(response, "leaderboard.csv", drop_columns=["SubmissionVideoS3path"])
