import os
import click
from deepracer_race_stats.constants import RAW_DATA_LEADERBOARDS_FOLDER, RAW_DATA_TRACK_FOLDER

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
def track_update(ctx):
    """Updates the available data on race tracks.

    Args:
        ctx: Click context
    """

    response = list_tracks()
    output_path = os.path.join(RAW_DATA_TRACK_FOLDER, "tracks.csv")

    boto_response_to_csv(response, output_path)


@cli.command()
@click.pass_context
def leaderboards_update(ctx):
    """Updates the available data on race tracks.

    Args:
        ctx: Click context
    """

    response = list_leaderboards()
    output_path = os.path.join(RAW_DATA_LEADERBOARDS_FOLDER, "leaderboards.csv")

    boto_response_to_csv(response, output_path)


@cli.command()
@click.option("-l", "--leaderboard-arn", required=True)
@click.pass_context
def test_leaderboard(ctx, leaderboard_arn):
    response = list_leaderboard(leaderboard_arn)

    boto_response_to_csv(response, "leaderboard.csv", drop_columns=["SubmissionVideoS3path"])
