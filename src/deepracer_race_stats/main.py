import os
import click
import requests

from deepracer_race_stats.constants import (
    RAW_DATA_ASSETS_LEADERBOARDS_FOLDER,
    RAW_DATA_ASSETS_TRACK_FOLDER,
    RAW_DATA_LEADERBOARDS_FOLDER,
    RAW_DATA_TRACK_FOLDER,
)

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

    # Specific for tracks, we also collect the assets.
    for r in response:
        try:
            track_name = r["TrackName"]
            track_picture_url = r["TrackPicture"]

            _, output_ext = os.path.splitext(track_picture_url)
            output_path = os.path.join(RAW_DATA_ASSETS_TRACK_FOLDER, "{}{}".format(track_name, output_ext))

            r = requests.get(track_picture_url)
            if r.status_code == 200:
                with open(output_path, "wb") as f:
                    f.write(r.content)
        except Exception:
            # Assume we can't get it then.
            pass


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

    # Specific for leaderboards, we also collect the assets.
    for r in response:
        try:
            leaderboard_name = r["Name"]
            leaderboard_image_url = r["ImageUrl"]

            print(leaderboard_image_url)

            _, output_ext = os.path.splitext(leaderboard_image_url)
            output_path = os.path.join(RAW_DATA_ASSETS_LEADERBOARDS_FOLDER, "{}{}".format(leaderboard_name, output_ext))

            r = requests.get(leaderboard_image_url)
            if r.status_code == 200:
                with open(output_path, "wb") as f:
                    f.write(r.content)
        except Exception as e:
            # Assume we can't get it then.
            pass


@cli.command()
@click.option("-l", "--leaderboard-arn", required=True)
@click.pass_context
def test_leaderboard(ctx, leaderboard_arn):
    response = list_leaderboard(leaderboard_arn)

    boto_response_to_csv(response, "leaderboard.csv", drop_columns=["SubmissionVideoS3path"])
