import os
import click

from datetime import datetime
from joblib import Parallel, delayed
from deepracer_race_stats.constants import (
    LEADERBOARDS_CSV_FILEPATH,
    LEADERBOARDS_FOLDER_ASSETS,
    LEADERBOARD_FOLDER,
    TRACK_CSV_FILEPATH,
    TRACK_FOLDER_ASSETS,
)

from deepracer_race_stats.util.csv_util import boto_response_to_csv
from deepracer_race_stats.util.deepracer_service import (
    list_leaderboard,
    list_leaderboards,
    list_tracks,
)
from deepracer_race_stats.util.media import fetch_media_assets


@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)

    # Set context flags here (like env vars?)


@cli.command()
@click.option("-o", "--output-folder", required=True)
@click.pass_context
def track_update(ctx, output_folder):
    """Updates the available data on race tracks.

    Args:
        ctx: Click context
    """

    response = list_tracks()
    output_path = os.path.join(output_folder, TRACK_CSV_FILEPATH)

    boto_response_to_csv(response, output_path)

    asset_map = {r["TrackArn"]: r["TrackPicture"] for r in response}

    output_assets_folder = os.path.join(output_folder, TRACK_FOLDER_ASSETS)

    fetch_media_assets(asset_map, output_assets_folder)


@cli.command()
@click.option("-o", "--output-folder", required=True)
@click.pass_context
def leaderboard_update(ctx, output_folder):
    """Updates the available data on race tracks.

    Args:
        ctx: Click context
    """

    response = list_leaderboards()
    output_path = os.path.join(output_folder, LEADERBOARDS_CSV_FILEPATH)

    boto_response_to_csv(response, output_path)

    asset_map = {r["Arn"]: r["ImageUrl"] for r in response if "ImageUrl" in r}
    output_assets_folder = os.path.join(output_folder, LEADERBOARDS_FOLDER_ASSETS)

    fetch_media_assets(asset_map, output_assets_folder)

    # Now do an update for each unique ARN:
    # - If OPEN: We collect a snapshot and save it under the current data and time.
    # - If CLOSED: We assume it is final and store it as FINAL.csv -> Any update to this will be version controlled.

    def update(leaderboard_arn, status):
        leaderboard_output_folder = os.path.join(output_folder, LEADERBOARD_FOLDER, leaderboard_arn)

        if not os.path.exists(leaderboard_output_folder):
            os.makedirs(leaderboard_output_folder, exist_ok=True)

        if status == "OPEN":
            now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
            # Output filename of the nearest hour (2021-01-01 01:00:00.csv)
            output_file = os.path.join(leaderboard_output_folder, "{}.csv".format(now.isoformat()))
            response = list_leaderboard(leaderboard_arn)
        else:
            output_file = os.path.join(leaderboard_output_folder, "FINAL.csv")
            response = list_leaderboard(leaderboard_arn)

        # Video S3 column is too large and will not work anyways so we drop it.
        boto_response_to_csv(response, output_file, drop_columns=["SubmissionVideoS3path"])

    Parallel(n_jobs=2, prefer="threads")(delayed(update)(r["Arn"], r["Status"]) for r in response)
