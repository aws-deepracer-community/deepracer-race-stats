import os
import click
import tarfile
from datetime import datetime
from joblib import Parallel, delayed

from deepracer_race_stats.constants import (
    RAW_DATA_ASSETS_LEADERBOARDS_FOLDER,
    RAW_DATA_ASSETS_TRACK_FOLDER,
    RAW_DATA_FOLDER,
    RAW_DATA_LEADERBOARDS_FOLDER,
    RAW_DATA_LEADERBOARD_FOLDER,
    RAW_DATA_TRACK_FOLDER,
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
@click.pass_context
def track_update(ctx):
    """Updates the available data on race tracks.

    Args:
        ctx: Click context
    """

    response = list_tracks()
    output_path = os.path.join(RAW_DATA_TRACK_FOLDER, "tracks.csv")

    boto_response_to_csv(response, output_path)

    asset_map = {r["TrackArn"]: r["TrackPicture"] for r in response}
    fetch_media_assets(asset_map, RAW_DATA_ASSETS_TRACK_FOLDER)


@cli.command()
@click.pass_context
def leaderboard_update(ctx):
    """Updates the available data on race tracks.

    Args:
        ctx: Click context
    """

    response = list_leaderboards()
    output_path = os.path.join(RAW_DATA_LEADERBOARDS_FOLDER, "leaderboards.csv")

    boto_response_to_csv(response, output_path)

    asset_map = {r["Arn"]: r["ImageUrl"] for r in response if "ImageUrl" in r}
    fetch_media_assets(asset_map, RAW_DATA_ASSETS_LEADERBOARDS_FOLDER)

    # Now do an update for each unique ARN:
    # - If OPEN: We collect a snapshot and save it under the current data and time.
    # - If CLOSED: We assume it is final and store it as FINAL.csv -> Any update to this will be version controlled.

    def update(leaderboard_arn, status):
        output_folder = os.path.join(RAW_DATA_LEADERBOARD_FOLDER, leaderboard_arn)

        if not os.path.exists(output_folder):
            os.makedirs(output_folder, exist_ok=True)

        if status == "OPEN":
            now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
            # Output filename of the nearest hour (2021-01-01 01:00:00.csv)
            output_file = os.path.join(output_folder, "{}.csv".format(now.isoformat()))
            response = list_leaderboard(leaderboard_arn)
        else:
            output_file = os.path.join(output_folder, "FINAL.csv")
            response = list_leaderboard(leaderboard_arn)

        # Video S3 column is too large and will not work anyways so we drop it.
        boto_response_to_csv(response, output_file, drop_columns=["SubmissionVideoS3path"])

    Parallel(n_jobs=-1, prefer="threads")(delayed(update)(r["Arn"], r["Status"]) for r in response)


@cli.command()
@click.option("-o", "output_filename", required=False, default="raw_data.tar.gz")
@click.pass_context
def archive(ctx, output_filename):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(RAW_DATA_FOLDER, arcname=os.path.basename(RAW_DATA_FOLDER))
