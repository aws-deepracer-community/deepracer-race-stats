import os
import click
import boto3
import glob
import shutil
import tarfile
import subprocess

from datetime import datetime
from joblib import Parallel, delayed
from deepracer_race_stats.constants import (
    LEADERBOARDS_CSV_FILEPATH,
    LEADERBOARDS_FOLDER_ASSETS,
    LEADERBOARDS_FOLDER,
    SIMAPP_TAR_GZ,
    TRACK_CSV_FILEPATH,
    TRACK_FOLDER_ASSETS,
    TRACK_FOLDER_ROUTES,
)

from deepracer_race_stats.util.csv_util import boto_response_to_csv
from deepracer_race_stats.util.deepracer_service import (
    list_leaderboard,
    list_leaderboards,
    list_tracks,
)
from deepracer_race_stats.util.assets import fetch_assets, extract_asset_paths


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

    asset_map = {}

    for r in response:
        asset_map.update(extract_asset_paths(r, arn_key="TrackArn"))

    output_assets_folder = os.path.join(output_folder, TRACK_FOLDER_ASSETS)
    fetch_assets(asset_map, output_assets_folder)


@cli.command()
@click.option("-o", "--output-folder", required=True)
@click.option("-m", "--use-s3", is_flag=True, default=False)
@click.option("-d", "--docker-image-url", default="public.ecr.aws/k1d3r4z1/deepracer-sim-public:latest")
@click.option("-b", "--simapp-bucket", default="deepracer-melodic-managed-resources-us-east-1")
@click.option("-k", "--simapp-key", default=SIMAPP_TAR_GZ)
@click.pass_context
def simapp_update(ctx, output_folder, use_s3, docker_image_url, simapp_bucket, simapp_key):
    tmp_folder = "simapp_tmp"

    if use_s3:
        # Download simapp bundle.
        route_prefix = "opt/install/deepracer_simulation_environment/share/deepracer_simulation_environment/routes"

        if not os.path.exists(tmp_folder):
            os.makedirs(tmp_folder)

        s3 = boto3.resource("s3")
        s3.Bucket(simapp_bucket).download_file(Key=simapp_key, Filename=os.path.join("simapp_tmp", SIMAPP_TAR_GZ))

        # Specify subfolders to extract.
        def subfolders(tf):
            for m in tf.getmembers():
                # Routes folder with the track numpy files.
            
                if m.path.startswith(route_prefix):
                    yield m

        with tarfile.open(os.path.join(tmp_folder, SIMAPP_TAR_GZ)) as f:
            f.extractall(path=tmp_folder)

        # Extract specific parts of the simapp we want to store.
        with tarfile.open(os.path.join(tmp_folder, "bundle.tar"), mode="r") as f:
            f.extractall(path=tmp_folder, members=subfolders(f))
    else:
        route_prefix = "opt/amazon/routes"
        tmp_output_folder = os.path.join(tmp_folder, "opt", "amazon")

        if not os.path.exists(tmp_output_folder):
            os.makedirs(tmp_output_folder)

        container_id = subprocess.check_output(f"docker create {docker_image_url}", shell=True, encoding='UTF-8').strip()
        subprocess.check_output(f"docker cp {container_id}:/{route_prefix} {tmp_output_folder}", shell=True)
        subprocess.check_output(f"docker rm {container_id}", shell=True)


    # Move the files we want to the raw_data folder.
    output_track_folder = os.path.join(output_folder, TRACK_FOLDER_ROUTES)

    if not os.path.exists(output_track_folder):
        os.makedirs(output_track_folder)

    for route in glob.glob(os.path.join(tmp_folder, route_prefix, "*.npy")):
        output_path = os.path.join(output_track_folder, os.path.basename(route))

        shutil.copy(route, output_path)

    # Remove temporary folder
    shutil.rmtree(tmp_folder)


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

    asset_map = {}
    for r in response:
        asset_map.update(extract_asset_paths(r))

    output_assets_folder = os.path.join(output_folder, LEADERBOARDS_FOLDER_ASSETS)
    fetch_assets(asset_map, output_assets_folder)

    # Now do an update for each unique ARN:
    # - If OPEN: We collect a snapshot and save it under the current data and time.
    # - If CLOSED: We assume it is final and store it as FINAL.csv -> Any update to this will be version controlled.

    def update(leaderboard_arn, status):
        leaderboard_output_folder = os.path.join(output_folder, LEADERBOARDS_FOLDER, leaderboard_arn)

        if not os.path.exists(leaderboard_output_folder):
            os.makedirs(leaderboard_output_folder, exist_ok=True)

        if status == "OPEN":
            now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
            # Output filename of the nearest hour (2021-01-01 01:00:00.csv)
            output_file = os.path.join(leaderboard_output_folder, "{}.csv".format(now.strftime("%Y-%m-%dT%H:%M:%SZ")))
            response = list_leaderboard(leaderboard_arn)
        elif status == "CLOSED":
            output_file = os.path.join(leaderboard_output_folder, "FINAL.csv")
            response = list_leaderboard(leaderboard_arn)
        else:
            return

        # Video S3 column is too large and will not work anyways so we drop it.
        boto_response_to_csv(response, output_file, drop_columns=["SubmissionVideoS3path"])

    Parallel(n_jobs=2, prefer="threads")(delayed(update)(r["Arn"], r["Status"]) for r in response)
