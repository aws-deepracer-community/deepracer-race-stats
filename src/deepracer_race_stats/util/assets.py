import os
import requests
from joblib import Parallel, delayed
from urllib.parse import urlparse


def fetch_assets(key_url_map, output_folder):
    # Specific for tracks, we also collect the assets.
    def download(key, url):
        try:
            output_path = os.path.join(output_folder, key)
            output_dir = os.path.dirname(output_path)

            if not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)

            r = requests.get(url)
            if r.status_code == 200:
                with open(output_path, "wb") as f:
                    f.write(r.content)
        except Exception as e:
            print(e)
            # Assume we can't get it then.
            pass

    return Parallel(n_jobs=-1, prefer="threads")(delayed(download)(key, url) for key, url in key_url_map.items())


def get_asset_path(arn, url):
    return os.path.join(arn, urlparse(url).path.lstrip("/"))


def extract_asset_paths(r, arn_key="Arn"):
    arn = r[arn_key]
    response_asset_map = {}

    columns = ["ImageUrl", "LeaderboardImage", "TrackPicture"]

    for c in columns:
        if c in r:
            response_asset_map[get_asset_path(arn, r[c])] = r[c]

    if "TrackRaceTypePictureMap" in r:
        for key, value in r["TrackRaceTypePictureMap"].items():
            response_asset_map[get_asset_path(arn, r["TrackRaceTypePictureMap"][key])] = value

    return response_asset_map
