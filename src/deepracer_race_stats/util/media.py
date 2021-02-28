import os
import requests
from joblib import Parallel, delayed


def fetch_media_assets(key_url_map, output_folder):
    # Specific for tracks, we also collect the assets.
    def download(key, url):
        try:
            _, output_ext = os.path.splitext(url)
            output_path = os.path.join(output_folder, "{}{}".format(key, output_ext))
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
