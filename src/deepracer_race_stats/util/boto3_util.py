import boto3

from deepracer_race_stats.constants import SERVICE_FOLDER


def get_boto_deepracer_client(region="us-east-1", session=None):
    if not session:
        session = boto3.Session()
        session._loader.search_paths.append(SERVICE_FOLDER)

    return session.client(
        "deepracer",
        region_name=region,
        endpoint_url="https://deepracer-prod.{}.amazonaws.com".format(region),
    )
