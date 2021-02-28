import os
import boto3


def get_boto_deepracer_client(region="us-east-1", session=None):
    SERVICE_FOLDER = os.path.join(os.path.dirname(__file__), "..", "service")

    if not session:
        session = boto3.Session()
        session._loader.search_paths.append(SERVICE_FOLDER)

    return session.client(
        "deepracer",
        region_name=region,
        endpoint_url="https://deepracer-prod.{}.amazonaws.com".format(region),
    )
