from deepracer.boto3_enhancer import deepracer_client
import os

SEARCH_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "service")


def list_leaderboards(sortKey="CloseTime", reverseSort=False, limit=None):
    client = deepracer_client(search_path=SEARCH_PATH)

    entries = []
    response = client.list_leaderboards(MaxResults=100)
    entries.extend(response["Leaderboards"])

    while "NextToken" in response:
        response = client.list_leaderboards(MaxResults=100, NextToken=response["NextToken"])

        entries.extend(response["Leaderboards"])

    if sortKey:
        entries = sorted(entries, key=lambda x: x[sortKey], reverse=reverseSort)

    if limit:
        entries = entries[:limit]

    return entries


def list_tracks(sortKey=None, reverseSort=False, limit=None):
    client = deepracer_client(search_path=SEARCH_PATH)

    entries = []
    response = client.list_tracks(MaxResults=100)
    entries.extend(response["Tracks"])

    while "NextToken" in response:
        response = client.list_tracks(
            MaxResults=100,
            NextToken=response["NextToken"],
        )

        entries.extend(response["Tracks"])

    if sortKey:
        entries = sorted(entries, key=lambda x: x[sortKey], reverse=reverseSort)

    if limit:
        entries = entries[:limit]

    return entries


def list_leaderboard(leaderboard_arn, sortKey="Rank", reverseSort=False, limit=None):
    client = deepracer_client(search_path=SEARCH_PATH)

    entries = []
    response = client.list_leaderboard_submissions(LeaderboardArn=leaderboard_arn, MaxResults=100)
    entries.extend(response["LeaderboardSubmissions"])

    while "NextToken" in response:
        response = client.list_leaderboard_submissions(
            MaxResults=100,
            NextToken=response["NextToken"],
            LeaderboardArn=leaderboard_arn,
        )

        entries.extend(response["LeaderboardSubmissions"])

    if sortKey:
        entries = sorted(entries, key=lambda x: x[sortKey], reverse=reverseSort)

    if limit:
        entries = entries[:limit]

    return entries
