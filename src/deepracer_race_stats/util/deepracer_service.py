from deepracer.boto3_enhancer import deepracer_client


def list_leaderboards(sortKey="CloseTime", reverseSort=False, limit=None):
    client = deepracer_client()

    entries = []
    response = client.list_leaderboards(MaxResults=100)
    entries.extend(response["Leaderboards"])

    while "NextToken" in response:
        response = client.list_leaderboards(
            MaxResults=100,
            NextToken=response["NextToken"],
        )

        entries.extend(response["Leaderboards"])

    if sortKey:
        entries = sorted(entries, key=lambda x: x[sortKey], reverse=reverseSort)

    if limit:
        entries = entries[:limit]

    return entries


def list_tracks(sortKey=None, reverseSort=False, limit=None):
    client = deepracer_client()

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


def list_leaderboard(leaderboard_arn, sortKeys=["Rank", "RankingScore"], reverseSort=False, limit=None):
    client = deepracer_client()

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

    for sortKey in sortKeys:
        try:
            entries = sorted(entries, key=lambda x: x[sortKey], reverse=reverseSort)
            break
        except KeyError:
            pass

    if limit:
        entries = entries[:limit]

    return entries
