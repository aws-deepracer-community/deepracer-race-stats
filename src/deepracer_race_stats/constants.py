import os

SERVICE_FOLDER = os.path.join(os.path.dirname(__file__), "service")

# Track data.
TRACK_FOLDER = "tracks"
TRACK_FOLDER_ASSETS = os.path.join(TRACK_FOLDER, "assets")
TRACK_CSV_FILEPATH = os.path.join(TRACK_FOLDER, "tracks.csv")

# Leaderboard list data
LEADERBOARDS_FOLDER = "leaderboards"
LEADERBOARDS_FOLDER_ASSETS = os.path.join(LEADERBOARDS_FOLDER, "assets")
LEADERBOARDS_CSV_FILEPATH = os.path.join(LEADERBOARDS_FOLDER, "leaderboards.csv")

# Leaderboard entry data.
LEADERBOARD_FOLDER = "leaderboard"
