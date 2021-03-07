import os

# Track data.
TRACK_FOLDER = "tracks"
TRACK_FOLDER_ASSETS = os.path.join(TRACK_FOLDER, "assets")
TRACK_FOLDER_ROUTES = os.path.join(TRACK_FOLDER, "npy")
TRACK_CSV_FILEPATH = os.path.join(TRACK_FOLDER, "tracks.csv")

# Leaderboard data
LEADERBOARDS_FOLDER = "leaderboards"
LEADERBOARDS_FOLDER_ASSETS = os.path.join(LEADERBOARDS_FOLDER, "assets")
LEADERBOARDS_CSV_FILEPATH = os.path.join(LEADERBOARDS_FOLDER, "leaderboards.csv")

# Simapp
SIMAPP_TAR_GZ = "deepracer-simapp.tar.gz"
