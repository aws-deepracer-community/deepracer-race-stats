import os

SERVICE_FOLDER = os.path.join(os.path.dirname(__file__), "service")

RAW_DATA_FOLDER = os.path.join(os.path.dirname(__file__), "..", "..", "raw_data")

# Track data.
RAW_DATA_TRACK_FOLDER = os.path.join(RAW_DATA_FOLDER, "tracks")
RAW_DATA_ASSETS_TRACK_FOLDER = os.path.join(RAW_DATA_TRACK_FOLDER, "assets")

# Leaderboard list data
RAW_DATA_LEADERBOARDS_FOLDER = os.path.join(RAW_DATA_FOLDER, "leaderboards")
RAW_DATA_ASSETS_LEADERBOARDS_FOLDER = os.path.join(RAW_DATA_LEADERBOARDS_FOLDER, "assets")

# Leaderboard entry data.
RAW_DATA_LEADERBOARD_FOLDER = os.path.join(RAW_DATA_FOLDER, "leaderboard")