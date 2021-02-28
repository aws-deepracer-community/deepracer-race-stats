import pandas as pd


def boto_response_to_csv(response, path, drop_columns=[]):
    # Normalize the input list of dictionaries such that we get a column for every nested dictionary.
    df = pd.json_normalize(response, sep="_")

    # Remove specified columns from output, handy if there is data we don't need or the data takes up too much space.
    columns = [c for c in df.columns.tolist() if c not in drop_columns]

    # Now write that output to CSV.
    df.to_csv(path, columns=columns, index=False, na_rep="")
