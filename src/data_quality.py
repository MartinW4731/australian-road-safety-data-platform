def validate_primary_key(df, column):
    if df[column].isnull().any():
        raise ValueError(f"{column} contains null values")

    if df[column].duplicated().any():
        raise ValueError(f"{column} contains duplicate values")

    return True