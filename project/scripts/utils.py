import pandas as pd

def clean_timestamp(df, col):
    df[col] = df[col].astype(str)
    df[col] = df[col].str.replace(",", ".", regex=False)

    df["event_timestamp"] = pd.to_datetime(
        df[col],
        errors="coerce",
        infer_datetime_format=True,
        dayfirst=False
    )

    df["is_valid_timestamp"] = df["event_timestamp"].notna()

    return df