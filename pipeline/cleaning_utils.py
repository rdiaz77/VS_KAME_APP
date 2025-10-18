import pandas as pd

def basic_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """Utility: standard column normalization and trimming."""
    df = df.copy()
    df.columns = df.columns.str.strip().str.title()
    df = df.dropna(how="all")

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()

    return df
