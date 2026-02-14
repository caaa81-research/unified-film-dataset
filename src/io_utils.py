import pandas as pd
from pathlib import Path

def read_imdb_file(path: str) -> pd.DataFrame:
    """Read a manually-downloaded IMDb CSV/TSV file."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"IMDb input not found: {path}")

    if p.suffix.lower() == ".tsv":
        df = pd.read_csv(path, sep="\t", dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)

    for col in ["isAdult", "startYear", "runtimeMinutes"]:
        if col in df.columns:
            df[col] = df[col].replace({"\\N": None})

    return df

def ensure_columns(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df
