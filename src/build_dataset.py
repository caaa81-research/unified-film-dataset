import pandas as pd
from datetime import datetime, date
from typing import Optional, Dict, Any, List
from .io_utils import ensure_columns

FINAL_COLUMNS = [
    "tconst","originalTitle","startYear",
    "rated","director","writer","actors","language","country",
    "genres","runtimeMinutes","awards","production",
    "imdbRating","imdbVotes","metascore","rottenTomatoes",
    "distributor","productionMethod","productionBudget",
    "boxOfficeDomestic","boxOfficeInternational","boxOfficeWorldwide",
    "RL.Theatrical","RL.Premiere","RL.Digital","RL.Physical","RL.TV"
]

def normalize_omdb_fields(omdb_json: dict) -> dict:
    if not isinstance(omdb_json, dict) or omdb_json.get("Response") == "False":
        return {}

    rt = None
    for r in omdb_json.get("Ratings", []) or []:
        if r.get("Source") == "Rotten Tomatoes":
            rt = r.get("Value")
            break

    runtime = (omdb_json.get("Runtime") or "").replace(" min", "").strip() or None
    imdb_votes = (omdb_json.get("imdbVotes") or "").replace(",", "").strip() or None

    return {
        "rated": omdb_json.get("Rated"),
        "director": omdb_json.get("Director"),
        "writer": omdb_json.get("Writer"),
        "actors": omdb_json.get("Actors"),
        "language": omdb_json.get("Language"),
        "country": omdb_json.get("Country"),
        "genres_omdb": omdb_json.get("Genre"),
        "runtimeMinutes": runtime,
        "awards": omdb_json.get("Awards"),
        "production": omdb_json.get("Production"),
        "imdbRating": omdb_json.get("imdbRating"),
        "imdbVotes": imdb_votes,
        "metascore": omdb_json.get("Metascore"),
        "rottenTomatoes": rt,
    }

# --- TMDb release lag (your rules) ---
TMDB_TYPE_TO_WINDOW = {
    1: "RL.Premiere",
    2: "RL.Theatrical",
    3: "RL.Theatrical",
    4: "RL.Digital",
    5: "RL.Physical",
    6: "RL.TV",
}
THEATRICAL_TYPES = {2, 3}

def _parse_tmdb_date(s: Optional[str]) -> Optional[date]:
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s[:10]).date()
    except Exception:
        return None

def _iter_release_dates(tmdb_release_dates_json: Dict[str, Any]):
    results = (tmdb_release_dates_json or {}).get("results") or []
    for country in results:
        for rd in (country.get("release_dates") or []):
            t = rd.get("type")
            d = _parse_tmdb_date(rd.get("release_date"))
            if isinstance(t, int) and d is not None:
                yield t, d

def compute_release_lags(tmdb_release_dates_json: Dict[str, Any]) -> Dict[str, Optional[str]]:
    first_dates: Dict[str, date] = {}

    for t, d in _iter_release_dates(tmdb_release_dates_json):
        window = TMDB_TYPE_TO_WINDOW.get(t)
        if window is None:
            continue

        if window not in first_dates or d < first_dates[window]:
            first_dates[window] = d

    return {
        "RL.Theatrical": first_dates.get("RL.Theatrical").isoformat()
            if first_dates.get("RL.Theatrical") else None,
        "RL.Premiere": first_dates.get("RL.Premiere").isoformat()
            if first_dates.get("RL.Premiere") else None,
        "RL.Digital": first_dates.get("RL.Digital").isoformat()
            if first_dates.get("RL.Digital") else None,
        "RL.Physical": first_dates.get("RL.Physical").isoformat()
            if first_dates.get("RL.Physical") else None,
        "RL.TV": first_dates.get("RL.TV").isoformat()
            if first_dates.get("RL.TV") else None,
    }


def assemble_dataset(imdb_df, omdb_df, tmdb_df, box_df):
    df = imdb_df.merge(omdb_df, on="tconst", how="left") \
                .merge(tmdb_df, on="tconst", how="left") \
                .merge(box_df, on="tconst", how="left")

    # Fallback: IMDb genres -> OMDb genres
    if "genres" not in df.columns:
        df["genres"] = None

    df["genres"] = df["genres"].where(
        df["genres"].notna() & (df["genres"].astype(str).str.strip() != ""),
        df.get("genres_omdb")
    )

    df = ensure_columns(df, FINAL_COLUMNS)
    df = df[FINAL_COLUMNS]
    return df

