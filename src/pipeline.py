import argparse
import pandas as pd
from tqdm import tqdm

from .config import SETTINGS
from .http_utils import HTTPCache, RateLimiter
from .io_utils import read_imdb_file
from .omdb_client import OMDbClient
from .tmdb_client import TMDbClient
from .wikidata_client import WikidataClient
from .thenumbers_scraper import TheNumbersScraper
from .build_dataset import normalize_omdb_fields, compute_release_lags, assemble_dataset

def run(imdb_path: str, out_path: str):
    cache = HTTPCache(SETTINGS.cache_dir)
    limiter = RateLimiter(SETTINGS.rps)

    imdb_df = read_imdb_file(imdb_path)
    if "tconst" not in imdb_df.columns:
        raise ValueError("IMDb input must contain a 'tconst' column.")

    tconsts = imdb_df["tconst"].dropna().astype(str).unique().tolist()

    # 1) OMDb
    omdb = OMDbClient(SETTINGS.omdb_api_key, cache, limiter, SETTINGS.user_agent)
    omdb_rows = []
    for tconst in tqdm(tconsts, desc="OMDb"):
        data = omdb.fetch_by_imdb_id(tconst)
        omdb_rows.append({"tconst": tconst, **normalize_omdb_fields(data)})
    omdb_df = pd.DataFrame(omdb_rows)

    # 2) TMDb release lags
    tmdb = TMDbClient(SETTINGS.tmdb_bearer_token, cache, limiter, SETTINGS.user_agent)
    tmdb_rows = []
    for tconst in tqdm(tconsts, desc="TMDb"):
        tmdb_id = tmdb.find_tmdb_movie_id(tconst)
        if tmdb_id is None:
            tmdb_rows.append({"tconst": tconst})
            continue
        rd = tmdb.fetch_release_dates(tmdb_id)
        lags = compute_release_lags(rd)
        tmdb_rows.append({"tconst": tconst, **lags})
    tmdb_df = pd.DataFrame(tmdb_rows)

    # 3) Wikidata -> The Numbers
    wd = WikidataClient(cache, limiter, SETTINGS.user_agent)
    tn = TheNumbersScraper(cache, limiter, SETTINGS.user_agent)

    box_rows = []
    for tconst in tqdm(tconsts, desc="Wikidata/TheNumbers"):
        q = wd.query_by_imdb_id(tconst)

        thenumbers_id = None
        try:
            bindings = q.get("results", {}).get("bindings", [])
            if bindings and "thenumbersId" in bindings[0]:
                thenumbers_id = bindings[0]["thenumbersId"]["value"]
        except Exception:
            thenumbers_id = None

        parsed = {}
        if thenumbers_id:
            html = tn.fetch_movie_page(thenumbers_id)
            parsed = tn.parse_box_office(html) if html else {}

        box_rows.append({"tconst": tconst, **parsed})

    box_df = pd.DataFrame(box_rows)

    # 4) Assemble final
    df_final = assemble_dataset(imdb_df, omdb_df, tmdb_df, box_df)
    df_final.to_csv(out_path, index=False)
    return df_final

def main():
    ap = argparse.ArgumentParser(description="Build Unified Film Dataset (UFD)")
    ap.add_argument("--imdb", default=SETTINGS.imdb_input_path, help="Path to manual IMDb CSV/TSV")
    ap.add_argument("--out", default=SETTINGS.out_path, help="Output CSV path")
    args = ap.parse_args()
    run(args.imdb, args.out)

if __name__ == "__main__":
    main()
