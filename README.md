# Unified Film Dataset (UFD)

Reproducible research pipeline to construct an integrated film-level dataset using:

- IMDb (manual download required)
- OMDb API
- TMDb API (release dates for window proxies)
- Wikidata SPARQL (deterministic linkage to The Numbers via P3808 when available)
- The Numbers (scraping of selected summary fields; cached and rate-limited)

# Unified Film Dataset (UFD)


## Project Structure

- `src/` → core pipeline logic (importable)
- `data/raw/` → manually downloaded IMDb files (not versioned)
- `data/processed/` → final dataset output (not versioned by default)
- `cache/` → API and scraping cache (not versioned)

## Setup

1. Place an IMDb export in `data/raw/` (CSV or TSV). It must include at least:
   `tconst, originalTitle, startYear`.

2. Copy `.env.example` → `.env` and fill in credentials.

3. Install:
   ```bash
   pip install -r requirements.txt
   ```

## Run pipeline

From the repository root:

```bash
python -m src.pipeline --imdb data/raw/imdb_input.csv --out data/processed/ufd.csv
```

## Release Definition (TMDb)

- Unit: **days**
- Dates: first **global** occurrence of each release type (no country filtering)

## The Numbers fields

- `productionBudget`, `boxOfficeDomestic`, `boxOfficeInternational`, `boxOfficeWorldwide`
- `distributor`: extracted from the first *Domestic Releases* entry (e.g., "by Warner Bros.")
- `productionMethod`: extracted from *Production Method* (e.g., "Live Action")

## Example (Wikidata → The Numbers)

Wikidata can provide deterministic IDs:
- IMDb ID (P345): `tt2582500`
- The Numbers ID (P3808): `Shut-In`

The Numbers movie page:
`https://www.the-numbers.com/movie/Shut-In#tab=summary`

## Created by

Carlos A. del Alamo Alonso - cadelalamo@uloyola.es