from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    imdb_input_path: str = os.getenv("IMDB_INPUT_PATH", "data/raw/imdb_input.csv")
    out_path: str = os.getenv("OUT_PATH", "data/processed/ufd.csv")

    omdb_api_key: str = os.getenv("OMDB_API_KEY", "")
    tmdb_bearer_token: str = os.getenv("TMDB_BEARER_TOKEN", "")

    user_agent: str = os.getenv("USER_AGENT", "UFDResearchBot/1.0")
    rps: float = float(os.getenv("REQUESTS_PER_SECOND", "4"))

    cache_dir: str = os.getenv("CACHE_DIR", "cache")

SETTINGS = Settings()
