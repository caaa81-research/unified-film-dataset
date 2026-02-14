from typing import Dict, Optional
from .http_utils import safe_get, HTTPCache, RateLimiter

OMDB_URL = "https://www.omdbapi.com/"

class OMDbClient:
    def __init__(self, api_key: str, cache: HTTPCache, limiter: RateLimiter, user_agent: str):
        if not api_key:
            raise ValueError("Missing OMDB_API_KEY")
        self.api_key = api_key
        self.cache = cache
        self.limiter = limiter
        self.headers = {"User-Agent": user_agent}

    def fetch_by_imdb_id(self, tconst: str) -> Optional[Dict]:
        cached = self.cache.get("omdb", tconst)
        if cached is not None:
            return cached

        self.limiter.wait()
        params = {"apikey": self.api_key, "i": tconst, "plot": "short"}
        r = safe_get(OMDB_URL, headers=self.headers, params=params)
        data = r.json()

        self.cache.set("omdb", tconst, data)
        return data
