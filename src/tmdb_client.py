from typing import Dict, Optional
from .http_utils import safe_get, HTTPCache, RateLimiter

TMDB_FIND_URL = "https://api.themoviedb.org/3/find/{imdb_id}"

class TMDbClient:
    def __init__(self, bearer_token: str, cache: HTTPCache, limiter: RateLimiter, user_agent: str):
        if not bearer_token:
            raise ValueError("Missing TMDB_BEARER_TOKEN")
        self.cache = cache
        self.limiter = limiter
        self.headers = {
            "Authorization": f"Bearer {bearer_token}",
            "User-Agent": user_agent,
            "accept": "application/json",
        }

    def find_tmdb_movie_id(self, tconst: str) -> Optional[int]:
        cached = self.cache.get("tmdb", f"find_{tconst}")
        if cached is not None:
            return cached.get("tmdb_id")

        self.limiter.wait()
        url = TMDB_FIND_URL.format(imdb_id=tconst)
        r = safe_get(url, headers=self.headers, params={"external_source": "imdb_id"})
        data = r.json()

        tmdb_id = None
        if data.get("movie_results"):
            tmdb_id = data["movie_results"][0].get("id")

        self.cache.set("tmdb", f"find_{tconst}", {"tmdb_id": tmdb_id, "raw": data})
        return tmdb_id

    def fetch_release_dates(self, tmdb_id: int) -> Dict:
        key = f"release_dates_{tmdb_id}"
        cached = self.cache.get("tmdb", key)
        if cached is not None:
            return cached

        self.limiter.wait()
        url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/release_dates"
        r = safe_get(url, headers=self.headers)
        data = r.json()
        self.cache.set("tmdb", key, data)
        return data
