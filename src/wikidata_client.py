from typing import Dict, Optional
from .http_utils import safe_get, HTTPCache, RateLimiter

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

class WikidataClient:
    def __init__(self, cache: HTTPCache, limiter: RateLimiter, user_agent: str):
        self.cache = cache
        self.limiter = limiter
        self.headers = {
            "User-Agent": user_agent,
            "Accept": "application/sparql-results+json",
        }

    def query_by_imdb_id(self, tconst: str) -> Optional[Dict]:
        cached = self.cache.get("wikidata", tconst)
        if cached is not None:
            return cached

        self.limiter.wait()
        sparql = f"""
        SELECT ?item ?itemLabel ?thenumbersId WHERE {{
          ?item wdt:P345 "{tconst}" .
          OPTIONAL {{ ?item wdt:P3808 ?thenumbersId . }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }} LIMIT 5
        """
        r = safe_get(WIKIDATA_SPARQL, headers=self.headers, params={"query": sparql, "format": "json"})
        data = r.json()
        self.cache.set("wikidata", tconst, data)
        return data
