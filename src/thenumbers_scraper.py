from typing import Dict, Optional
import re
from bs4 import BeautifulSoup
from .http_utils import safe_get, HTTPCache, RateLimiter

_MONEY_RE = re.compile(r"\$\s*([0-9]{1,3}(?:,[0-9]{3})*)")

def _money_to_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    m = _MONEY_RE.search(s)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))

class TheNumbersScraper:
    """
    Scrapes a conservative subset of fields from The Numbers movie pages
    (Summary tab). Cached + rate-limited.

    Example:
      https://www.the-numbers.com/movie/Joker-Folie-a-Deux-(2024)#tab=summary
    """
    BASE = "https://www.the-numbers.com"

    def __init__(self, cache: HTTPCache, limiter: RateLimiter, user_agent: str):
        self.cache = cache
        self.limiter = limiter
        self.headers = {"User-Agent": user_agent}

    def fetch_movie_page(self, thenumbers_id: str) -> Optional[str]:
        key = f"html_{thenumbers_id}"
        cached = self.cache.get("thenumbers", key)
        if cached is not None:
            return cached.get("html")

        self.limiter.wait()
        # Note: URL fragment (#tab=summary) is not sent in HTTP requests.
        url = f"{self.BASE}/movie/{thenumbers_id}"
        r = safe_get(url, headers=self.headers)
        html = r.text
        self.cache.set("thenumbers", key, {"html": html})
        return html

    def _extract_amount_from_record_row(self, soup: BeautifulSoup, anchor_text: str) -> Optional[int]:
        a = soup.find("a", string=lambda t: isinstance(t, str) and anchor_text in t)
        if not a:
            return None
        tr = a.find_parent("tr")
        if not tr:
            return None
        tds = tr.find_all("td")
        if not tds:
            return None
        return _money_to_int(tds[-1].get_text(" ", strip=True))

    def parse_box_office(self, html: str) -> Dict:
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(" ", strip=True)

        # --- Production Budget ---
        production_budget = None
        m = re.search(r"Production\s*Budget:\s*\$\s*([0-9]{1,3}(?:,[0-9]{3})*)", text)
        if m:
            production_budget = int(m.group(1).replace(",", ""))

        # --- Box office totals ---
        dom = self._extract_amount_from_record_row(soup, "All Time Domestic Box Office")
        intl = self._extract_amount_from_record_row(soup, "All Time International Box Office")
        ww = self._extract_amount_from_record_row(soup, "All Time Worldwide Box Office")

        # --- Distributor (first Domestic Releases entry) ---
        distributor = None
        md = re.search(
            r"Domestic\s+Releases:\s.*?\sby\s([A-Za-z0-9&\.\-’' ]+?)"
            r"(?:\s+October|\s+International|\s+Video\s+Release:|\s+MPAA\s+Rating:|$)",
            text
        )
        if md:
            distributor = md.group(1).strip()
        else:
            # fallback: grab first anchor after Domestic Releases label
            label = soup.find(string=lambda t: isinstance(t, str) and "Domestic Releases:" in t)
            if label and getattr(label, "parent", None):
                parent = label.parent
                a = parent.find("a")
                if a:
                    distributor = a.get_text(" ", strip=True) or None

        # --- Production Method ---
        production_method = None
        pm = re.search(
            r"Production\s*Method:\s*([A-Za-z0-9 \-/]+?)"
            r"(?:\s+Creative\s*Type:|\s+Production/Financing|\s+Production\s+Countries:|\s+Languages:|$)",
            text
        )
        if pm:
            production_method = pm.group(1).strip()

        return {
            "productionBudget": production_budget,
            "boxOfficeDomestic": dom,
            "boxOfficeInternational": intl,
            "boxOfficeWorldwide": ww,
            "distributor": distributor,
            "productionMethod": production_method,
        }
