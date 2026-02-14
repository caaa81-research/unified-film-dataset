import json, os, time
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class HTTPCache:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)

    def path(self, namespace: str, key: str) -> str:
        ns_dir = os.path.join(self.base_dir, namespace)
        os.makedirs(ns_dir, exist_ok=True)
        safe = "".join(c for c in key if c.isalnum() or c in ("-", "_", "."))
        return os.path.join(ns_dir, f"{safe}.json")

    def get(self, namespace: str, key: str):
        p = self.path(namespace, key)
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        return None

    def set(self, namespace: str, key: str, obj):
        p = self.path(namespace, key)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

class RateLimiter:
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(rps, 0.1)
        self._last = 0.0

    def wait(self):
        now = time.time()
        elapsed = now - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.time()

@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=20),
    retry=retry_if_exception_type(requests.RequestException),
)
def safe_get(url: str, headers=None, params=None, timeout=30):
    resp = requests.get(url, headers=headers, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp
