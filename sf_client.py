import requests
from requests.auth import HTTPBasicAuth

def odata_results(payload: dict) -> list[dict]:
    d = payload.get("d")
    if isinstance(d, dict):
        return d.get("results") or []
    return []

class SFClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.auth = HTTPBasicAuth(username, password)

    def get_all(self, path: str, params: dict, page_size: int = 1000, max_pages: int = 200) -> list[dict]:
        all_rows = []
        skip = 0
        for _ in range(max_pages):
            p = dict(params)
            p["$top"] = str(page_size)
            p["$skip"] = str(skip)
            p["$format"] = "json"

            url = f"{self.base_url}{path}"
            r = requests.get(url, params=p, auth=self.auth, timeout=60)
            r.raise_for_status()

            rows = odata_results(r.json())
            all_rows.extend(rows)

            if len(rows) < page_size:
                break
            skip += page_size
        return all_rows
