# sf_client.py
from __future__ import annotations

import requests
from urllib.parse import urljoin


def normalize_base_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    return u.rstrip("/")


class SFClient:
    def __init__(self, base_url: str, username: str, password: str, timeout: int = 60, verify_ssl: bool = True):
        self.base_url = normalize_base_url(base_url)
        self.username = username
        self.password = password
        self.timeout = timeout
        self.verify_ssl = verify_ssl

    def _request(self, path: str, params: dict | None = None) -> dict:
        url = urljoin(self.base_url + "/", path.lstrip("/"))
        r = requests.get(
            url,
            params=params or {},
            auth=(self.username, self.password),
            timeout=self.timeout,
            verify=self.verify_ssl,
            headers={"Accept": "application/json"},
            allow_redirects=True,
        )

        # Fail hard on 4xx/5xx
        r.raise_for_status()

        # Critical: ensure JSON (wrong base often returns HTML 200)
        ctype = (r.headers.get("Content-Type") or "").lower()
        if "application/json" not in ctype and "json" not in ctype:
            snippet = (r.text or "")[:300].replace("\n", " ")
            raise RuntimeError(
                f"Non-JSON response from {url} (Content-Type={ctype}). "
                f"Likely wrong API base URL. Body starts: {snippet}"
            )

        try:
            return r.json()
        except Exception:
            snippet = (r.text or "")[:300].replace("\n", " ")
            raise RuntimeError(f"JSON decode failed from {url}. Body starts: {snippet}")

    def probe(self) -> bool:
        """
        Cheap call to confirm base_url is a real OData JSON endpoint.
        """
        j = self._request(
            "/odata/v2/User",
            {"$select": "userId", "$top": 1, "$format": "json"},
        )
        # Expect OData v2 JSON shape
        return isinstance(j, dict) and ("d" in j)

    def get_all(self, path: str, params: dict) -> list[dict]:
        params = dict(params or {})
        params.setdefault("$format", "json")
        params.setdefault("$top", 1000)

        out: list[dict] = []
        skip = 0

        while True:
            params["$skip"] = skip
            j = self._request(path, params)

            d = j.get("d") or {}
            results = d.get("results") or []
            if not results:
                break

            out.extend(results)
            if len(results) < int(params["$top"]):
                break

            skip += int(params["$top"])

        return out
