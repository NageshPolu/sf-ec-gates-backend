# sf_client.py
from __future__ import annotations

import requests
from typing import Any, Dict, List, Optional


def normalize_base_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    return u.rstrip("/")


class SFClient:
    """
    SuccessFactors OData v2 client with paging.
    Uses basic auth.
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: int = 60,
        verify_ssl: bool = True,
    ):
        self.base_url = normalize_base_url(base_url)
        self.username = username
        self.password = password
        self.timeout = timeout
        self.verify_ssl = verify_ssl

        if not self.base_url:
            raise ValueError("SF base_url is empty")

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{path}"

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = self._url(path)
        r = requests.get(
            url,
            params=params or {},
            auth=(self.username, self.password),
            headers={"Accept": "application/json"},
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        # Important: raise so backend returns 500 (not “Run completed”)
        r.raise_for_status()
        return r.json()

    def get_all(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 1000,
        max_pages: int = 200,
    ) -> List[dict]:
        """
        Fetches all rows via $top/$skip.
        """
        params = dict(params or {})
        params.setdefault("$format", "json")

        rows: List[dict] = []
        skip = 0

        for _ in range(max_pages):
            page_params = dict(params)
            page_params["$top"] = page_size
            page_params["$skip"] = skip

            data = self.get(path, page_params)

            # OData v2 JSON shape
            d = data.get("d") or {}
            results = d.get("results") or []
            if not isinstance(results, list):
                break

            rows.extend(results)

            if len(results) < page_size:
                break

            skip += page_size

        return rows
