# src/fetch_offers.py
from __future__ import annotations

import time
import json
from typing import Any, Dict, List, Optional, Set, Tuple
import requests

DEFAULT_BASE_URL = "https://job-offers-api-ujjz.onrender.com/job-offers"

DEFAULT_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_SECONDS = 1
DEFAULT_SLEEP_BETWEEN_PAGES = 0.3

USER_AGENT = (
    "job-offers-analyzer/1.0 (+https://github.com/fmarinoa/job-offers-analyzer)"
)


class HttpError(Exception):
    """Errores HTTP no recuperables."""
    pass


class HttpClient:
    """
    Cliente HTTP simple con reintentos y backoff exponencial.
    SRP: Encapsula transporte HTTP.
    """

    def __init__(
            self,
            base_url: str = DEFAULT_BASE_URL,
            timeout: float = DEFAULT_TIMEOUT,
            max_retries: int = DEFAULT_MAX_RETRIES,
            backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })

    def get_json(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Realiza un GET a base_url con params y devuelve JSON.
        Reintenta ante 429/5xx o errores transitorios de red.
        """
        url = self.base_url
        backoff = self.backoff_seconds

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                # Reintentos ante 429/5xx
                if resp.status_code in (429, 500, 502, 503, 504):
                    if attempt < self.max_retries:
                        time.sleep(backoff)
                        backoff *= 2
                        continue
                # Errores no recuperables
                if not resp.ok:
                    raise HttpError(
                        f"HTTP {resp.status_code} for {resp.url}: {resp.text[:200]}"
                    )
                return resp.json()
            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt < self.max_retries:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                raise HttpError(f"Network error after {attempt} attempts: {e}") from e

        # En teoría no llegamos aquí
        raise HttpError("Exhausted retries without response")


class JobOffersFetcher:
    """
    Coordina la obtención paginada de ofertas desde la API.
    SRP: Conocer la paginación y consolidación de resultados.
    """

    def __init__(
            self,
            http: Optional[HttpClient] = None,
            sleep_between_pages: float = DEFAULT_SLEEP_BETWEEN_PAGES,
    ) -> None:
        self.http = http or HttpClient()
        self.sleep_between_pages = sleep_between_pages

    @staticmethod
    def _dedupe_key(item: Dict[str, Any]) -> str:
        """
        Clave de deduplicación estable: prioriza _id y cae a linkOffer.
        """
        return str(item.get("_id") or item.get("linkOffer") or id(item))

    def fetch_page(self, days: int, page: int) -> Dict[str, Any]:
        """
        Obtiene una página cruda del endpoint.
        Devuelve el dict con llaves esperadas: total, page, totalPages, results.
        """
        params = {"days": days, "page": page}
        data = self.http.get_json(params=params)

        # Validación mínima
        for key in ("results", "total", "totalPages", "page"):
            if key not in data:
                raise ValueError(f"Missing key '{key}' in API response (page={page})")
        if not isinstance(data["results"], list):
            raise TypeError("Expected 'results' to be a list")

        return data

    def get_all_offers(
            self,
            days: int = 7,
            max_pages: Optional[int] = None,
            respect_rate_limit: bool = True,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Itera sobre todas las páginas necesarias hasta totalPages (o max_pages si se define),
        deduplica resultados y devuelve:
          - lista consolidada de ofertas
          - metadatos agregados (total reportado por API, páginas recorridas, etc.)

        Returns:
            (offers, meta)
        """
        # Primera página para conocer totalPages
        first = self.fetch_page(days=days, page=1)
        total_pages_api = int(first.get("totalPages", 1))

        if max_pages is not None:
            total_pages = min(total_pages_api, max_pages)
        else:
            total_pages = total_pages_api

        seen: Set[str] = set()
        consolidated: List[Dict[str, Any]] = []

        def add_batch(items: List[Dict[str, Any]]) -> None:
            for it in items:
                key = self._dedupe_key(it)
                if key in seen:
                    continue
                seen.add(key)
                consolidated.append(it)

        add_batch(first["results"])

        # Resto de páginas
        for p in range(2, total_pages + 1):
            if respect_rate_limit and self.sleep_between_pages > 0:
                time.sleep(self.sleep_between_pages)

            page_data = self.fetch_page(days=days, page=p)
            add_batch(page_data["results"])

        meta = {
            "total_reported": first.get("total"),
            "pages_traversed": total_pages,
            "total_pages_api": total_pages_api,
            "items_consolidated": len(consolidated),
            "days_param": days,
        }

        return consolidated, meta


# Ejecución directa: guarda el JSON crudo consolidado (útil para debugging local o en Actions)
if __name__ == "__main__":
    import argparse
    import pathlib

    parser = argparse.ArgumentParser(description="Fetch job offers (paginated) and consolidate results.")
    parser.add_argument("--days", type=int, default=7, help="Filtro days del endpoint (default: 7)")
    parser.add_argument("--max_pages", type=int, default=None, help="Límite opcional de páginas a recorrer")
    parser.add_argument("--output", type=str, default="data/raw_offers.json", help="Ruta del archivo de salida JSON")
    args = parser.parse_args()

    fetcher = JobOffersFetcher()
    offers, meta = fetcher.get_all_offers(days=args.days, max_pages=args.max_pages)

    out_path = pathlib.Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as f:
        json.dump({"meta": meta, "results": offers}, f, ensure_ascii=False, indent=2)

    print(f"[OK] Guardado {len(offers)} ofertas en {out_path} | meta={meta}")
