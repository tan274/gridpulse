import requests

EIA_BASE_URL = "https://api.eia.gov/v2/electricity/retail-sales/data/"
PAGE_SIZE = 5000


def fetch_retail_sales(
    api_key: str,
    start_period: str,
    end_period: str,
    state_ids: list[str] | None = None,
    sector_ids: list[str] | None = None,
) -> list[dict]:
    """Fetch EIA retail-sales rows and return a flat list of raw row dicts."""
    all_rows = []
    offset = 0

    while True:
        params = [
            ("api_key", api_key),
            ("data[]", "price"),
            ("data[]", "sales"),
            ("data[]", "revenue"),
            ("data[]", "customers"),
            ("frequency", "monthly"),
            ("start", start_period),
            ("end", end_period),
            ("sort[0][column]", "period"),
            ("sort[0][direction]", "asc"),
            ("offset", offset),
            ("length", PAGE_SIZE),
        ]

        if state_ids:
            for s in state_ids:
                params.append(("facets[stateid][]", s))

        if sector_ids:
            for s in sector_ids:
                params.append(("facets[sectorid][]", s))

        try:
            response = requests.get(EIA_BASE_URL, params=params, timeout=30)
        except requests.exceptions.Timeout:
            raise RuntimeError("EIA API request timed out")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"EIA API network error: {e}")

        if response.status_code != 200:
            raise RuntimeError(
                f"EIA API returned status {response.status_code}: {response.text[:200]}"
            )

        try:
            body = response.json()
        except ValueError as e:
            raise RuntimeError(f"EIA API returned malformed JSON: {e}")

        if "response" not in body or "data" not in body["response"]:
            raise RuntimeError("EIA API response missing 'response.data'")

        rows = body["response"]["data"]
        all_rows.extend(rows)

        total = body["response"].get("total", len(all_rows))
        offset += len(rows)

        if offset >= int(total) or len(rows) == 0:
            break

    return all_rows
