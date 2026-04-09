import json
import pytest
from unittest.mock import MagicMock
from pathlib import Path
import requests

from app.services.eia_client import fetch_retail_sales

FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture(name):
    return json.loads((FIXTURES / name).read_text())


def mock_response(status_code=200, json_data=None):
    mock = MagicMock()
    mock.status_code = status_code
    if json_data is not None:
        mock.json.return_value = json_data
        mock.text = json.dumps(json_data)
    else:
        mock.json.side_effect = ValueError("no json")
        mock.text = "bad response"
    return mock


def test_successful_fetch_returns_rows(mocker):
    fixture = load_fixture("eia_retail_sales_sample.json")
    mocker.patch("requests.get", return_value=mock_response(json_data=fixture))

    rows = fetch_retail_sales("testkey", "2024-01", "2024-01")

    assert len(rows) == 4
    assert rows[0]["stateid"] == "CA"
    assert rows[0]["sectorid"] == "RES"
    assert rows[0]["price"] == "31.52"


def test_pagination_combines_rows(mocker):
    page1 = {
        "response": {
            "total": 6,
            "data": [{"period": "2024-01", "stateid": f"S{i}"} for i in range(4)],
        }
    }
    page2 = {
        "response": {
            "total": 6,
            "data": [{"period": "2024-01", "stateid": f"S{i}"} for i in range(4, 6)],
        }
    }
    mocker.patch(
        "requests.get",
        side_effect=[
            mock_response(json_data=page1),
            mock_response(json_data=page2),
        ],
    )

    rows = fetch_retail_sales("testkey", "2024-01", "2024-01")

    assert len(rows) == 6
    assert rows[0]["stateid"] == "S0"
    assert rows[5]["stateid"] == "S5"


def test_missing_response_data_raises(mocker):
    fixture = load_fixture("eia_retail_sales_bad_rows.json")
    mocker.patch("requests.get", return_value=mock_response(json_data=fixture))

    with pytest.raises(RuntimeError, match="missing 'response.data'"):
        fetch_retail_sales("testkey", "2024-01", "2024-01")


def test_non_200_response_raises(mocker):
    mocker.patch(
        "requests.get",
        return_value=mock_response(status_code=500, json_data={"error": "server error"}),
    )

    with pytest.raises(RuntimeError, match="status 500"):
        fetch_retail_sales("testkey", "2024-01", "2024-01")


def test_timeout_raises(mocker):
    mocker.patch("requests.get", side_effect=requests.exceptions.Timeout())

    with pytest.raises(RuntimeError, match="timed out"):
        fetch_retail_sales("testkey", "2024-01", "2024-01")
