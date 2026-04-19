"""
tests/test_app_utils.py

Unit tests for utility functions originally in app.py and config.py.

Note: app.py cannot be imported in tests because it executes Streamlit code
at module level (loads BigQuery data, renders UI). Instead, we test the pure
utility functions by re-defining or copying them here, and we test config.py
directly without importing app.py.
"""
import sys
import os
import json
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ---------------------------------------------------------------------------
# Pure utility functions extracted from app.py for isolated testing.
# These are copy-tested — if app.py changes these functions, update here too.
# ---------------------------------------------------------------------------
def safe_get(value):
    """Return first element of a Series or pass scalar through."""
    if isinstance(value, pd.Series):
        if value.empty:
            return None
        return value.iloc[0]
    return value


def get_copa_default_index(lista_opcoes):
    """Return the index of the active copa, or 0 as default."""
    try:
        if os.path.exists("copas.json"):
            with open("copas.json", "r") as f:
                configs = json.load(f)
            nome_ativa = next((c['nome_visual'] for c in configs if c.get('ativa')), None)
            if nome_ativa and nome_ativa in lista_opcoes:
                return list(lista_opcoes).index(nome_ativa)
    except Exception:
        pass
    return 0


# ---------------------------------------------------------------------------
# safe_get tests
# ---------------------------------------------------------------------------
class TestSafeGet:
    def test_non_empty_series_returns_first(self):
        s = pd.Series([42, 99])
        assert safe_get(s) == 42

    def test_empty_series_returns_none(self):
        s = pd.Series([], dtype=float)
        assert safe_get(s) is None

    def test_scalar_value_passes_through(self):
        assert safe_get(123) == 123
        assert safe_get("hello") == "hello"
        assert safe_get(None) is None

    def test_zero_series_returns_zero(self):
        s = pd.Series([0.0])
        assert safe_get(s) == 0.0

    def test_string_series_returns_first_string(self):
        s = pd.Series(["Alpha", "Beta"])
        assert safe_get(s) == "Alpha"


# ---------------------------------------------------------------------------
# get_copa_default_index tests
# ---------------------------------------------------------------------------
class TestGetCopaDefaultIndex:
    def test_returns_zero_when_no_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)  # no copas.json here
        result = get_copa_default_index(["Copa A", "Copa B"])
        assert result == 0

    def test_returns_correct_index_for_active_copa(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        config = [
            {"nome_visual": "Copa A", "ativa": False},
            {"nome_visual": "Copa B", "ativa": True},
        ]
        (tmp_path / "copas.json").write_text(json.dumps(config), encoding="utf-8")
        result = get_copa_default_index(["Copa A", "Copa B"])
        assert result == 1

    def test_returns_zero_when_active_copa_not_in_list(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        config = [{"nome_visual": "Copa Fantasma", "ativa": True}]
        (tmp_path / "copas.json").write_text(json.dumps(config), encoding="utf-8")
        result = get_copa_default_index(["Copa A", "Copa B"])
        assert result == 0

    def test_returns_zero_on_invalid_json(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "copas.json").write_text("not json!", encoding="utf-8")
        result = get_copa_default_index(["Copa A"])
        assert result == 0

    def test_returns_first_active_when_multiple_marked_active(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        config = [
            {"nome_visual": "Copa A", "ativa": True},
            {"nome_visual": "Copa B", "ativa": True},
        ]
        (tmp_path / "copas.json").write_text(json.dumps(config), encoding="utf-8")
        # Should return index of the first active one
        result = get_copa_default_index(["Copa A", "Copa B"])
        assert result == 0


# ---------------------------------------------------------------------------
# config.py — get_bq_client tests
# ---------------------------------------------------------------------------
class TestConfigGetBqClient:
    def test_raises_valueerror_on_bad_json(self, monkeypatch, tmp_path):
        """Malformed GCP_SERVICE_ACCOUNT JSON must raise ValueError, not crash silently."""
        monkeypatch.chdir(tmp_path)  # no credentials.json here
        monkeypatch.setenv("GCP_SERVICE_ACCOUNT", "this is not json{{{")

        import config
        import importlib
        importlib.reload(config)  # pick up new env var

        with pytest.raises(ValueError, match="GCP_SERVICE_ACCOUNT"):
            config.get_bq_client()

    def test_uses_credentials_file_when_present(self, tmp_path, monkeypatch):
        """If credentials.json exists, it should be used (mocked)."""
        cred_file = tmp_path / "credentials.json"
        cred_file.write_text("{}", encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("GCP_SERVICE_ACCOUNT", raising=False)

        import config
        with patch("config.bigquery.Client.from_service_account_json") as mock_from_file:
            mock_from_file.return_value = MagicMock()
            config.get_bq_client()
            mock_from_file.assert_called_once_with("credentials.json")

    def test_falls_back_to_adc_when_no_env_or_file(self, tmp_path, monkeypatch):
        """No credentials.json + no env var → use Application Default Credentials."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("GCP_SERVICE_ACCOUNT", raising=False)

        import config
        with patch("config.bigquery.Client") as mock_client:
            mock_client.return_value = MagicMock()
            config.get_bq_client()
            mock_client.assert_called_once_with()

