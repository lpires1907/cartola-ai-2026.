"""
tests/test_coletor_copa.py

Unit tests for coletor_copa.py — covers config loading, token retrieval,
and safe API access patterns. All network and BigQuery calls are mocked.
"""
import sys
import os
import json
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture(autouse=True)
def mock_bq_client(monkeypatch):
    """Prevent any real BigQuery client creation during tests."""
    fake_client = MagicMock()
    fake_client.project = "test-project"

    # Mock both module-level get_bq_client and the cartola_utils import
    with patch("coletor_copa.get_bq_client", return_value=fake_client):
        with patch("coletor_copa.cartola_utils") as mock_utils:
            mock_utils.buscar_parciais_globais.return_value = {}
            yield fake_client


# ---------------------------------------------------------------------------
# Config loading tests
# ---------------------------------------------------------------------------
class TestCarregarConfiguracao:
    def test_returns_empty_list_when_file_missing(self, tmp_path, monkeypatch):
        import coletor_copa
        monkeypatch.setattr(coletor_copa, "ARQUIVO_CONFIG", str(tmp_path / "nonexistent.json"))
        result = coletor_copa.carregar_configuracao()
        assert result == []

    def test_returns_empty_list_on_invalid_json(self, tmp_path, monkeypatch):
        import coletor_copa
        bad_file = tmp_path / "copas.json"
        bad_file.write_text("this is not json", encoding="utf-8")
        monkeypatch.setattr(coletor_copa, "ARQUIVO_CONFIG", str(bad_file))
        result = coletor_copa.carregar_configuracao()
        assert result == []

    def test_returns_list_on_valid_json(self, tmp_path, monkeypatch):
        import coletor_copa
        data = [{"slug": "copa-teste", "nome_visual": "Copa Teste", "ativa": True}]
        good_file = tmp_path / "copas.json"
        good_file.write_text(json.dumps(data), encoding="utf-8")
        monkeypatch.setattr(coletor_copa, "ARQUIVO_CONFIG", str(good_file))
        result = coletor_copa.carregar_configuracao()
        assert len(result) == 1
        assert result[0]["slug"] == "copa-teste"

    def test_returns_empty_list_on_empty_file(self, tmp_path, monkeypatch):
        import coletor_copa
        empty_file = tmp_path / "copas.json"
        empty_file.write_text("", encoding="utf-8")
        monkeypatch.setattr(coletor_copa, "ARQUIVO_CONFIG", str(empty_file))
        result = coletor_copa.carregar_configuracao()
        assert result == []


# ---------------------------------------------------------------------------
# Token retrieval tests
# ---------------------------------------------------------------------------
class TestGetToken:
    def test_returns_none_when_not_set(self, monkeypatch):
        import coletor_copa
        monkeypatch.delenv("CARTOLA_GLBID", raising=False)
        assert coletor_copa.get_token() is None

    def test_returns_token_when_set(self, monkeypatch):
        import coletor_copa
        monkeypatch.setenv("CARTOLA_GLBID", "fake_bearer_token")
        assert coletor_copa.get_token() == "fake_bearer_token"


# ---------------------------------------------------------------------------
# coletar_dados_copa tests
# ---------------------------------------------------------------------------
class TestColetarDadosCopa:
    def test_returns_early_when_no_copas(self, tmp_path, monkeypatch):
        """Empty config returns immediately without any API call."""
        import coletor_copa
        empty_file = tmp_path / "copas.json"
        empty_file.write_text("[]", encoding="utf-8")
        monkeypatch.setattr(coletor_copa, "ARQUIVO_CONFIG", str(empty_file))
        monkeypatch.setenv("CARTOLA_GLBID", "token")

        # Market status mock
        fake_market = MagicMock()
        fake_market.status_code = 200
        fake_market.json.return_value = {"status_mercado": 1, "rodada_atual": 5}

        with patch("coletor_copa.requests.get", return_value=fake_market) as mock_get:
            coletor_copa.coletar_dados_copa()
            # Only the mercado/status call should happen, no liga calls
            calls = [str(c) for c in mock_get.call_args_list]
            assert all("mercado" in c or len(calls) <= 1 for c in calls)

    def test_handles_market_api_error_gracefully(self, tmp_path, monkeypatch):
        """Network error on mercado/status should not crash."""
        import coletor_copa
        data = [{"slug": "copa", "nome_visual": "Copa", "ativa": True}]
        config_file = tmp_path / "copas.json"
        config_file.write_text(json.dumps(data), encoding="utf-8")
        monkeypatch.setattr(coletor_copa, "ARQUIVO_CONFIG", str(config_file))
        monkeypatch.setenv("CARTOLA_GLBID", "token")

        with patch("coletor_copa.requests.get", side_effect=Exception("network error")):
            coletor_copa.coletar_dados_copa()  # Must not raise


# ---------------------------------------------------------------------------
# caçar_jogos_recursivo tests
# ---------------------------------------------------------------------------
class TestCacarJogosRecursivo:
    def test_finds_game_in_flat_dict(self):
        import coletor_copa
        jogo = {"time_mandante_id": 1, "time_visitante_id": 2, "placar": "2x1"}
        result = coletor_copa.caçar_jogos_recursivo(jogo)
        assert len(result) == 1
        assert result[0]["time_mandante_id"] == 1

    def test_finds_games_in_nested_dict(self):
        import coletor_copa
        dados = {
            "fase1": {
                "jogo_1": {"time_mandante_id": 10, "time_visitante_id": 20},
                "jogo_2": {"time_mandante_id": 30, "time_visitante_id": 40},
            }
        }
        result = coletor_copa.caçar_jogos_recursivo(dados)
        assert len(result) == 2

    def test_finds_games_in_list(self):
        import coletor_copa
        dados = [
            {"time_mandante_id": 1, "time_visitante_id": 2},
            {"other_key": "not_a_game"},
        ]
        result = coletor_copa.caçar_jogos_recursivo(dados)
        assert len(result) == 1

    def test_returns_empty_for_non_game_data(self):
        import coletor_copa
        result = coletor_copa.caçar_jogos_recursivo({"key": "value", "nested": {"x": 1}})
        assert result == []
