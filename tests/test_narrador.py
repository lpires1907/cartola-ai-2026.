"""
tests/test_narrador.py

Unit tests for narrador.py — covers Gemini caller, month column mapping,
and prompt generation helpers. All external calls are mocked.
"""
import sys
import os
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture(autouse=True)
def mock_bq_client(monkeypatch):
    fake_client = MagicMock()
    monkeypatch.setattr("config.get_bq_client", lambda: fake_client)
    return fake_client


# ---------------------------------------------------------------------------
# Month-column mapping tests
# ---------------------------------------------------------------------------
class TestGetColunaMes:
    def test_rodadas_1_to_8_jan_fev(self):
        import narrador
        for r in [1, 5, 8]:
            col, nome = narrador.get_coluna_mes(r)
            assert col == "pontos_jan_fev"
            assert "Jan" in nome

    def test_rodada_9_marco(self):
        import narrador
        col, nome = narrador.get_coluna_mes(9)
        assert col == "pontos_marco"

    def test_rodada_13_abril(self):
        import narrador
        col, nome = narrador.get_coluna_mes(13)
        assert col == "pontos_abril"

    def test_rodada_17_maio(self):
        import narrador
        col, nome = narrador.get_coluna_mes(17)
        assert col == "pontos_maio"

    def test_rodada_21_jun_jul(self):
        import narrador
        col, nome = narrador.get_coluna_mes(21)
        assert col == "pontos_jun_jul"

    def test_rodada_25_agosto(self):
        import narrador
        col, nome = narrador.get_coluna_mes(25)
        assert col == "pontos_agosto"

    def test_rodada_30_setembro(self):
        import narrador
        col, nome = narrador.get_coluna_mes(30)
        assert col == "pontos_setembro"

    def test_rodada_34_outubro(self):
        import narrador
        col, nome = narrador.get_coluna_mes(34)
        assert col == "pontos_outubro"

    def test_rodada_37_nov_dez(self):
        import narrador
        col, nome = narrador.get_coluna_mes(37)
        assert col == "pontos_nov_dez"


# ---------------------------------------------------------------------------
# Gemini caller tests
# ---------------------------------------------------------------------------
class TestChamarGemini:
    def test_returns_none_when_no_api_key(self, monkeypatch):
        import narrador
        monkeypatch.setattr(narrador, "GEMINI_KEY", None)
        result = narrador.chamar_gemini("test prompt")
        assert result is None

    def test_returns_text_on_success(self, monkeypatch):
        import narrador
        monkeypatch.setattr(narrador, "GEMINI_KEY", "fake-api-key")

        fake_response = MagicMock()
        fake_response.text = "Mocked AI response"

        with patch("narrador.genai.Client") as mock_client_class:
            instance = mock_client_class.return_value
            instance.models.generate_content.return_value = fake_response
            result = narrador.chamar_gemini("some prompt")

        assert result == "Mocked AI response"

    def test_returns_none_on_exception(self, monkeypatch):
        import narrador
        monkeypatch.setattr(narrador, "GEMINI_KEY", "fake-api-key")

        with patch("narrador.genai.Client", side_effect=Exception("API down")):
            result = narrador.chamar_gemini("test")

        assert result is None


# ---------------------------------------------------------------------------
# Prompt generation tests (no Gemini call — just check prompts have data)
# ---------------------------------------------------------------------------
class TestGerarAnaliseRodada:
    def _make_df(self):
        return pd.DataFrame([
            {"nome": "Time Alpha", "pontos": 80.5},
            {"nome": "Time Beta",  "pontos": 72.0},
            {"nome": "Time Gamma", "pontos": 60.0},
            {"nome": "Time Delta", "pontos": 45.5},
        ])

    def test_calls_gemini_with_team_names(self, monkeypatch):
        import narrador
        monkeypatch.setattr(narrador, "GEMINI_KEY", "key")

        captured_prompt = []

        def fake_gemini(prompt):
            captured_prompt.append(prompt)
            return "narration text"

        monkeypatch.setattr(narrador, "chamar_gemini", fake_gemini)
        result = narrador.gerar_analise_rodada(self._make_df(), rodada=5, status_rodada="OFICIAL")

        assert result == "narration text"
        assert "Time Alpha" in captured_prompt[0]
        assert "Time Delta" in captured_prompt[0]

    def test_handles_single_team_gracefully(self, monkeypatch):
        import narrador
        monkeypatch.setattr(narrador, "GEMINI_KEY", "key")
        monkeypatch.setattr(narrador, "chamar_gemini", lambda p: "ok")

        single = pd.DataFrame([{"nome": "Só Time", "pontos": 50.0}])
        # Should not raise IndexError
        narrador.gerar_analise_rodada(single, rodada=1, status_rodada="PARCIAL")
