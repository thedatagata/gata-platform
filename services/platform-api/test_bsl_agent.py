"""
Tests for BSL Agent service.

Tests cover:
1. LLM provider resolution (Ollama, Anthropic, fallback)
2. Keyword fallback model selection
3. API endpoint integration
"""

import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent))


class TestLLMProvider:
    """Test LLM provider resolution."""

    def test_ollama_unavailable_returns_empty_provider(self):
        from llm_provider import _try_ollama, LLMProviderConfig
        config = LLMProviderConfig(ollama_base_url="http://localhost:99999")
        result = _try_ollama(config)
        assert not result.is_available
        assert result.provider_name == "ollama"

    def test_provider_none_returns_empty(self):
        from llm_provider import get_llm_provider
        with patch.dict("os.environ", {"BSL_LLM_PROVIDER": "none"}):
            provider = get_llm_provider(force_refresh=True)
            assert provider.llm is None
            assert provider.provider_name == "none"

    def test_anthropic_without_key_unavailable(self):
        from llm_provider import _try_anthropic, LLMProviderConfig
        with patch.dict("os.environ", {}, clear=True):
            config = LLMProviderConfig()
            result = _try_anthropic(config)
            assert not result.is_available
            assert "ANTHROPIC_API_KEY not set" in result.error_message


class TestKeywordFallback:
    """Test keyword-based model selection."""

    def test_ad_keywords_match_ad_performance(self):
        from bsl_agent import _fallback_keyword_search
        result = _fallback_keyword_search(
            "What is my total ad spend?", "tyrell_corp"
        )
        assert "ad_performance" in result.model_used

    def test_order_keywords_match_orders(self):
        from bsl_agent import _fallback_keyword_search
        result = _fallback_keyword_search(
            "Show me revenue by month", "tyrell_corp"
        )
        assert "orders" in result.model_used

    def test_session_keywords_match_sessions(self):
        from bsl_agent import _fallback_keyword_search
        result = _fallback_keyword_search(
            "What is my conversion rate?", "tyrell_corp"
        )
        assert "sessions" in result.model_used

    def test_unknown_keywords_suggest_models(self):
        from bsl_agent import _fallback_keyword_search
        result = _fallback_keyword_search(
            "Something completely unrelated", "tyrell_corp"
        )
        assert "couldn't determine" in result.answer.lower() or "available models" in result.answer.lower()

    def test_fallback_provider_label(self):
        from bsl_agent import _fallback_keyword_search
        result = _fallback_keyword_search("show spend", "tyrell_corp")
        assert result.provider == "keyword_fallback"


class TestAskEndpoint:
    """Test the /ask API endpoint."""

    def test_ask_invalid_tenant_returns_404(self):
        from main import app
        client = TestClient(app)
        response = client.post(
            "/semantic-layer/nonexistent/ask",
            json={"question": "test"}
        )
        assert response.status_code == 404

    def test_ask_valid_tenant_returns_answer(self):
        from main import app
        client = TestClient(app)
        response = client.post(
            "/semantic-layer/tyrell_corp/ask",
            json={"question": "What is my ad spend?"}
        )
        assert response.status_code == 200
        data = response.json()
        assert "answer" in data
        assert "provider" in data

    def test_llm_status_endpoint(self):
        from main import app
        client = TestClient(app)
        response = client.get("/semantic-layer/llm-status")
        assert response.status_code == 200
        data = response.json()
        assert "provider" in data
        assert "is_available" in data

    def test_ask_max_records_validation(self):
        from main import app
        client = TestClient(app)
        response = client.post(
            "/semantic-layer/tyrell_corp/ask",
            json={"question": "test", "max_records": 9999}
        )
        # Should fail validation (max 1000)
        assert response.status_code == 422
