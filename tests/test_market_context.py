import time

from startup.market_context import MarketContextService


def test_stale_asset_context_is_neutral_even_if_other_symbols_are_fresh():
    service = MarketContextService()
    service._snapshot = {
        "ts": time.time(),
        "fresh": True,
        "regime": "risk_on",
        "summary": "stale EURUSD sample",
        "macro": {"DXY": {"momentum": -0.4, "trend": "DOWN", "ret_15m_bps": -10, "bar_age_s": 15}},
        "assets": {"EURUSD": {"momentum": 0.8, "bias": 0.75, "bar_age_s": 900}},
    }

    context = service.get_asset_context("EURUSD")

    assert context["fresh"] is False
    assert context["bias"] == 0.0
    assert context["regime"] == "unknown"


def test_fresh_asset_context_preserves_bounded_bias():
    service = MarketContextService()
    service._snapshot = {
        "ts": time.time(),
        "fresh": True,
        "regime": "neutral",
        "summary": "fresh sample",
        "macro": {"DXY": {"momentum": -0.2, "trend": "DOWN", "ret_15m_bps": -5, "bar_age_s": 20}},
        "assets": {"EURUSD": {"momentum": 0.5, "bias": 0.6, "bar_age_s": 20}},
    }

    context = service.get_asset_context("EURUSD")

    assert context["fresh"] is True
    assert context["bias"] == 0.6
