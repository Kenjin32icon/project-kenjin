from startup.jobs.trade_guardian import evaluate

def base_report(**overrides):
    data = {
        "asset": "EURUSD", "ticket": 42, "side": "BUY", "open_price": 1.1000,
        "price": 1.0990, "sl": 1.0985, "tp": 1.1030, "lots": 0.1, "profit": -10.0,
        "r_multiple": -0.9, "peak_r": 0.0, "initial_sl_distance": 0.0015,
        "open_seconds": 120.0, "max_hold_seconds": 3600.0, "momentum_against": False,
    }
    data.update(overrides)
    return data

def test_guardian_hard_stop_closes():
    d = evaluate(base_report(), tier2_action="HOLD", micro_trend="NEUTRAL", micro_strength=0.0, market_bias=0.0, market_fresh=False)
    assert d["action"] == "CLOSE"
    assert d["reason"] == "hard_stop"

def test_guardian_holds_safe_position():
    d = evaluate(base_report(price=1.1010, r_multiple=0.4, profit=4.0), tier2_action="HOLD", micro_trend="NEUTRAL", micro_strength=0.0, market_bias=0.0, market_fresh=False)
    assert d["action"] in ("HOLD", "MOVE_SL")

def test_guardian_break_even_ratchet_moves_stop_beyond_entry():
    d = evaluate(
        base_report(price=1.1015, r_multiple=1.1, profit=15.0, sl=1.0985),
        tier2_action="HOLD", micro_trend="NEUTRAL", micro_strength=0.0,
        market_bias=0.0, market_fresh=False,
    )
    assert d["action"] == "MOVE_SL"
    assert d["new_sl"] > 1.1000
