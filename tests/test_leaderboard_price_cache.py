import importlib
import sys
import types
from pathlib import Path

import pytest


@pytest.fixture()
def app_client(tmp_path, monkeypatch):
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("APP_BASE_URL", "https://example.com")
    if "msal" not in sys.modules:
        sys.modules["msal"] = types.SimpleNamespace(ConfidentialClientApplication=object)
    if "app" in sys.modules:
        del sys.modules["app"]
    app_module = importlib.import_module("app")
    app_module.app.config["TESTING"] = True
    with app_module.app.app_context():
        app_module.db.drop_all()
        app_module.db.create_all()
    return app_module.app.test_client(), app_module


def _seed_competition_with_holdings(app_module, num_members=3, holdings_per_member=2):
    """Two members hold the same symbols so the cache should collapse fetches."""
    with app_module.app.app_context():
        teacher = app_module.User(username="teacher", email="t@example.com")
        teacher.set_password("StrongPass!234")
        app_module.db.session.add(teacher)
        app_module.db.session.flush()
        comp = app_module.Competition(code="LBPERF", name="LB Perf Cup", created_by=teacher.id)
        app_module.db.session.add(comp)
        app_module.db.session.flush()

        for i in range(num_members):
            u = app_module.User(username=f"student{i}", email=f"s{i}@example.com")
            u.set_password("StrongPass!234")
            app_module.db.session.add(u)
            app_module.db.session.flush()
            member = app_module.CompetitionMember(
                user_id=u.id, competition_id=comp.id, cash_balance=50000.0
            )
            app_module.db.session.add(member)
            app_module.db.session.flush()
            for j in range(holdings_per_member):
                symbol = ["AAPL", "MSFT", "TSLA"][j % 3]
                app_module.db.session.add(
                    app_module.CompetitionHolding(
                        competition_member_id=member.id,
                        symbol=symbol,
                        quantity=10,
                        buy_price=100.0,
                    )
                )
        app_module.db.session.commit()


def test_leaderboard_uses_cached_price_and_collapses_repeat_symbol_fetches(app_client, monkeypatch):
    client, app_module = app_client
    _seed_competition_with_holdings(app_module, num_members=3, holdings_per_member=2)

    # Reset the per-process cache so this test is independent.
    app_module._leaderboard_price_cache.clear()

    fetch_calls = []

    def fake_get_current_price(symbol):
        fetch_calls.append(symbol)
        return 150.0

    monkeypatch.setattr(app_module, "get_current_price", fake_get_current_price)

    resp = client.get("/competition/LBPERF/leaderboard")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert len(payload) == 3

    # 3 members x 2 holdings = 6 lookups, but only 2 unique symbols (AAPL, MSFT).
    # With the cache the upstream is hit once per unique symbol, not once per holding.
    unique_symbols_fetched = set(fetch_calls)
    assert unique_symbols_fetched == {"AAPL", "MSFT"}
    assert len(fetch_calls) == 2, f"expected exactly 2 upstream calls, got {len(fetch_calls)}: {fetch_calls}"


def test_leaderboard_cache_expires_after_ttl(app_client, monkeypatch):
    client, app_module = app_client
    _seed_competition_with_holdings(app_module, num_members=1, holdings_per_member=1)
    app_module._leaderboard_price_cache.clear()

    fetch_calls = []

    def fake_get_current_price(symbol):
        fetch_calls.append(symbol)
        return 150.0

    monkeypatch.setattr(app_module, "get_current_price", fake_get_current_price)

    fake_now = {"value": 1_000_000.0}
    monkeypatch.setattr(app_module.time, "time", lambda: fake_now["value"])

    resp1 = client.get("/competition/LBPERF/leaderboard")
    assert resp1.status_code == 200
    assert len(fetch_calls) == 1

    # Within the TTL window — no new upstream call.
    fake_now["value"] += app_module._LEADERBOARD_PRICE_CACHE_TTL_SECONDS - 1
    resp2 = client.get("/competition/LBPERF/leaderboard")
    assert resp2.status_code == 200
    assert len(fetch_calls) == 1

    # After the TTL — refetch.
    fake_now["value"] += 2
    resp3 = client.get("/competition/LBPERF/leaderboard")
    assert resp3.status_code == 200
    assert len(fetch_calls) == 2


def test_buy_endpoint_does_not_use_cached_price(app_client, monkeypatch):
    """Trades must always fetch a fresh price, never a cached one."""
    client, app_module = app_client

    with app_module.app.app_context():
        u = app_module.User(username="trader", email="trader@example.com", cash_balance=100000.0)
        u.set_password("StrongPass!234")
        app_module.db.session.add(u)
        app_module.db.session.commit()

    # Pre-warm the cache with a stale price.
    app_module._leaderboard_price_cache.clear()
    app_module._leaderboard_price_cache["AAPL"] = (app_module.time.time(), 50.0)

    fresh_calls = []

    def fake_get_current_price(symbol):
        fresh_calls.append(symbol)
        return 200.0  # different from the cached 50.0

    monkeypatch.setattr(app_module, "get_current_price", fake_get_current_price)

    resp = client.post(
        "/buy",
        json={"username": "trader", "symbol": "AAPL", "quantity": 1},
    )
    assert resp.status_code == 200, resp.get_json()
    assert fresh_calls == ["AAPL"], "buy must call the uncached fetcher"

    with app_module.app.app_context():
        trader = app_module.User.query.filter_by(username="trader").first()
        # Cash debited at the FRESH price (200), not the stale cache (50).
        assert trader.cash_balance == pytest.approx(100000.0 - 200.0)
