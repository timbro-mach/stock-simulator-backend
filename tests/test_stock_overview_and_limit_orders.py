import importlib
import sys
import types
from datetime import datetime, timedelta
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


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload


def make_fake_get(current_price=110.0, prev_close=100.0, quote_change=None, quote_change_percent=None):
    today = datetime.utcnow().date()
    d0 = today.isoformat()
    d1 = (today - timedelta(days=1)).isoformat()
    d2 = (today - timedelta(days=2)).isoformat()
    d3 = (today - timedelta(days=3)).isoformat()
    intraday_today_open = f"{d0} 09:35:00"
    intraday_today_mid = f"{d0} 10:00:00"
    intraday_yesterday_close = f"{d1} 15:55:00"
    w0 = today.isoformat()
    w1 = (today - timedelta(days=7)).isoformat()
    w2 = (today - timedelta(days=14)).isoformat()

    def fake_get(url, params=None, timeout=None):
        if params and params.get("function") == "GLOBAL_QUOTE":
            quote = {"05. price": str(current_price), "08. previous close": str(prev_close)}
            if quote_change is not None:
                quote["09. change"] = str(quote_change)
            if quote_change_percent is not None:
                quote["10. change percent"] = str(quote_change_percent)
            return FakeResponse({"Global Quote": quote})
        if params and params.get("function") == "TIME_SERIES_INTRADAY":
            return FakeResponse({
                "Time Series (5min)": {
                    intraday_today_mid: {"4. close": "95"},
                    intraday_today_open: {"4. close": "90"},
                    intraday_yesterday_close: {"4. close": "89"},
                }
            })
        if params and params.get("function") == "TIME_SERIES_DAILY_ADJUSTED":
            return FakeResponse({
                "Time Series (Daily)": {
                    d0: {"4. close": "100"},
                    d1: {"4. close": "98"},
                    d2: {"4. close": "95"},
                    d3: {"4. close": "90"},
                }
            })
        if params and params.get("function") == "TIME_SERIES_WEEKLY_ADJUSTED":
            return FakeResponse({
                "Weekly Adjusted Time Series": {
                    w0: {"4. close": "100"},
                    w1: {"4. close": "96"},
                    w2: {"4. close": "90"},
                }
            })
        raise AssertionError(f"Unexpected params {params}")

    return fake_get


def create_user(app_module, username="tester", email="user@example.com", password="StrongPass!234"):
    with app_module.app.app_context():
        user = app_module.User(username=username, email=email)
        user.set_password(password)
        app_module.db.session.add(user)
        app_module.db.session.commit()


def test_today_metrics_invariant_across_ranges(app_client, monkeypatch):
    _, app_module = app_client
    monkeypatch.setattr(app_module.requests, "get", make_fake_get(current_price=105.0, prev_close=100.0))

    today_values = []
    for r in ["1D", "1W", "1M", "6M", "1Y"]:
        payload = app_module.build_stock_overview("AAPL", r)
        today_values.append((payload["today_change_value"], payload["today_change_percent"]))

    assert len(set(today_values)) == 1
    assert today_values[0] == (5.0, 5.0)




def test_today_metrics_use_global_quote_change_fields(app_client, monkeypatch):
    _, app_module = app_client
    monkeypatch.setattr(
        app_module.requests,
        "get",
        make_fake_get(current_price=123.0, prev_close=100.0, quote_change=7.5, quote_change_percent="6.10%"),
    )

    one_day = app_module.build_stock_overview("AAPL", "1D")
    one_year = app_module.build_stock_overview("AAPL", "1Y")

    assert one_day["today_change_value"] == 7.5
    assert one_day["today_change_percent"] == 6.1
    assert one_year["today_change_value"] == 7.5
    assert one_year["today_change_percent"] == 6.1




def test_one_day_range_uses_today_change_metrics(app_client, monkeypatch):
    _, app_module = app_client
    monkeypatch.setattr(
        app_module.requests,
        "get",
        make_fake_get(current_price=123.0, prev_close=100.0, quote_change=7.5, quote_change_percent="6.10%"),
    )

    payload = app_module.build_stock_overview("AAPL", "1D")

    assert payload["range_start_price"] == 100.0
    assert payload["range_change_value"] == 7.5
    assert payload["range_change_percent"] == 6.1


def test_one_day_chart_points_include_only_today(app_client, monkeypatch):
    _, app_module = app_client
    monkeypatch.setattr(app_module.requests, "get", make_fake_get(current_price=110.0, prev_close=100.0))

    payload = app_module.build_stock_overview("AAPL", "1D")

    assert len(payload["chart_points"]) == 2
    assert all(point["timestamp"].startswith(datetime.utcnow().date().isoformat()) for point in payload["chart_points"])

def test_range_metrics_correctness_and_sorted_points(app_client, monkeypatch):
    _, app_module = app_client
    monkeypatch.setattr(app_module.requests, "get", make_fake_get(current_price=110.0, prev_close=100.0))

    payload = app_module.build_stock_overview("AAPL", "1M")
    assert payload["range_start_price"] == 90.0
    assert payload["range_change_value"] == 20.0
    assert round(payload["range_change_percent"], 4) == round((20.0 / 90.0) * 100, 4)
    timestamps = [p["timestamp"] for p in payload["chart_points"]]
    assert timestamps == sorted(timestamps)


def test_prev_close_fallback_behavior(app_client, monkeypatch):
    _, app_module = app_client
    monkeypatch.setattr(app_module.requests, "get", make_fake_get(current_price=110.0, prev_close=0.0))

    payload = app_module.build_stock_overview("AAPL", "1W")
    assert payload["prev_close_price"] == 98.0
    assert payload["today_change_value"] == 12.0


def test_limit_order_persistence_and_status_transitions(app_client, monkeypatch):
    client, app_module = app_client
    create_user(app_module)

    create_resp = client.post(
        "/orders/limit",
        json={"username": "tester", "symbol": "AAPL", "side": "buy", "quantity": 2, "limit_price": 101},
    )
    assert create_resp.status_code == 201
    order_id = create_resp.get_json()["id"]

    monkeypatch.setattr(app_module, "get_current_price", lambda symbol: 100.0)
    app_module.process_open_limit_orders()

    list_resp = client.get("/orders/limit", query_string={"username": "tester", "status": "filled"})
    assert list_resp.status_code == 200
    filled = list_resp.get_json()
    assert filled[0]["id"] == order_id
    assert filled[0]["status"] == "filled"

    cancel_resp = client.post(f"/orders/limit/{order_id}/cancel", json={"username": "tester"})
    assert cancel_resp.status_code == 200
    assert cancel_resp.get_json()["status"] == "filled"


def test_integration_metrics_consistency_across_requests(app_client, monkeypatch):
    client, app_module = app_client
    monkeypatch.setattr(app_module.requests, "get", make_fake_get(current_price=111.0, prev_close=100.0))

    first = client.get("/stock_overview/AAPL", query_string={"range": "1W"}).get_json()
    second = client.get("/stock_overview/AAPL", query_string={"range": "1W"}).get_json()

    assert first["today_change_value"] == second["today_change_value"]
    assert first["today_change_percent"] == second["today_change_percent"]
    assert first["range_change_value"] == second["range_change_value"]
    assert first["chart_points"] == second["chart_points"]


def test_integration_limit_order_survives_login_session_and_cancel_path(app_client, monkeypatch):
    client, app_module = app_client
    create_user(app_module)

    login_resp = client.post("/login", json={"username": "tester", "password": "StrongPass!234"})
    assert login_resp.status_code == 200

    create_resp = client.post(
        "/orders/limit",
        json={
            "username": "tester",
            "symbol": "MSFT",
            "side": "buy",
            "quantity": 1,
            "limit_price": 300,
            "idempotency_key": "abc-123",
        },
    )
    assert create_resp.status_code == 201
    order = create_resp.get_json()

    login_resp_2 = client.post("/login", json={"username": "tester", "password": "StrongPass!234"})
    assert login_resp_2.status_code == 200

    open_orders = client.get("/orders/limit", query_string={"username": "tester", "status": "open"}).get_json()
    assert any(o["id"] == order["id"] for o in open_orders)

    cancel_resp = client.post(f"/orders/limit/{order['id']}/cancel", json={"username": "tester"})
    assert cancel_resp.status_code == 200
    assert cancel_resp.get_json()["status"] == "cancelled"

    second_create = client.post(
        "/orders/limit",
        json={
            "username": "tester",
            "symbol": "MSFT",
            "side": "buy",
            "quantity": 1,
            "limit_price": 300,
            "idempotency_key": "abc-123",
        },
    )
    assert second_create.status_code == 200
    assert second_create.get_json()["id"] == order["id"]

    monkeypatch.setattr(app_module, "get_current_price", lambda symbol: 250.0)
    app_module.process_open_limit_orders()
    historical = client.get("/orders/limit", query_string={"username": "tester"}).get_json()
    assert any(item["status"] in {"filled", "cancelled"} for item in historical)


def test_trade_blotter_tracks_market_and_limit_trades(app_client, monkeypatch):
    client, app_module = app_client
    create_user(app_module)

    monkeypatch.setattr(app_module, "get_current_price", lambda symbol: 100.0)

    buy_resp = client.post(
        "/buy",
        json={"username": "tester", "symbol": "AAPL", "quantity": 2},
    )
    assert buy_resp.status_code == 200

    create_limit = client.post(
        "/orders/limit",
        json={"username": "tester", "symbol": "AAPL", "side": "sell", "quantity": 1, "limit_price": 90},
    )
    assert create_limit.status_code == 201

    app_module.process_open_limit_orders()

    blotter_resp = client.get("/trades/blotter", query_string={"username": "tester"})
    assert blotter_resp.status_code == 200
    rows = blotter_resp.get_json()

    assert len(rows) == 2
    assert rows[0]["order_type"] == "limit"
    assert rows[0]["side"] == "sell"
    assert rows[1]["order_type"] == "market"
    assert rows[1]["side"] == "buy"


def test_trade_blotter_requires_valid_params(app_client):
    client, app_module = app_client
    create_user(app_module)

    missing_identity = client.get("/trades/blotter")
    assert missing_identity.status_code == 400

    invalid_limit = client.get("/trades/blotter", query_string={"username": "tester", "limit": "abc"})
    assert invalid_limit.status_code == 400


def test_trade_blotter_supports_trade_history_alias_and_user_id(app_client, monkeypatch):
    client, app_module = app_client
    create_user(app_module)

    monkeypatch.setattr(app_module, "get_current_price", lambda symbol: 100.0)
    buy_resp = client.post("/buy", json={"username": "tester", "symbol": "AAPL", "quantity": 1})
    assert buy_resp.status_code == 200

    with app_module.app.app_context():
        user = app_module.User.query.filter_by(username="tester").first()
        user_id = user.id

    alias_resp = client.get("/trade-history", query_string={"userId": user_id})
    assert alias_resp.status_code == 200
    rows = alias_resp.get_json()
    assert len(rows) == 1
    assert rows[0]["side"] == "buy"
