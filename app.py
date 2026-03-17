from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
import requests, secrets
from datetime import datetime, timedelta, timezone
from dateutil import tz
import logging
logging.basicConfig(level=logging.INFO)
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import os
import hashlib
import msal

logger = logging.getLogger(__name__)

PASSWORD_RESET_TOKEN_BYTES = 32
PASSWORD_RESET_TOKEN_TTL_MINUTES = 60
PASSWORD_RESET_RATE_LIMIT_IP = 5
PASSWORD_RESET_RATE_LIMIT_EMAIL = 3

app = Flask(__name__)
CORS(app, origins=[
    "https://stock-simulator-frontend.vercel.app",
    "https://simulator.gostockpro.com",
    "http://localhost:3000"
])

# --- Database Configuration ---
raw_db_url = os.getenv("DATABASE_URL", "").strip()

if not raw_db_url:
    # fallback for local dev
    raw_db_url = "sqlite:///local.db"
else:
    # ensure sslmode=require is appended only once
    if "sslmode" not in raw_db_url:
        if raw_db_url.endswith("/"):
            raw_db_url = raw_db_url[:-1]
        raw_db_url += "?sslmode=require"

app.config["SQLALCHEMY_DATABASE_URI"] = raw_db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

print(f"✅ Connected to database: {app.config['SQLALCHEMY_DATABASE_URI']}")
# ----------------------------------

# ----------------------------------


# Alpha Vantage API Key
ALPHA_VANTAGE_API_KEY = "2QZ58MHB8CG5PYYJ"

# --------------------
# Models
# --------------------
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=True)
    password_hash = db.Column(db.String(128), nullable=False)
    cash_balance = db.Column(db.Float, default=100000)
    is_admin = db.Column(db.Boolean, default=False)  # Admin flag
    start_of_day_value = db.Column(db.Float, default=100000.0)  # Daily P&L anchor
    realized_pnl = db.Column(db.Float, default=0.0)

   

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

class PasswordResetToken(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    token_hash = db.Column(db.String(64), unique=True, nullable=False, index=True)
    expires_at = db.Column(db.DateTime, nullable=False)
    used_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    request_ip = db.Column(db.String(45), nullable=True)
    user_agent = db.Column(db.String(256), nullable=True)

class PasswordResetRequest(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email_hash = db.Column(db.String(64), nullable=True, index=True)
    request_ip = db.Column(db.String(45), nullable=True, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)

class Holding(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    symbol = db.Column(db.String(10), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    buy_price = db.Column(db.Float, nullable=False)

class Competition(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(16), unique=True, nullable=False)
    name = db.Column(db.String(80), nullable=True)
    created_by = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    start_date = db.Column(db.DateTime, nullable=True)
    end_date = db.Column(db.DateTime, nullable=True)
    featured = db.Column(db.Boolean, default=False)
    max_position_limit = db.Column(db.String(10), nullable=True)
    is_open = db.Column(db.Boolean, default=True)  # True for open; False for restricted

class CompetitionMember(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    competition_id = db.Column(db.Integer, db.ForeignKey('competition.id'), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    cash_balance = db.Column(db.Float, default=100000)
    start_of_day_value = db.Column(db.Float, default=100000.0)
    realized_pnl = db.Column(db.Float, default=0.0)
    __table_args__ = (db.UniqueConstraint('competition_id', 'user_id', name='_competition_user_uc'),)

class CompetitionHolding(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    competition_member_id = db.Column(db.Integer, db.ForeignKey('competition_member.id'), nullable=False)
    symbol = db.Column(db.String(10), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    buy_price = db.Column(db.Float, nullable=False)


# --------------------
# New Models for Teams
# --------------------
class Team(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), nullable=False)
    created_by = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    cash_balance = db.Column(db.Float, default=100000)
    realized_pnl = db.Column(db.Float, default=0.0)

class TeamMember(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    __table_args__ = (db.UniqueConstraint('team_id', 'user_id', name='_team_user_uc'),)

class TeamHolding(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False)
    symbol = db.Column(db.String(10), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    buy_price = db.Column(db.Float, nullable=False)

# Model for teams joining competitions
class CompetitionTeam(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    competition_id = db.Column(db.Integer, db.ForeignKey('competition.id'), nullable=False)
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False)
    cash_balance = db.Column(db.Float, default=100000)
    start_of_day_value = db.Column(db.Float, default=100000.0)
    realized_pnl = db.Column(db.Float, default=0.0)
    __table_args__ = (db.UniqueConstraint('competition_id', 'team_id', name='_competition_team_uc'),)

class CompetitionTeamHolding(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    competition_team_id = db.Column(db.Integer, db.ForeignKey('competition_team.id'), nullable=False)
    symbol = db.Column(db.String(10), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    buy_price = db.Column(db.Float, nullable=False)


class LimitOrder(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    symbol = db.Column(db.String(10), nullable=False)
    side = db.Column(db.String(8), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    limit_price = db.Column(db.Float, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    status = db.Column(db.String(20), nullable=False, default="open", index=True)
    account_context = db.Column(db.String(32), nullable=False, default="global")
    filled_qty = db.Column(db.Integer, nullable=False, default=0)
    avg_fill_price = db.Column(db.Float, nullable=True)


class TradeBlotterEntry(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    symbol = db.Column(db.String(10), nullable=False)
    side = db.Column(db.String(8), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    price = db.Column(db.Float, nullable=False)
    order_type = db.Column(db.String(16), nullable=False, default='market')
    account_context = db.Column(db.String(32), nullable=False, default='global')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False, index=True)

with app.app_context():
    db.create_all()

# --------------------
# Helper Functions: Password Reset
# --------------------
def normalize_email(email):
    return email.strip().lower() if email else None

def hash_value(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def generate_reset_token():
    return secrets.token_urlsafe(PASSWORD_RESET_TOKEN_BYTES)

def is_password_strong(password):
    if not password or len(password) < 12:
        return False
    categories = 0
    categories += any(c.islower() for c in password)
    categories += any(c.isupper() for c in password)
    categories += any(c.isdigit() for c in password)
    categories += any(not c.isalnum() for c in password)
    return categories >= 3

def send_reset_email(recipient_email, reset_url, expires_minutes):
    tenant_id = os.getenv("MS_TENANT_ID")
    client_id = os.getenv("MS_CLIENT_ID")
    client_secret = os.getenv("MS_CLIENT_SECRET")
    sender_email = os.getenv("MS_SENDER_EMAIL")

    if not tenant_id or not client_id or not client_secret or not sender_email:
        logger.warning("Microsoft Graph email not configured; missing tenant/client/sender settings.")
        return

    authority = f"https://login.microsoftonline.com/{tenant_id}"
    graph_app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority
    )
    token_result = graph_app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )
    access_token = token_result.get("access_token")
    if not access_token:
        logger.warning("Microsoft Graph token acquisition failed. error=%s", token_result.get("error"))
        return

    html_body = (
        "<p>Reset your Stock Simulator password</p>"
        f'<p><a href="{reset_url}" '
        'style="padding:10px 16px;background:#0f172a;color:#fff;text-decoration:none;border-radius:6px;">'
        "Reset Password</a></p>"
        f"<p>This link expires in {expires_minutes} minutes.</p>"
        "<p>If you didn’t request this, you can safely ignore this email.</p>"
    )

    payload = {
        "message": {
            "subject": "Reset your Stock Simulator password",
            "body": {
                "contentType": "HTML",
                "content": html_body
            },
            "toRecipients": [
                {"emailAddress": {"address": recipient_email}}
            ]
        },
        "saveToSentItems": "false"
    }

    try:
        response = requests.post(
            f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=10
        )
        if 200 <= response.status_code < 300:
            logger.info("Microsoft Graph sendMail success status=%s", response.status_code)
        else:
            logger.warning(
                "Microsoft Graph sendMail failed status=%s response=%s",
                response.status_code,
                response.text
            )
    except requests.RequestException as exc:
        logger.exception("Microsoft Graph sendMail exception: %s", exc)

def record_password_reset_request(email_hash, request_ip):
    entry = PasswordResetRequest(
        email_hash=email_hash,
        request_ip=request_ip
    )
    db.session.add(entry)
    db.session.commit()

def is_rate_limited(email_hash, request_ip):
    cutoff = datetime.utcnow() - timedelta(minutes=1)
    ip_count = PasswordResetRequest.query.filter(
        PasswordResetRequest.request_ip == request_ip,
        PasswordResetRequest.created_at >= cutoff
    ).count()
    email_count = PasswordResetRequest.query.filter(
        PasswordResetRequest.email_hash == email_hash,
        PasswordResetRequest.created_at >= cutoff
    ).count()
    return ip_count >= PASSWORD_RESET_RATE_LIMIT_IP or email_count >= PASSWORD_RESET_RATE_LIMIT_EMAIL

# --------------------
# Helper Function: Fetch current price from Alpha Vantage
# --------------------
def get_current_price(symbol):
    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&entitlement=realtime&apikey={ALPHA_VANTAGE_API_KEY}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Alpha Vantage API error: {response.status_code}")
    data = response.json()
    if "Global Quote" not in data or not data["Global Quote"]:
        raise Exception(f"No data found for symbol {symbol}")
    global_quote = data["Global Quote"]
    if "05. price" not in global_quote:
        raise Exception(f"No price information available for symbol {symbol}")
    return float(global_quote["05. price"])


def get_current_and_prev_close(symbol):
    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&entitlement=realtime&apikey={ALPHA_VANTAGE_API_KEY}"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Alpha Vantage API error: {response.status_code}")
    data = response.json()
    if "Global Quote" not in data or not data["Global Quote"]:
        raise Exception(f"No data found for symbol {symbol}")

    global_quote = data["Global Quote"]
    current_price = float(global_quote.get("05. price") or 0.0)
    prev_close = float(global_quote.get("08. previous close") or 0.0)
    if prev_close <= 0:
        prev_close = current_price
    return current_price, prev_close


VALID_RANGES = {"1D", "1W", "1M", "6M", "1Y"}


def _round_metric(value, places=4):
    if value is None:
        return None
    return round(float(value), places)


def _market_session(now_est):
    if now_est.weekday() >= 5:
        return "closed"
    market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
    regular_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
    post_close = now_est.replace(hour=20, minute=0, second=0, microsecond=0)
    if now_est < market_open:
        return "pre"
    if now_est <= regular_close:
        return "regular"
    if now_est <= post_close:
        return "post"
    return "closed"


def _fetch_alpha_vantage(params):
    response = requests.get("https://www.alphavantage.co/query", params=params, timeout=15)
    if response.status_code != 200:
        raise Exception(f"Alpha Vantage API error: {response.status_code}")
    data = response.json()
    if isinstance(data, dict) and data.get("Note"):
        raise Exception(data["Note"])
    return data


def _parse_chart_points(time_series):
    points = []
    for ts, point in time_series.items():
        price_val = point.get("4. close") or point.get("5. adjusted close")
        if not price_val:
            continue
        ts_normalized = ts.replace(" ", "T")
        if len(ts_normalized) == 10:
            ts_normalized = f"{ts_normalized}T00:00:00"
        dt = datetime.fromisoformat(ts_normalized)
        points.append({"timestamp": dt, "price": float(price_val)})
    points.sort(key=lambda x: x["timestamp"])
    return points


def _range_window(range_param, now_est):
    if range_param == "1D":
        return now_est.replace(hour=0, minute=0, second=0, microsecond=0)
    if range_param == "1W":
        return now_est - timedelta(days=7)
    if range_param == "1M":
        return now_est - timedelta(days=30)
    if range_param == "6M":
        return now_est - timedelta(days=182)
    return now_est - timedelta(days=365)


def build_stock_overview(symbol, range_param):
    range_param = (range_param or "1M").upper()
    if range_param not in VALID_RANGES:
        raise ValueError("Invalid range")

    now_utc = datetime.now(timezone.utc)
    now_est = now_utc.astimezone(pytz.timezone("America/New_York"))
    window_start_est = _range_window(range_param, now_est)

    quote_data = _fetch_alpha_vantage({
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "entitlement": "realtime",
        "apikey": ALPHA_VANTAGE_API_KEY,
    })
    global_quote = quote_data.get("Global Quote") or {}
    current_price = float(global_quote.get("05. price") or 0)
    prev_close_price = float(global_quote.get("08. previous close") or 0)

    if range_param == "1D":
        series_params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": "5min",
            "apikey": ALPHA_VANTAGE_API_KEY,
        }
        ts_key_resolver = lambda d: next((k for k in d if "Time Series" in k), None)
    elif range_param in {"1W", "1M"}:
        series_params = {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol, "apikey": ALPHA_VANTAGE_API_KEY}
        ts_key_resolver = lambda _: "Time Series (Daily)"
    else:
        series_params = {"function": "TIME_SERIES_WEEKLY_ADJUSTED", "symbol": symbol, "apikey": ALPHA_VANTAGE_API_KEY}
        ts_key_resolver = lambda _: "Weekly Adjusted Time Series"

    series_data = _fetch_alpha_vantage(series_params)
    ts_key = ts_key_resolver(series_data)
    if not ts_key or ts_key not in series_data:
        raise Exception(f"No valid chart data found for symbol {symbol}")

    points = _parse_chart_points(series_data[ts_key])
    filtered_points = []
    est_tz = pytz.timezone("America/New_York")
    for point in points:
        if point["timestamp"].tzinfo is None:
            point_est = est_tz.localize(point["timestamp"])
        else:
            point_est = point["timestamp"].astimezone(est_tz)
        if point_est >= window_start_est:
            filtered_points.append(point)

    if not filtered_points and points:
        filtered_points = [points[-1]]

    if prev_close_price <= 0:
        daily_data = _fetch_alpha_vantage({"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol, "apikey": ALPHA_VANTAGE_API_KEY})
        daily_series = daily_data.get("Time Series (Daily)", {})
        prev_close_candidates = sorted(daily_series.items(), key=lambda item: item[0], reverse=True)
        if len(prev_close_candidates) > 1:
            prev_close_price = float(prev_close_candidates[1][1].get("4. close") or prev_close_candidates[1][1].get("5. adjusted close") or 0)

    # Keep today's metrics canonical across all ranges by deriving from
    # current and previous close instead of provider change fields, which can
    # intermittently be stale/zero on intraday responses.
    today_change_value = current_price - prev_close_price
    today_change_percent = (today_change_value / prev_close_price * 100.0) if prev_close_price > 0 else None

    if range_param == "1D" and prev_close_price > 0:
        range_start_price = prev_close_price
        range_change_value = today_change_value
        range_change_percent = today_change_percent
    else:
        range_start_price = filtered_points[0]["price"] if filtered_points else current_price
        range_change_value = current_price - range_start_price
        range_change_percent = (range_change_value / range_start_price * 100.0) if range_start_price > 0 else None

    app.logger.info(
        "stock_overview_metrics symbol=%s range=%s as_of=%s current=%s prev_close=%s range_start=%s",
        symbol,
        range_param,
        now_utc.isoformat(),
        current_price,
        prev_close_price,
        range_start_price,
    )
    if prev_close_price <= 0 or range_start_price <= 0:
        app.logger.warning("metric_warning_missing_baseline symbol=%s range=%s", symbol, range_param)

    chart_points = [
        {
            "timestamp": p["timestamp"].isoformat(),
            "price": _round_metric(p["price"]),
        }
        for p in filtered_points
    ]

    latest_point_ts = chart_points[-1]["timestamp"] if chart_points else now_utc.isoformat()
    is_stale = False
    if chart_points:
        latest_ts = datetime.fromisoformat(latest_point_ts)
        if latest_ts.tzinfo is None:
            latest_ts = latest_ts.replace(tzinfo=timezone.utc)
        age_seconds = (now_utc - latest_ts.astimezone(timezone.utc)).total_seconds()
        is_stale = age_seconds > 900
        if is_stale:
            app.logger.warning("provider_data_stale symbol=%s range=%s latest_point_ts=%s age_seconds=%s", symbol, range_param, latest_point_ts, age_seconds)

    app.logger.info("provider_snapshot symbol=%s quote_timestamp=%s chart_latest=%s", symbol, now_utc.isoformat(), latest_point_ts)

    return {
        "symbol": symbol.upper(),
        "as_of_timestamp": now_utc.isoformat(),
        "current_price": _round_metric(current_price),
        "prev_close_price": _round_metric(prev_close_price),
        "today_change_value": _round_metric(today_change_value),
        "today_change_percent": _round_metric(today_change_percent),
        "range": range_param,
        "range_start_price": _round_metric(range_start_price),
        "range_change_value": _round_metric(range_change_value),
        "range_change_percent": _round_metric(range_change_percent),
        "chart_points": chart_points,
        "metadata": {
            "price_source": "alpha_vantage",
            "timezone": "America/New_York",
            "market_session": _market_session(now_est),
            "is_stale": is_stale,
        },
    }

# --------------------
# Endpoints for Registration and Login
# --------------------
@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    email = data.get('email')
    if User.query.filter_by(username=username).first():
        return jsonify({'message': 'User already exists'}), 400
    if User.query.filter_by(email=email).first():
        return jsonify({'message': 'Email already in use'}), 400
    new_user = User(username=username, email=email)
    new_user.set_password(password)
    db.session.add(new_user)
    db.session.commit()
    return jsonify({'message': 'User created successfully'})

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    user = User.query.filter_by(username=username).first()

    if user and user.check_password(password):
        # --- Competition Accounts ---
        competition_accounts = []
        memberships = CompetitionMember.query.filter_by(user_id=user.id).all()
        for m in memberships:
            comp = db.session.get(Competition, m.competition_id)
            if comp:
                comp_holdings = CompetitionHolding.query.filter_by(competition_member_id=m.id).all()
                comp_portfolio = []
                total_holdings_value = 0
                comp_pnl = 0

                for ch in comp_holdings:
                    try:
                        price = get_current_price(ch.symbol)
                    except Exception:
                        price = 0
                    value = price * ch.quantity
                    pnl = (price - ch.buy_price) * ch.quantity
                    comp_pnl += pnl
                    total_holdings_value += value
                    comp_portfolio.append({
                        "symbol": ch.symbol,
                        "quantity": ch.quantity,
                        "current_price": price,
                        "total_value": value,
                        "buy_price": ch.buy_price
                    })

                total_value = m.cash_balance + total_holdings_value
                total_pnl = total_value - 100000
                return_pct = (total_pnl / 100000) * 100

                competition_accounts.append({
                    "code": comp.code,
                    "name": comp.name,
                    "cash_balance": m.cash_balance,
                    "portfolio": comp_portfolio,
                    "total_value": total_value,
                    "pnl": total_pnl,
                    "return_pct": return_pct,
                    "realized_pnl": m.realized_pnl or 0.0,
                })

        # --- Team Competitions ---
        team_memberships = TeamMember.query.filter_by(user_id=user.id).all()
        team_competitions = []
        for tm in team_memberships:
            ct_entries = CompetitionTeam.query.filter_by(team_id=tm.team_id).all()
            for ct in ct_entries:
                comp = db.session.get(Competition, ct.competition_id)
                if comp:
                    ct_holdings = CompetitionTeamHolding.query.filter_by(competition_team_id=ct.id).all()
                    team_portfolio = []
                    total_holdings_value = 0
                    team_pnl = 0

                    for cht in ct_holdings:
                        try:
                            price = get_current_price(cht.symbol)
                        except Exception:
                            price = 0
                        value = price * cht.quantity
                        pnl = (price - cht.buy_price) * cht.quantity
                        team_pnl += pnl
                        total_holdings_value += value
                        team_portfolio.append({
                            "symbol": cht.symbol,
                            "quantity": cht.quantity,
                            "current_price": price,
                            "total_value": value,
                            "buy_price": cht.buy_price
                        })

                    total_value = ct.cash_balance + total_holdings_value
                    total_pnl = total_value - 100000
                    return_pct = (total_pnl / 100000) * 100

                    team_competitions.append({
                        "code": comp.code,
                        "name": comp.name,
                        "cash_balance": ct.cash_balance,
                        "portfolio": team_portfolio,
                        "total_value": total_value,
                        "pnl": total_pnl,
                        "return_pct": return_pct,
                        'realized_pnl': ct.realized_pnl or 0.0,
                        "team_id": ct.team_id
                    })

        return jsonify({
            'message': 'Login successful',
            'username': user.username,
            'cash_balance': user.cash_balance,
            'is_admin': user.is_admin,
            'competition_accounts': competition_accounts,
            'team_competitions': team_competitions
        }), 200

    # --- Invalid credentials ---
    else:
        return jsonify({'message': 'Invalid credentials'}), 401


# --------------------
# Password Reset Endpoints
# --------------------
@app.route('/api/auth/forgot-password', methods=['POST'])
def forgot_password():
    data = request.get_json() or {}
    email = normalize_email(data.get('email'))
    email_hash = hash_value(email) if email else hash_value("")
    request_ip = (request.headers.get("X-Forwarded-For", request.remote_addr) or "").split(",")[0].strip()
    user_agent = request.headers.get("User-Agent", "")

    rate_limited = is_rate_limited(email_hash, request_ip)
    record_password_reset_request(email_hash, request_ip)

    if rate_limited:
        logger.info(
            "password_reset_rate_limited email_hash=%s ip=%s user_agent=%s",
            email_hash,
            request_ip,
            user_agent
        )
        return jsonify({'message': 'If an account exists for that email, we sent a reset link.'}), 200

    user = User.query.filter(func.lower(User.email) == email).first() if email else None
    if user:
        logger.info("password_reset_user_lookup found user_id=%s", user.id)
        now = datetime.utcnow()
        PasswordResetToken.query.filter(
            PasswordResetToken.user_id == user.id,
            PasswordResetToken.used_at.is_(None),
            PasswordResetToken.expires_at > now
        ).update({PasswordResetToken.used_at: now}, synchronize_session=False)
        raw_token = generate_reset_token()
        token_hash = hash_value(raw_token)
        expires_at = now + timedelta(minutes=PASSWORD_RESET_TOKEN_TTL_MINUTES)
        reset_token = PasswordResetToken(
            user_id=user.id,
            token_hash=token_hash,
            expires_at=expires_at,
            request_ip=request_ip,
            user_agent=user_agent[:256]
        )
        db.session.add(reset_token)
        db.session.commit()

        app_base_url = os.getenv("APP_BASE_URL", "").rstrip("/")
        if app_base_url:
            reset_url = f"{app_base_url}/reset-password?token={raw_token}"
            send_reset_email(user.email, reset_url, PASSWORD_RESET_TOKEN_TTL_MINUTES)
        else:
            logger.warning("APP_BASE_URL not set; unable to send reset link.")

        logger.info(
            "password_reset_requested user_id=%s email_hash=%s ip=%s user_agent=%s",
            user.id,
            email_hash,
            request_ip,
            user_agent
        )
    else:
        logger.info("password_reset_user_lookup not_found")
        logger.info(
            "password_reset_requested email_hash=%s ip=%s user_agent=%s",
            email_hash,
            request_ip,
            user_agent
        )

    return jsonify({'message': 'If an account exists for that email, we sent a reset link.'}), 200

@app.route('/api/auth/reset-password', methods=['POST'])
def reset_password():
    data = request.get_json() or {}
    raw_token = data.get('token')
    new_password = data.get('newPassword')

    if not raw_token:
        return jsonify({'message': 'Token is required.'}), 400
    if not is_password_strong(new_password):
        return jsonify({'message': 'Password does not meet strength requirements.'}), 400

    token_hash = hash_value(raw_token)
    token_record = PasswordResetToken.query.filter_by(token_hash=token_hash).first()
    if not token_record:
        return jsonify({'message': 'Invalid or expired token.'}), 400
    if token_record.used_at is not None or token_record.expires_at < datetime.utcnow():
        return jsonify({'message': 'Invalid or expired token.'}), 400

    user = db.session.get(User, token_record.user_id)
    if not user:
        return jsonify({'message': 'Invalid or expired token.'}), 400

    user.set_password(new_password)
    token_record.used_at = datetime.utcnow()
    db.session.commit()

    logger.info(
        "password_reset_completed user_id=%s token_id=%s",
        user.id,
        token_record.id
    )

    return jsonify({'message': 'Password updated successfully.'}), 200


# --------------------
# Endpoint for Global User Data (including team competition accounts)
# --------------------
@app.route('/user', methods=['GET'])
def get_user():
    username = request.args.get('username')
    if not username:
        return jsonify({'message': 'Username is required'}), 400

    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    # --- Global Account ---
    holdings = Holding.query.filter_by(user_id=user.id).all()
    global_portfolio = []
    global_total_holdings_value = 0
    global_unrealized_pnl = 0
    global_holdings_prev_close_value = 0

    for h in holdings:
        try:
            price, prev_close = get_current_and_prev_close(h.symbol)
        except Exception:
            price, prev_close = 0, 0
        value = price * h.quantity
        pnl = (price - h.buy_price) * h.quantity
        global_unrealized_pnl += pnl
        global_total_holdings_value += value
        global_holdings_prev_close_value += prev_close * h.quantity
        global_portfolio.append({
            'symbol': h.symbol,
            'quantity': h.quantity,
            'current_price': price,
            'total_value': value,
            'buy_price': h.buy_price
        })

    global_total_pnl = (user.realized_pnl or 0.0) + global_unrealized_pnl
    global_total_value = user.cash_balance + global_total_holdings_value
    global_return_pct = ((global_total_value - 100000.0) / 100000.0) * 100.0
    global_start_of_day_value = user.cash_balance + global_holdings_prev_close_value
    global_pnl_today = global_total_value - global_start_of_day_value
    global_pnl_pct_today = (global_pnl_today / global_start_of_day_value * 100.0) if global_start_of_day_value > 0 else 0.0

    # --- Individual Competition Accounts ---
    competition_accounts = []
    memberships = CompetitionMember.query.filter_by(user_id=user.id).all()
    for m in memberships:
        comp = db.session.get(Competition, m.competition_id)
        if not comp:
            continue

        comp_holdings = CompetitionHolding.query.filter_by(competition_member_id=m.id).all()
        comp_portfolio = []
        comp_total_holdings_value = 0
        comp_unrealized_pnl = 0
        comp_holdings_prev_close_value = 0

        for ch in comp_holdings:
            try:
                price, prev_close = get_current_and_prev_close(ch.symbol)
            except Exception:
                price, prev_close = 0, 0
            value = price * ch.quantity
            pnl = (price - ch.buy_price) * ch.quantity
            comp_unrealized_pnl += pnl
            comp_total_holdings_value += value
            comp_holdings_prev_close_value += prev_close * ch.quantity
            comp_portfolio.append({
                'symbol': ch.symbol,
                'quantity': ch.quantity,
                'current_price': price,
                'total_value': value,
                'buy_price': ch.buy_price
            })

        comp_total_pnl = (m.realized_pnl or 0.0) + comp_unrealized_pnl
        comp_total_value = m.cash_balance + comp_total_holdings_value
        comp_return_pct = ((comp_total_value - 100000.0) / 100000.0) * 100.0
        comp_start_of_day_value = m.cash_balance + comp_holdings_prev_close_value
        comp_pnl_today = comp_total_value - comp_start_of_day_value
        comp_pnl_pct_today = (comp_pnl_today / comp_start_of_day_value * 100.0) if comp_start_of_day_value > 0 else 0.0

        competition_accounts.append({
            'code': comp.code,
            'name': comp.name,
            'cash_balance': m.cash_balance,
            'portfolio': comp_portfolio,
            'total_value': comp_total_value,
            'pnl': comp_total_pnl,
            'return_pct': comp_return_pct,
            'realized_pnl': m.realized_pnl or 0.0,
            'start_of_day_value': comp_start_of_day_value,
            'pnl_today': comp_pnl_today,
            'pnl_pct_today': comp_pnl_pct_today
        })

    # --- Team Competitions ---
    team_competitions = []
    team_memberships = TeamMember.query.filter_by(user_id=user.id).all()
    for tm in team_memberships:
        ct_entries = CompetitionTeam.query.filter_by(team_id=tm.team_id).all()
        for ct in ct_entries:
            comp = db.session.get(Competition, ct.competition_id)
            if not comp:
                continue

            ct_holdings = CompetitionTeamHolding.query.filter_by(competition_team_id=ct.id).all()
            team_portfolio = []
            team_total_holdings_value = 0
            team_unrealized_pnl = 0
            team_holdings_prev_close_value = 0

            for cht in ct_holdings:
                try:
                    price, prev_close = get_current_and_prev_close(cht.symbol)
                except Exception:
                    price, prev_close = 0, 0
                value = price * cht.quantity
                pnl = (price - cht.buy_price) * cht.quantity
                team_unrealized_pnl += pnl
                team_total_holdings_value += value
                team_holdings_prev_close_value += prev_close * cht.quantity
                team_portfolio.append({
                    'symbol': cht.symbol,
                    'quantity': cht.quantity,
                    'current_price': price,
                    'total_value': value,
                    'buy_price': cht.buy_price
                })

            team_total_pnl = (ct.realized_pnl or 0.0) + team_unrealized_pnl
            team_total_value = ct.cash_balance + team_total_holdings_value
            team_return_pct = ((team_total_value - 100000.0) / 100000.0) * 100.0
            team_start_of_day_value = ct.cash_balance + team_holdings_prev_close_value
            team_pnl_today = team_total_value - team_start_of_day_value
            team_pnl_pct_today = (team_pnl_today / team_start_of_day_value * 100.0) if team_start_of_day_value > 0 else 0.0

            team_competitions.append({
                'code': comp.code,
                'name': comp.name,
                'cash_balance': ct.cash_balance,
                'portfolio': team_portfolio,
                'total_value': team_total_value,
                'team_id': ct.team_id,
                'pnl': team_total_pnl,
                'return_pct': team_return_pct,
                'realized_pnl': ct.realized_pnl or 0.0,
                'start_of_day_value': team_start_of_day_value,
                'pnl_today': team_pnl_today,
                'pnl_pct_today': team_pnl_pct_today
            })

    # --- Final Response ---
    response_data = {
        'username': user.username,
        'is_admin': user.is_admin,
        'global_account': {
            'cash_balance': user.cash_balance,
            'portfolio': global_portfolio,
            'total_value': global_total_value,
            'pnl': global_total_pnl,
            'realized_pnl': user.realized_pnl,
            'return_pct': global_return_pct,
            'start_of_day_value': global_start_of_day_value,
            'pnl_today': global_pnl_today,
            'pnl_pct_today': global_pnl_pct_today
        },
        'competition_accounts': competition_accounts,
        'team_competitions': team_competitions
    }

    return jsonify(response_data)


# --------------------
# Stock Endpoints
# --------------------
@app.route('/stock/<symbol>', methods=['GET'])
def get_stock(symbol):
    try:
        app.logger.info(f"Fetching current price for {symbol}")
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&entitlement=realtime&apikey={ALPHA_VANTAGE_API_KEY}"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Alpha Vantage API error: {response.status_code}")
        data = response.json()
        if "Global Quote" not in data or not data["Global Quote"]:
            raise Exception(f"No data found for symbol {symbol}")
        global_quote = data["Global Quote"]
        if "05. price" not in global_quote:
            raise Exception(f"No price information available for symbol {symbol}")
        price = float(global_quote["05. price"])
        return jsonify({'symbol': symbol, 'price': price})
    except Exception as e:
        app.logger.error(f"Error fetching data for {symbol}: {e}")
        return jsonify({'error': f'Failed to fetch data for symbol {symbol}: {str(e)}'}), 400

@app.route('/stock_chart/<symbol>', methods=['GET'])
def stock_chart(symbol):
    """
    Dynamic chart endpoint supporting range queries:
    /stock_chart/AAPL?range=1D|1W|1M|6M|1Y
    """
    range_param = request.args.get("range", "1M").upper()

    try:
        overview = build_stock_overview(symbol, range_param)
        chart_data = [{"date": p["timestamp"], "close": p["price"]} for p in overview["chart_points"]]
        return jsonify(chart_data)
    except Exception as e:
        app.logger.error(f"Error fetching chart data for {symbol}: {e}")
        return jsonify({"error": f"Failed to fetch chart data for {symbol}: {str(e)}"}), 400


@app.route('/stock_overview/<symbol>', methods=['GET'])
def stock_overview(symbol):
    range_param = request.args.get("range", "1M").upper()
    try:
        overview = build_stock_overview(symbol, range_param)
        return jsonify(overview)
    except ValueError:
        return jsonify({"error": "range must be one of 1D,1W,1M,6M,1Y"}), 400
    except Exception as e:
        app.logger.error("Error generating stock overview for %s: %s", symbol, e)
        return jsonify({"error": f"Failed to fetch overview for symbol {symbol}: {str(e)}"}), 400


# --------------------
# Global Trading Endpoints
# --------------------
@app.route('/buy', methods=['POST'])
def buy_stock():
    data = request.get_json()
    username = data.get('username')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))
    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': f'Error fetching price for symbol {symbol}: {str(e)}'}), 400
    
    
    cost = quantity * price
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    if user.cash_balance < cost:
        return jsonify({'message': 'Insufficient funds'}), 400
    user.cash_balance -= cost
    existing = Holding.query.filter_by(user_id=user.id, symbol=symbol).first()
    if existing:
        existing.quantity += quantity
    else:
        new_hold = Holding(user_id=user.id, symbol=symbol, quantity=quantity, buy_price=price)
        db.session.add(new_hold)
    _record_trade_blotter_entry(user.id, symbol, 'buy', quantity, price, order_type='market', account_context='global')
    db.session.commit()
    return jsonify({'message': 'Buy successful', 'cash_balance': user.cash_balance})

@app.route('/sell', methods=['POST'])
def sell_stock():
    data = request.get_json()
    username = data.get('username')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))
    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': f'Error fetching price for symbol {symbol}: {str(e)}'}), 400
    proceeds = quantity * price
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    holding = Holding.query.filter_by(user_id=user.id, symbol=symbol).first()
    if not holding or holding.quantity < quantity:
        return jsonify({'message': 'Not enough shares to sell'}), 400
    

    # --- Record realized profit or loss ---
    profit = (price - holding.buy_price) * quantity
    user.realized_pnl = (user.realized_pnl or 0.0) + profit

    holding.quantity -= quantity
    if holding.quantity == 0:
        db.session.delete(holding)
    user.cash_balance += proceeds
    _record_trade_blotter_entry(user.id, symbol, 'sell', quantity, price, order_type='market', account_context='global')
    db.session.commit()
    return jsonify({'message': 'Sell successful', 'cash_balance': user.cash_balance})



@app.route('/reset_global', methods=['POST'])
def reset_global():
    data = request.get_json()
    username = data.get('username')
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    # Delete all holdings
    Holding.query.filter_by(user_id=user.id).delete()

    # Reset balance
    user.cash_balance = 100000
    user.realized_pnl = 0.0
    db.session.commit()

    return jsonify({'message': 'Global account reset to $100,000 successfully.'}), 200


# --------------------
# Competition Endpoints (Individual)
# --------------------
@app.route('/competition/create', methods=['POST'])
def create_competition():
    data = request.get_json()
    username = data.get('username')
    competition_name = data.get('competition_name')
    start_date_str = data.get('start_date')
    end_date_str = data.get('end_date')
    max_position_limit = data.get('max_position_limit')
    feature_competition = data.get('feature_competition', False)
    is_open = data.get('is_open', True)
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    code = secrets.token_hex(4)
    while Competition.query.filter_by(code=code).first():
        code = secrets.token_hex(4)
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d") if start_date_str else None
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else None
    comp = Competition(
        code=code, 
        name=competition_name, 
        created_by=user.id,
        start_date=start_date, 
        end_date=end_date, 
        max_position_limit=max_position_limit,
        featured=feature_competition,
        is_open=is_open
    )
    db.session.add(comp)
    db.session.commit()
    return jsonify({'message': 'Competition created successfully', 'competition_code': code})

@app.route('/competition/join', methods=['POST'])
def join_competition():
    data = request.get_json()
    username = data.get('username')
    competition_code = data.get('competition_code')
    access_code = (data.get('access_code') or '').strip()

    user = User.query.filter_by(username=username).first()
    comp = Competition.query.filter_by(code=competition_code).first()

    if not user or not comp:
        return jsonify({"message": "Invalid user or competition."}), 400

    # 🔒 Require code for restricted competitions
    if not comp.is_open:
        if not access_code or access_code != comp.code:
            return jsonify({"message": "Access denied: Invalid or missing competition code."}), 403

    # Prevent duplicate join
    existing = CompetitionMember.query.filter_by(user_id=user.id, competition_id=comp.id).first()
    if existing:
        return jsonify({"message": "Already joined this competition."}), 400

    # Create new competition member
    new_member = CompetitionMember(user_id=user.id, competition_id=comp.id, cash_balance=100000)
    db.session.add(new_member)
    db.session.commit()

    return jsonify({"message": f"Successfully joined {comp.name}!"}), 200


@app.route('/competition/buy', methods=['POST'])
def competition_buy():
    data = request.get_json()
    username = data.get('username')
    competition_code = data.get('competition_code')
    symbol = data.get('symbol').upper()
    quantity = int(data.get('quantity'))

    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    comp = Competition.query.filter_by(code=competition_code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404

    # ---------- Enforce start/end dates ----------
    now = datetime.utcnow()
    if comp.start_date and now < comp.start_date:
        return jsonify({'message': 'Competition has not started yet. No trades allowed.'}), 400
    if comp.end_date and now > comp.end_date:
        return jsonify({'message': 'Competition has ended. No trades allowed.'}), 400
    # --------------------------------------------

    member = CompetitionMember.query.filter_by(competition_id=comp.id, user_id=user.id).first()
    if not member:
        return jsonify({'message': 'User is not a member of this competition'}), 404

    # ---------- Get current price ----------
    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': f'Error fetching price for symbol {symbol}: {str(e)}'}), 400
    # --- Record realized profit or loss ---
    # ---------------------------------------

    cost = price * quantity
    if member.cash_balance < cost:
        return jsonify({'message': 'Insufficient funds in competition account'}), 400

    # ---------- NEW: Enforce position limit ----------
    limit_str = comp.max_position_limit or "100%"
    try:
        limit_pct = float(limit_str.strip('%')) / 100.0
    except Exception:
        limit_pct = 1.0  # default to 100% if malformed

    holdings = CompetitionHolding.query.filter_by(competition_member_id=member.id).all()
    total_value = sum((get_current_price(h.symbol) * h.quantity) for h in holdings) + member.cash_balance

    existing = CompetitionHolding.query.filter_by(
        competition_member_id=member.id, symbol=symbol
    ).first()
    existing_value = (existing.quantity * price) if existing else 0.0

    new_symbol_value = existing_value + cost
    new_symbol_pct = new_symbol_value / total_value if total_value > 0 else 1.0

    if new_symbol_pct > limit_pct:
        return jsonify({
            "message": (
                f"Buy rejected: would exceed {limit_str} position limit "
                f"({new_symbol_pct * 100:.2f}% of portfolio)"
            )
        }), 400
    # -----------------------------------------------

    # Proceed with purchase
    member.cash_balance -= cost
    if existing:
        existing.quantity += quantity
    else:
        new_holding = CompetitionHolding(
            competition_member_id=member.id,
            symbol=symbol,
            quantity=quantity,
            buy_price=price
        )
        db.session.add(new_holding)

    _record_trade_blotter_entry(
        user.id,
        symbol,
        'buy',
        quantity,
        price,
        order_type='market',
        account_context=f'competition:{competition_code}',
    )
    db.session.commit()
    return jsonify({'message': 'Competition buy successful', 'competition_cash': member.cash_balance})



@app.route('/competition/sell', methods=['POST'])
def competition_sell():
    
    data = request.get_json()
    username = data.get('username')
    competition_code = data.get('competition_code')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))

    # 1. Find user
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    # 2. Find competition
    comp = Competition.query.filter_by(code=competition_code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404

    # 3. Enforce start/end dates
    now = datetime.utcnow()
    if comp.start_date and now < comp.start_date:
        return jsonify({'message': 'Competition has not started yet. No trading allowed.'}), 400
    if comp.end_date and now > comp.end_date:
        return jsonify({'message': 'Competition has ended. No trading allowed.'}), 400

    # 4. Check membership
    member = CompetitionMember.query.filter_by(competition_id=comp.id, user_id=user.id).first()
    if not member:
        return jsonify({'message': 'User is not a member of this competition'}), 404

    # 5. Check holding
    holding = CompetitionHolding.query.filter_by(competition_member_id=member.id, symbol=symbol).first()
    if not holding or holding.quantity < quantity:
        return jsonify({'message': 'Not enough shares to sell in competition account'}), 400

    # 6. Fetch current price
    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': f'Error fetching price for symbol {symbol}: {str(e)}'}), 400

    # 7. Process sell
    proceeds = price * quantity
        # --- Record realized profit/loss for this competition account ---
    profit = (price - holding.buy_price) * quantity
    member.realized_pnl = (member.realized_pnl or 0.0) + profit

    
    holding.quantity -= quantity
    if holding.quantity == 0:
        db.session.delete(holding)
    member.cash_balance += proceeds
    _record_trade_blotter_entry(
        user.id,
        symbol,
        'sell',
        quantity,
        price,
        order_type='market',
        account_context=f'competition:{competition_code}',
    )
    db.session.commit()

    return jsonify({'message': 'Competition sell successful', 'competition_cash': member.cash_balance})


# --------------------
# Endpoints for Team (Global Team Account)
# --------------------
@app.route('/team/create', methods=['POST'])
def create_team():
    data = request.get_json()
    username = data.get('username')
    team_name = data.get('team_name')
    
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    
    team = Team(name=team_name, created_by=user.id)
    db.session.add(team)
    db.session.commit()
    
    team_member = TeamMember(team_id=team.id, user_id=user.id)
    db.session.add(team_member)
    db.session.commit()
    
    return jsonify({'message': 'Team created successfully', 'team_id': team.id, 'team_code': team.id})

@app.route('/team/join', methods=['POST'])
def join_team():
    data = request.get_json()
    username = data.get('username')
    team_code = data.get('team_code')
    
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    
    team = Team.query.filter_by(id=team_code).first()
    if not team:
        return jsonify({'message': 'Team not found'}), 404
    
    if TeamMember.query.filter_by(team_id=team.id, user_id=user.id).first():
        return jsonify({'message': 'User already in the team'}), 200
    
    team_member = TeamMember(team_id=team.id, user_id=user.id)
    db.session.add(team_member)
    db.session.commit()
    return jsonify({'message': 'Joined team successfully'})

@app.route('/team/buy', methods=['POST'])
def team_buy():
    data = request.get_json()
    username = data.get('username')
    team_id = data.get('team_id')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))
    
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    
    team = Team.query.get(team_id)
    if not team:
        return jsonify({'message': 'Team not found'}), 404
    
    if not TeamMember.query.filter_by(team_id=team_id, user_id=user.id).first():
        return jsonify({'message': 'User is not a member of this team'}), 403
    
    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': f'Error fetching price for symbol {symbol}: {str(e)}'}), 400
    
    cost = price * quantity
    if team.cash_balance < cost:
        return jsonify({'message': 'Insufficient team funds'}), 400
    
    team.cash_balance -= cost
    holding = TeamHolding.query.filter_by(team_id=team_id, symbol=symbol).first()
    if holding:
        holding.quantity += quantity
    else:
        new_holding = TeamHolding(team_id=team_id, symbol=symbol, quantity=quantity, buy_price=price)
        db.session.add(new_holding)
    
    db.session.commit()
    return jsonify({'message': 'Team buy successful', 'team_cash': team.cash_balance})

@app.route('/team/sell', methods=['POST'])
def team_sell():
    data = request.get_json()
    username = data.get('username')
    team_id = data.get('team_id')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))
    
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    
    team = Team.query.get(team_id)
    if not team:
        return jsonify({'message': 'Team not found'}), 404
    
    if not TeamMember.query.filter_by(team_id=team_id, user_id=user.id).first():
        return jsonify({'message': 'User is not a member of this team'}), 403
    
    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': f'Error fetching price for symbol {symbol}: {str(e)}'}), 400
    
    proceeds = price * quantity
    holding = TeamHolding.query.filter_by(team_id=team_id, symbol=symbol).first()
    if not holding or holding.quantity < quantity:
        return jsonify({'message': 'Not enough shares to sell'}), 400
    holding.quantity -= quantity
    if holding.quantity == 0:
        db.session.delete(holding)
    team.cash_balance += proceeds
    db.session.commit()
    return jsonify({'message': 'Team sell successful', 'team_cash': team.cash_balance})

# --------------------
# Endpoints for Competition Team (Teams participating in Competitions)
# --------------------
@app.route('/competition/team/join', methods=['POST'])
def competition_team_join():
    data = request.get_json()
    username = data.get('username')
    team_code = data.get('team_code')
    competition_code = data.get('competition_code')
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    team = Team.query.filter_by(id=team_code).first()
    if not team:
        return jsonify({'message': 'Team not found'}), 404
    if not TeamMember.query.filter_by(team_id=team.id, user_id=user.id).first():
        return jsonify({'message': 'User is not a member of this team'}), 403

    comp = Competition.query.filter_by(code=competition_code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404
    
    existing = CompetitionTeam.query.filter_by(competition_id=comp.id, team_id=team.id).first()
    if existing:
        return jsonify({'message': 'Team already joined this competition'}), 200
    comp_team = CompetitionTeam(competition_id=comp.id, team_id=team.id, cash_balance=100000)
    db.session.add(comp_team)
    db.session.commit()
    return jsonify({'message': 'Team successfully joined competition'})

@app.route('/competition/team/buy', methods=['POST'])
def competition_team_buy():
    data = request.get_json()
    username = data.get('username')
    competition_code = data.get('competition_code')
    team_id = data.get('team_id')
    symbol = data.get('symbol').upper()
    quantity = int(data.get('quantity'))

    # 1. Find the user
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    # 2. Find the competition
    comp = Competition.query.filter_by(code=competition_code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404

    # 3. Enforce start/end dates
    now = datetime.utcnow()
    if comp.start_date and now < comp.start_date:
        return jsonify({'message': 'Competition has not started yet. No trading allowed.'}), 400
    if comp.end_date and now > comp.end_date:
        return jsonify({'message': 'Competition has ended. No trading allowed.'}), 400

    # 4. Check if the team is part of this competition
    comp_team = CompetitionTeam.query.filter_by(competition_id=comp.id, team_id=team_id).first()
    if not comp_team:
        return jsonify({'message': 'Team is not part of this competition'}), 404

    # 5. Check user membership on the team
    if not TeamMember.query.filter_by(team_id=team_id, user_id=user.id).first():
        return jsonify({'message': 'User is not a member of this team'}), 403

    # 6. Fetch current price
    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': f'Error fetching price for symbol {symbol}: {str(e)}'}), 400

    # 7. Check funds
    cost = price * quantity
    if comp_team.cash_balance < cost:
        return jsonify({'message': 'Insufficient funds in competition team account'}), 400

    # ---------- NEW: Enforce position limit ----------
    limit_str = comp.max_position_limit or "100%"
    try:
        limit_pct = float(limit_str.strip('%')) / 100.0
    except Exception:
        limit_pct = 1.0  # default to 100%

    # Calculate total team portfolio value
    holdings = CompetitionTeamHolding.query.filter_by(competition_team_id=comp_team.id).all()
    total_value = sum((get_current_price(h.symbol) * h.quantity) for h in holdings) + comp_team.cash_balance

    # Determine current position size for this stock
    existing = CompetitionTeamHolding.query.filter_by(
        competition_team_id=comp_team.id,
        symbol=symbol
    ).first()
    existing_value = (existing.quantity * price) if existing else 0.0

    new_symbol_value = existing_value + cost
    new_symbol_pct = new_symbol_value / total_value if total_value > 0 else 1.0

    if new_symbol_pct > limit_pct:
        return jsonify({
            "message": (
                f"Buy rejected: would exceed {limit_str} position limit "
                f"({new_symbol_pct * 100:.2f}% of portfolio)"
            )
        }), 400
    # -----------------------------------------------

    # 8. Deduct funds, update holding
    comp_team.cash_balance -= cost
    holding = CompetitionTeamHolding.query.filter_by(
        competition_team_id=comp_team.id,
        symbol=symbol
    ).first()
    if holding:
        holding.quantity += quantity
    else:
        new_holding = CompetitionTeamHolding(
            competition_team_id=comp_team.id,
            symbol=symbol,
            quantity=quantity,
            buy_price=price
        )
        db.session.add(new_holding)

    _record_trade_blotter_entry(
        user.id,
        symbol,
        'buy',
        quantity,
        price,
        order_type='market',
        account_context=f'competition_team:{competition_code}:{team_id}',
    )
    db.session.commit()
    return jsonify({
        'message': 'Competition team buy successful',
        'competition_team_cash': comp_team.cash_balance
    })



@app.route('/competition/team/sell', methods=['POST'])
def competition_team_sell():
    
    data = request.get_json()
    username = data.get('username')
    competition_code = data.get('competition_code')
    team_id = data.get('team_id')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))

    # 1. Find the user
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    # 2. Find the competition
    comp = Competition.query.filter_by(code=competition_code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404

    # 3. Enforce start/end dates
    now = datetime.utcnow()
    if comp.start_date and now < comp.start_date:
        return jsonify({'message': 'Competition has not started yet. No trading allowed.'}), 400
    if comp.end_date and now > comp.end_date:
        return jsonify({'message': 'Competition has ended. No trading allowed.'}), 400

    # 4. Check if the team is in this competition
    comp_team = CompetitionTeam.query.filter_by(competition_id=comp.id, team_id=team_id).first()
    if not comp_team:
        return jsonify({'message': 'Team is not part of this competition'}), 404

    # 5. Check user membership on the team
    if not TeamMember.query.filter_by(team_id=team_id, user_id=user.id).first():
        return jsonify({'message': 'User is not a member of this team'}), 403

    # 6. Find the team's holding
    holding = CompetitionTeamHolding.query.filter_by(
        competition_team_id=comp_team.id,
        symbol=symbol
    ).first()
    if not holding or holding.quantity < quantity:
        return jsonify({'message': 'Not enough shares to sell in competition team account'}), 400

    # 7. Fetch current price
    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': f'Error fetching price for symbol {symbol}: {str(e)}'}), 400

    # --- Record realized profit or loss ---
    profit = (price - holding.buy_price) * quantity
    comp_team.realized_pnl = (comp_team.realized_pnl or 0.0) + profit

    # 8. Update quantity, add proceeds
    proceeds = price * quantity
    holding.quantity -= quantity
    if holding.quantity == 0:
        db.session.delete(holding)
    comp_team.cash_balance += proceeds

    _record_trade_blotter_entry(
        user.id,
        symbol,
        'sell',
        quantity,
        price,
        order_type='market',
        account_context=f'competition_team:{competition_code}:{team_id}',
    )
    db.session.commit()
    return jsonify({
        'message': 'Competition team sell successful',
        'competition_team_cash': comp_team.cash_balance
    })


# --------------------
# Admin Endpoints
# --------------------
@app.route('/admin/competitions', methods=['GET'])
def admin_get_competitions():
    admin_username = request.args.get('admin_username')
    admin_user = User.query.filter_by(username=admin_username).first()
    if not admin_user or not admin_user.is_admin:
        return jsonify({'message': 'Not authorized'}), 403

    competitions = Competition.query.all()
    data = [{
        'code': c.code,
        'name': c.name,
        'start_date': c.start_date.isoformat() if c.start_date else None,
        'end_date': c.end_date.isoformat() if c.end_date else None,
        'featured': c.featured,
        'is_open': c.is_open
    } for c in competitions]
    return jsonify(data)


@app.route('/admin/stats', methods=['GET'])
def admin_stats():
    total_users = User.query.count()
    total_competitions = Competition.query.count()
    return jsonify({'total_users': total_users, 'total_competitions': total_competitions})

@app.route('/admin/delete_competition', methods=['POST'])
def admin_delete_competition():
    data = request.get_json()
    username = data.get('username') or data.get('admin_username')
    code = data.get('competition_code')

    admin_user = User.query.filter_by(username=username).first()
    if not admin_user or not admin_user.is_admin:
        return jsonify({'message': 'Not authorized'}), 403

    comp = Competition.query.filter_by(code=code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404

    try:
        # --- Remove all related members & holdings ---
        CompetitionHolding.query.filter(
            CompetitionHolding.competition_member_id.in_(
                db.session.query(CompetitionMember.id).filter_by(competition_id=comp.id)
            )
        ).delete(synchronize_session=False)

        CompetitionMember.query.filter_by(competition_id=comp.id).delete(synchronize_session=False)

        # --- Remove all related team holdings & team members ---
        CompetitionTeamHolding.query.filter(
            CompetitionTeamHolding.competition_team_id.in_(
                db.session.query(CompetitionTeam.id).filter_by(competition_id=comp.id)
            )
        ).delete(synchronize_session=False)

        TeamMember.query.filter(
            TeamMember.team_id.in_(
                db.session.query(CompetitionTeam.team_id).filter_by(competition_id=comp.id)
            )
        ).delete(synchronize_session=False)

        CompetitionTeam.query.filter_by(competition_id=comp.id).delete(synchronize_session=False)

        # --- Delete the competition itself ---
        db.session.delete(comp)
        db.session.commit()

        return jsonify({'message': f'Competition {code} deleted successfully.'}), 200

    except Exception as e:
        db.session.rollback()
        app.logger.error(f"❌ Error deleting competition {code}: {e}")
        return jsonify({'message': f'Failed to delete competition: {str(e)}'}), 500



@app.route('/admin/delete_user', methods=['POST'])
def admin_delete_user():
    data = request.get_json()
    admin_username = data.get('username')
    target_username = data.get('target_username')

    admin_user = User.query.filter_by(username=admin_username).first()
    if not admin_user or not admin_user.is_admin:
        return jsonify({'message': 'Not authorized'}), 403

    target_user = User.query.filter_by(username=target_username).first()
    if not target_user:
        return jsonify({'message': 'User not found'}), 404

    try:
        # --- Delete all holdings ---
        Holding.query.filter_by(user_id=target_user.id).delete(synchronize_session=False)

        # --- Delete competitions created by this user ---
        comps = Competition.query.filter_by(created_by=target_user.id).all()
        for comp in comps:
            # delete related competition members and holdings
            CompetitionHolding.query.filter(
                CompetitionHolding.competition_member_id.in_(
                    db.session.query(CompetitionMember.id).filter_by(competition_id=comp.id)
                )
            ).delete(synchronize_session=False)
            CompetitionMember.query.filter_by(competition_id=comp.id).delete(synchronize_session=False)
            CompetitionTeam.query.filter_by(competition_id=comp.id).delete(synchronize_session=False)
            db.session.delete(comp)

        # --- Delete user’s competition memberships ---
        CompetitionHolding.query.filter(
            CompetitionHolding.competition_member_id.in_(
                db.session.query(CompetitionMember.id).filter_by(user_id=target_user.id)
            )
        ).delete(synchronize_session=False)
        CompetitionMember.query.filter_by(user_id=target_user.id).delete(synchronize_session=False)

        # --- Delete team memberships and teams created by user ---
        TeamMember.query.filter_by(user_id=target_user.id).delete(synchronize_session=False)
        Team.query.filter_by(created_by=target_user.id).delete(synchronize_session=False)

        # --- Finally delete the user ---
        db.session.delete(target_user)
        db.session.commit()

        return jsonify({'message': f'User {target_username} deleted successfully.'}), 200

    except Exception as e:
        db.session.rollback()
        app.logger.error(f"❌ Error deleting user {target_username}: {e}")
        return jsonify({'message': f'Failed to delete user: {str(e)}'}), 500




@app.route('/admin/update_competition_open', methods=['POST'])
def admin_update_competition_open():
    data = request.get_json()
    admin_username = data.get('admin_username')
    competition_code = data.get('competition_code')
    is_open = data.get('is_open')
    
    admin_user = User.query.filter_by(username=admin_username).first()
    if not admin_user or not admin_user.is_admin:
        return jsonify({'message': 'Not authorized'}), 403

    comp = Competition.query.filter_by(code=competition_code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404

    comp.is_open = is_open
    db.session.commit()
    return jsonify({'message': f'Competition {competition_code} open status updated to {is_open}.'})



# New endpoints for admin removal actions
@app.route('/admin/remove_user_from_competition', methods=['POST'])
def admin_remove_user_from_competition():
    data = request.get_json()
    admin_username = data.get('admin_username')
    target_username = data.get('target_username')
    competition_code = data.get('competition_code')
    
    # ✅ Validate admin
    admin_user = User.query.filter_by(username=admin_username).first()
    if not admin_user or not admin_user.is_admin:
        return jsonify({'message': 'Not authorized'}), 403

    # ✅ Find target user
    target_user = User.query.filter_by(username=target_username).first()
    if not target_user:
        return jsonify({'message': 'Target user not found'}), 404

    # ✅ Find competition
    comp = Competition.query.filter_by(code=competition_code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404

    # ✅ Find membership
    membership = CompetitionMember.query.filter_by(
        competition_id=comp.id,
        user_id=target_user.id
    ).first()
    if not membership:
        return jsonify({'message': 'User is not a member of this competition'}), 404

    # ✅ Delete related holdings FIRST (avoid FK constraint violation)
    CompetitionHolding.query.filter_by(competition_member_id=membership.id).delete()

    # ✅ Then delete the membership itself
    db.session.delete(membership)
    db.session.commit()

    return jsonify({'message': f'{target_username} has been removed from competition {competition_code}.'}), 200


@app.route('/admin/remove_user_from_team', methods=['POST'])
def remove_user_from_team():
    data = request.get_json()
    admin_username = data.get('admin_username')
    target_username = data.get('target_username')
    team_id = data.get('team_id')
    
    admin_user = User.query.filter_by(username=admin_username).first()
    if not admin_user or not admin_user.is_admin:
        return jsonify({'message': 'Not authorized'}), 403

    target_user = User.query.filter_by(username=target_username).first()
    if not target_user:
        return jsonify({'message': 'Target user not found'}), 404

    membership = TeamMember.query.filter_by(team_id=team_id, user_id=target_user.id).first()
    if not membership:
        return jsonify({'message': 'User is not a member of this team'}), 404

    db.session.delete(membership)
    db.session.commit()
    return jsonify({'message': f'{target_username} has been removed from team {team_id}.'})

# Endpoint for admin-only user info (listing all users)
@app.route('/users', methods=['GET'])
def get_all_users():
    admin_username = request.args.get('admin_username')
    admin_user = User.query.filter_by(username=admin_username).first()
    if not admin_user or not admin_user.is_admin:
         return jsonify({'message': 'Not authorized'}), 403
    users = User.query.all()
    users_data = [{
        'id': user.id,
        'username': user.username,
        'is_admin': user.is_admin,
        'cash_balance': user.cash_balance
    } for user in users]
    return jsonify(users_data)

# Endpoint for listing all competitions
@app.route('/competitions', methods=['GET'])
def get_all_competitions():
    competitions = Competition.query.all()
    competitions_data = [{
        'code': comp.code,
        'name': comp.name,
        'featured': comp.featured,
        'is_open': comp.is_open
    } for comp in competitions]
    return jsonify(competitions_data)

# --------------------
# Featured Competitions Endpoint (updated)
# --------------------
@app.route('/featured_competitions', methods=['GET'])
def get_featured_competitions():
    try:
        featured = Competition.query.filter_by(featured=True).all()
        result = []
        for comp in featured:
            if comp.end_date is None or comp.end_date >= datetime.utcnow():
                result.append({
                    'code': comp.code,
                    'name': comp.name,
                    'start_date': comp.start_date.isoformat(),
                    'end_date': comp.end_date.isoformat() if comp.end_date else None,
                    'is_open': comp.is_open
                })
        app.logger.info(f"Returning {len(result)} featured competitions")
        return jsonify(result), 200
    except Exception as e:
        app.logger.error(f"Error in /featured_competitions: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500
    
@app.route('/admin/update_featured_status', methods=['POST'])
def update_featured_status():
    data = request.get_json()
    admin_username = data.get("admin_username")
    competition_code = data.get("competition_code")
    feature_competition = data.get("feature_competition", False)

    admin_user = User.query.filter_by(username=admin_username).first()
    if not admin_user or not admin_user.is_admin:
        return jsonify({"message": "Not authorized"}), 403

    comp = Competition.query.filter_by(code=competition_code).first()
    if not comp:
        return jsonify({"message": "Competition not found"}), 404

    comp.featured = feature_competition
    db.session.commit()
    status = "featured" if feature_competition else "unfeatured"
    return jsonify({"message": f"Competition {competition_code} successfully {status}."})

def schedule_quick_pics_for_today():
    with app.app_context():
        now = datetime.utcnow()
        # Convert 'now' UTC to PST
        from_zone = tz.gettz('UTC')
        to_zone = tz.gettz('America/Los_Angeles')
        utc = now.replace(tzinfo=from_zone)
        pst_now = utc.astimezone(to_zone)

        # If it's Saturday (weekday=5) or Sunday (weekday=6), do nothing
        if pst_now.weekday() >= 5:
            app.logger.info("Weekend detected, skipping Quick Pics creation.")
            return

        # If you only want to schedule them if it's before 1PM, else do next day, you can do logic here
        # For now, let's assume we always schedule them for *today* if it's a weekday:
        base_date = pst_now.replace(hour=7, minute=0, second=0, microsecond=0)
        # For 6 hourly competitions: 7AM, 8AM, 9AM, 10AM, 11AM, 12PM (the last one ends at 1PM)
        for i in range(6):
            start_pst = base_date + timedelta(hours=i)
            end_pst = start_pst + timedelta(hours=1)
            # Convert back to UTC (no tz info) for storing in the DB
            start_utc = start_pst.astimezone(from_zone).replace(tzinfo=None)
            end_utc = end_pst.astimezone(from_zone).replace(tzinfo=None)

            code = secrets.token_hex(4)
            quick_comp = Competition(
                code=code,
                name="Quick Pics",
                created_by=1,  # system admin
                start_date=start_utc,
                end_date=end_utc,
                featured=True,
                max_position_limit="",
                is_open=True
            )
            db.session.add(quick_comp)
            db.session.commit()
            app.logger.info(f"Created Quick Pics competition {code} from {start_pst} - {end_pst}")
def reset_daily_pnl_at_open():
    """Run once per day at 6:35 AM PST – captures portfolio value at market open."""
    with app.app_context():
        pst = pytz.timezone('America/Los_Angeles')
        now_pst = datetime.now(pst)

        # Only run on weekdays at 6:35 AM
        if now_pst.weekday() >= 5 or now_pst.hour != 6 or now_pst.minute < 35:
            return

        # ----- GLOBAL ACCOUNTS -----
        for user in User.query.all():
            value = user.cash_balance
            for h in Holding.query.filter_by(user_id=user.id).all():
                try:
                    price = get_current_price(h.symbol)
                except Exception:
                    price = h.buy_price
                value += price * h.quantity
            user.start_of_day_value = value
            db.session.add(user)

        # ----- COMPETITION INDIVIDUAL -----
        for member in CompetitionMember.query.all():
            value = member.cash_balance
            for h in CompetitionHolding.query.filter_by(competition_member_id=member.id).all():
                try:
                    price = get_current_price(h.symbol)
                except Exception:
                    price = h.buy_price
                value += price * h.quantity
            member.start_of_day_value = value
            db.session.add(member)

        # ----- COMPETITION TEAM -----
        for ct in CompetitionTeam.query.all():
            value = ct.cash_balance
            for h in CompetitionTeamHolding.query.filter_by(competition_team_id=ct.id).all():
                try:
                    price = get_current_price(h.symbol)
                except Exception:
                    price = h.buy_price
                value += price * h.quantity
            ct.start_of_day_value = value
            db.session.add(ct)

        db.session.commit()
        app.logger.info("Daily P&L reset at market open (6:35 AM PST)")
        
@app.route('/quick_pics', methods=['GET'])
def quick_pics():
    now = datetime.utcnow()
    quick_comps = Competition.query.filter(
        Competition.name == "Quick Pics",
        Competition.start_date > now
    ).order_by(Competition.start_date).limit(2).all()
    result = []
    for comp in quick_comps:
        countdown = (comp.start_date - now).total_seconds() if comp.start_date > now else 0
        result.append({
            'code': comp.code,
            'name': comp.name,
            'start_date': comp.start_date.isoformat(),
            'end_date': comp.end_date.isoformat(),
            'countdown': countdown
        })
    return jsonify(result)

# --------------------
# Unified Competition Leaderboard (Individuals and Teams)
# --------------------
@app.route('/competition/<code>/leaderboard', methods=['GET'])
def competition_leaderboard(code):
    comp = Competition.query.filter_by(code=code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404

    leaderboard = []
    members = CompetitionMember.query.filter_by(competition_id=comp.id).all()

    for m in members:
        total_holdings = 0.0
        unrealized = 0.0

        choldings = CompetitionHolding.query.filter_by(competition_member_id=m.id).all()
        for h in choldings:
            try:
                price = get_current_price(h.symbol)
            except Exception:
                price = 0
            total_holdings += price * h.quantity
            unrealized += (price - h.buy_price) * h.quantity

        total = m.cash_balance + total_holdings
        total_pnl = (m.realized_pnl or 0.0) + unrealized
        return_pct = ((total - 100000.0) / 100000.0) * 100.0

        user = db.session.get(User, m.user_id)
        leaderboard.append({
            'name': user.username,
            'total_value': total,
            'pnl': total_pnl,
            'return_pct': return_pct
        })

    leaderboard_sorted = sorted(leaderboard, key=lambda x: x['total_value'], reverse=True)
    return jsonify(leaderboard_sorted)

@app.route('/competition/<code>/team_leaderboard', methods=['GET'])
def competition_team_leaderboard(code):
    comp = Competition.query.filter_by(code=code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404

    leaderboard = []
    comp_teams = CompetitionTeam.query.filter_by(competition_id=comp.id).all()

    for ct in comp_teams:
        total_holdings = 0.0
        unrealized = 0.0

        tholdings = CompetitionTeamHolding.query.filter_by(competition_team_id=ct.id).all()
        for h in tholdings:
            try:
                price = get_current_price(h.symbol)
            except Exception:
                price = 0
            total_holdings += price * h.quantity
            unrealized += (price - h.buy_price) * h.quantity

        total = ct.cash_balance + total_holdings
        total_pnl = (ct.realized_pnl or 0.0) + unrealized
        return_pct = ((total - 100000.0) / 100000.0) * 100.0

        team = db.session.get(Team, ct.team_id)
        leaderboard.append({
            'name': team.name,
            'total_value': total,
            'pnl': total_pnl,
            'return_pct': return_pct
        })

    leaderboard_sorted = sorted(leaderboard, key=lambda x: x['total_value'], reverse=True)
    return jsonify(leaderboard_sorted)


VALID_ORDER_STATUSES = {"open", "partially_filled", "filled", "cancelled", "expired", "rejected"}


def _serialize_trade_blotter_entry(entry):
    executed_at = entry.created_at.isoformat() + "Z" if entry.created_at else None
    account_context = entry.account_context or "global"
    return {
        "id": entry.id,
        "symbol": entry.symbol,
        "side": entry.side,
        "quantity": entry.quantity,
        "price": entry.price,
        "order_type": entry.order_type,
        "account_context": account_context,
        "account": account_context,
        "executed_at": executed_at,
    }


def _record_trade_blotter_entry(user_id, symbol, side, quantity, price, order_type="market", account_context="global"):
    entry = TradeBlotterEntry(
        user_id=user_id,
        symbol=symbol.upper(),
        side=side.lower(),
        quantity=int(quantity),
        price=float(price),
        order_type=order_type,
        account_context=account_context,
    )
    db.session.add(entry)


def _serialize_limit_order(order):
    return {
        "id": order.id,
        "user_id": order.user_id,
        "symbol": order.symbol,
        "side": order.side,
        "quantity": order.quantity,
        "limit_price": _round_metric(order.limit_price),
        "created_at": order.created_at.isoformat(),
        "updated_at": order.updated_at.isoformat(),
        "status": order.status,
        "account_context": order.account_context,
        "filled_qty": order.filled_qty,
        "avg_fill_price": _round_metric(order.avg_fill_price) if order.avg_fill_price is not None else None,
    }


def process_open_limit_orders():
    with app.app_context():
        open_orders = LimitOrder.query.filter(LimitOrder.status.in_(["open", "partially_filled"])).all()
        for order in open_orders:
            if order.status not in ["open", "partially_filled"]:
                continue
            try:
                current_price = get_current_price(order.symbol)
                should_fill = (order.side == "buy" and current_price <= order.limit_price) or (
                    order.side == "sell" and current_price >= order.limit_price
                )
                if not should_fill:
                    continue

                fill_qty = order.quantity - order.filled_qty
                if fill_qty <= 0:
                    continue

                user = db.session.get(User, order.user_id)
                if not user:
                    order.status = "rejected"
                    continue

                if order.side == "buy":
                    cost = current_price * fill_qty
                    if user.cash_balance < cost:
                        order.status = "rejected"
                        continue
                    user.cash_balance -= cost
                    holding = Holding.query.filter_by(user_id=user.id, symbol=order.symbol).first()
                    if holding:
                        holding.quantity += fill_qty
                    else:
                        db.session.add(Holding(user_id=user.id, symbol=order.symbol, quantity=fill_qty, buy_price=current_price))
                else:
                    holding = Holding.query.filter_by(user_id=user.id, symbol=order.symbol).first()
                    if not holding or holding.quantity < fill_qty:
                        order.status = "rejected"
                        continue
                    proceeds = current_price * fill_qty
                    user.cash_balance += proceeds
                    user.realized_pnl = (user.realized_pnl or 0.0) + ((current_price - holding.buy_price) * fill_qty)
                    holding.quantity -= fill_qty
                    if holding.quantity == 0:
                        db.session.delete(holding)

                _record_trade_blotter_entry(
                    user.id,
                    order.symbol,
                    order.side,
                    fill_qty,
                    current_price,
                    order_type='limit',
                    account_context=order.account_context,
                )
                order.filled_qty = order.quantity
                order.avg_fill_price = current_price
                order.status = "filled"
            except Exception as exc:
                app.logger.warning("limit_order_process_error id=%s error=%s", order.id, exc)
        db.session.commit()


@app.route('/orders/limit', methods=['GET'])
def list_limit_orders():
    username = request.args.get('username')
    if not username:
        return jsonify({'message': 'username is required'}), 400
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    status = request.args.get('status')
    query = LimitOrder.query.filter_by(user_id=user.id)
    if status:
        if status not in VALID_ORDER_STATUSES:
            return jsonify({'message': 'invalid status'}), 400
        query = query.filter_by(status=status)
    orders = query.order_by(LimitOrder.created_at.desc()).all()
    return jsonify([_serialize_limit_order(order) for order in orders])


@app.route('/trades/blotter', methods=['GET'])
@app.route('/trade-history', methods=['GET'])
def list_trade_blotter():
    username = request.args.get('username')
    user_id = request.args.get('user_id') or request.args.get('userId')

    user = None
    if username:
        user = User.query.filter_by(username=username).first()
    elif user_id:
        try:
            user = db.session.get(User, int(user_id))
        except (TypeError, ValueError):
            return jsonify({'message': 'user_id must be numeric'}), 400
    else:
        return jsonify({'message': 'username or user_id is required'}), 400

    if not user:
        return jsonify({'message': 'User not found'}), 404

    limit = request.args.get('limit', 100)
    try:
        limit = max(1, min(int(limit), 500))
    except (TypeError, ValueError):
        return jsonify({'message': 'limit must be numeric'}), 400

    entries = TradeBlotterEntry.query.filter_by(user_id=user.id).order_by(TradeBlotterEntry.created_at.desc()).limit(limit).all()
    return jsonify([_serialize_trade_blotter_entry(entry) for entry in entries])


@app.route('/orders/limit', methods=['POST'])
def create_limit_order():
    data = request.get_json() or {}
    username = data.get('username')
    symbol = (data.get('symbol') or '').upper()
    side = (data.get('side') or '').lower()
    account_context = data.get('account_context') or 'global'
    idempotency_key = data.get('idempotency_key')
    if not username or not symbol or side not in {'buy', 'sell'}:
        return jsonify({'message': 'username, symbol, and side are required'}), 400

    try:
        quantity = int(data.get('quantity'))
        limit_price = float(data.get('limit_price'))
    except (TypeError, ValueError):
        return jsonify({'message': 'quantity and limit_price must be numeric'}), 400

    if quantity <= 0 or limit_price <= 0:
        return jsonify({'message': 'quantity and limit_price must be positive'}), 400

    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    if idempotency_key:
        existing = LimitOrder.query.filter_by(user_id=user.id, account_context=f"{account_context}:{idempotency_key}").first()
        if existing:
            return jsonify(_serialize_limit_order(existing)), 200

    order = LimitOrder(
        user_id=user.id,
        symbol=symbol,
        side=side,
        quantity=quantity,
        limit_price=limit_price,
        status='open',
        account_context=f"{account_context}:{idempotency_key}" if idempotency_key else account_context,
        filled_qty=0,
    )
    db.session.add(order)
    db.session.commit()
    return jsonify(_serialize_limit_order(order)), 201


@app.route('/orders/limit/<int:order_id>/cancel', methods=['POST'])
def cancel_limit_order(order_id):
    data = request.get_json() or {}
    username = data.get('username')
    if not username:
        return jsonify({'message': 'username is required'}), 400
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    order = LimitOrder.query.filter_by(id=order_id, user_id=user.id).first()
    if not order:
        return jsonify({'message': 'Order not found'}), 404
    if order.status in {'filled', 'cancelled', 'expired', 'rejected'}:
        return jsonify(_serialize_limit_order(order)), 200
    order.status = 'cancelled'
    db.session.commit()
    return jsonify(_serialize_limit_order(order))


# ✅ Add this ABOVE the "if __name__ == '__main__'" block
@app.route('/admin/set_admin', methods=['POST'])
def set_admin():
    data = request.get_json()
    secret = data.get('secret')
    if secret != "Timb3000!":
        return jsonify({'message': 'Not authorized'}), 403

    username = data.get('username')
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    user.is_admin = True
    db.session.commit()
    return jsonify({'message': f"{username} is now an admin."})

# ---------- SCHEDULER ----------
scheduler = BackgroundScheduler()
scheduler.add_job(
    func=reset_daily_pnl_at_open,
    trigger="cron",
    hour=6,
    minute=35,
    timezone="America/Los_Angeles"
)
scheduler.add_job(func=process_open_limit_orders, trigger="interval", seconds=30)
scheduler.start()
# --------------------------------
# --------------------
# Run the app
# --------------------
if __name__ == '__main__':
    # Local development only
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
