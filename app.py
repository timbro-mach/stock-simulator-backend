from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from flask_sqlalchemy import SQLAlchemy
import requests, secrets
from datetime import datetime, timedelta
from dateutil import tz
import logging
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
import os

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
CORS(app, origins=[
    "https://stock-simulator-frontend.vercel.app",
    "https://simulator.gostockpro.com",
    "http://localhost:3000"
])

# --- Database Configuration ---
raw_db_url = os.getenv("DATABASE_URL", "").strip()

if not raw_db_url:
    raw_db_url = "sqlite:///local.db"
else:
    if "sslmode" not in raw_db_url:
        if raw_db_url.endswith("/"):
            raw_db_url = raw_db_url[:-1]
        raw_db_url += "?sslmode=require"

app.config["SQLALCHEMY_DATABASE_URI"] = raw_db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

print(f"Connected to database: {app.config['SQLALCHEMY_DATABASE_URI']}")

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
    is_admin = db.Column(db.Boolean, default=False)
    start_of_day_value = db.Column(db.Float, default=100000.0)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

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
    max_position_limit = db.Column(db.String(10), nullable=True)  # e.g. "20%", "50000"
    is_open = db.Column(db.Boolean, default=True)

class CompetitionMember(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    competition_id = db.Column(db.Integer, db.ForeignKey('competition.id'), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    cash_balance = db.Column(db.Float, default=100000)
    start_of_day_value = db.Column(db.Float, default=100000.0)
    __table_args__ = (db.UniqueConstraint('competition_id', 'user_id', name='_competition_user_uc'),)

class CompetitionHolding(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    competition_member_id = db.Column(db.Integer, db.ForeignKey('competition_member.id'), nullable=False)
    symbol = db.Column(db.String(10), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    buy_price = db.Column(db.Float, nullable=False)

class Team(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), nullable=False)
    created_by = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    cash_balance = db.Column(db.Float, default=100000)

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

class CompetitionTeam(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    competition_id = db.Column(db.Integer, db.ForeignKey('competition.id'), nullable=False)
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False)
    cash_balance = db.Column(db.Float, default=100000)
    start_of_day_value = db.Column(db.Float, default=100000.0)
    __table_args__ = (db.UniqueConstraint('competition_id', 'team_id', name='_competition_team_uc'),)

class CompetitionTeamHolding(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    competition_team_id = db.Column(db.Integer, db.ForeignKey('competition_team.id'), nullable=False)
    symbol = db.Column(db.String(10), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    buy_price = db.Column(db.Float, nullable=False)

with app.app_context():
    db.create_all()

# --------------------
# Helper: Get current price
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

# --------------------
# Helper: Check max position limit
# --------------------
def enforce_max_position(member_id, symbol, quantity, price, comp):
    if not comp.max_position_limit:
        return True
    try:
        limit = comp.max_position_limit.strip()
        if limit.endswith('%'):
            pct = float(limit[:-1]) / 100
            max_value = 100000 * pct
        else:
            max_value = float(limit)

        current_value = 0
        holdings = CompetitionHolding.query.filter_by(competition_member_id=member_id).all()
        for h in holdings:
            try:
                p = get_current_price(h.symbol) if h.symbol != symbol else price
            except:
                p = h.buy_price
            current_value += p * h.quantity

        new_value = current_value + (price * quantity)
        if new_value > max_value:
            return False
        return True
    except:
        return False  # Invalid limit → block

# --------------------
# Registration & Login
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
        # --- Global Account ---
        holdings = Holding.query.filter_by(user_id=user.id).all()
        global_portfolio = []
        total_holdings_value = 0
        global_pnl = 0

        for h in holdings:
            try:
                price = get_current_price(h.symbol)
            except:
                price = 0
            value = price * h.quantity
            pnl = (price - h.buy_price) * h.quantity
            global_pnl += pnl
            total_holdings_value += value
            global_portfolio.append({
                'symbol': h.symbol, 'quantity': h.quantity, 'current_price': price,
                'total_value': value, 'buy_price': h.buy_price
            })

        total_global_value = user.cash_balance + total_holdings_value
        global_return_pct = (global_pnl / 100000) * 100 if global_pnl else 0
        daily_pnl_global = total_global_value - (user.start_of_day_value or total_global_value)

        # --- Competition Accounts ---
        competition_accounts = []
        memberships = CompetitionMember.query.filter_by(user_id=user.id).all()
        for m in memberships:
            comp = db.session.get(Competition, m.competition_id)
            if not comp: continue
            comp_holdings = CompetitionHolding.query.filter_by(competition_member_id=m.id).all()
            comp_portfolio = []
            total_holdings_value = 0
            comp_pnl = 0

            for ch in comp_holdings:
                try:
                    price = get_current_price(ch.symbol)
                except:
                    price = 0
                value = price * ch.quantity
                pnl = (price - ch.buy_price) * ch.quantity
                comp_pnl += pnl
                total_holdings_value += value
                comp_portfolio.append({
                    'symbol': ch.symbol, 'quantity': ch.quantity, 'current_price': price,
                    'total_value': value, 'buy_price': ch.buy_price
                })

            total_value = m.cash_balance + total_holdings_value
            total_pnl = total_value - 100000
            return_pct = (total_pnl / 100000) * 100
            daily_pnl = total_value - (m.start_of_day_value or total_value)

            competition_accounts.append({
                'code': comp.code, 'name': comp.name, 'cash_balance': m.cash_balance,
                'portfolio': comp_portfolio, 'total_value': total_value,
                'pnl': total_pnl, 'return_pct': return_pct, 'daily_pnl': round(daily_pnl, 2)
            })

        # --- Team Competitions ---
        team_competitions = []
        team_memberships = TeamMember.query.filter_by(user_id=user.id).all()
        for tm in team_memberships:
            ct_entries = CompetitionTeam.query.filter_by(team_id=tm.team_id).all()
            for ct in ct_entries:
                comp = db.session.get(Competition, ct.competition_id)
                if not comp: continue
                ct_holdings = CompetitionTeamHolding.query.filter_by(competition_team_id=ct.id).all()
                team_portfolio = []
                total_holdings_value = 0
                team_pnl = 0

                for cht in ct_holdings:
                    try:
                        price = get_current_price(cht.symbol)
                    except:
                        price = 0
                    value = price * cht.quantity
                    pnl = (price - cht.buy_price) * cht.quantity
                    team_pnl += pnl
                    total_holdings_value += value
                    team_portfolio.append({
                        'symbol': cht.symbol, 'quantity': cht.quantity, 'current_price': price,
                        'total_value': value, 'buy_price': cht.buy_price
                    })

                total_value = ct.cash_balance + total_holdings_value
                total_pnl = total_value - 100000
                return_pct = (total_pnl / 100000) * 100
                daily_pnl = total_value - (ct.start_of_day_value or total_value)

                team_competitions.append({
                    'code': comp.code, 'name': comp.name, 'cash_balance': ct.cash_balance,
                    'portfolio': team_portfolio, 'total_value': total_value,
                    'pnl': total_pnl, 'return_pct': return_pct, 'team_id': ct.team_id,
                    'daily_pnl': round(daily_pnl, 2)
                })

        return jsonify({
            'message': 'Login successful',
            'username': user.username,
            'cash_balance': user.cash_balance,
            'is_admin': user.is_admin,
            'global_account': {
                'cash_balance': user.cash_balance,
                'portfolio': global_portfolio,
                'total_value': total_global_value,
                'pnl': global_pnl,
                'return_pct': global_return_pct,
                'daily_pnl': round(daily_pnl_global, 2)
            },
            'competition_accounts': competition_accounts,
            'team_competitions': team_competitions
        }), 200

    return jsonify({'message': 'Invalid credentials'}), 401

# --------------------
# User Data
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
    total_holdings_value = 0
    global_pnl = 0

    for h in holdings:
        try:
            price = get_current_price(h.symbol)
        except:
            price = 0
        value = price * h.quantity
        pnl = (price - h.buy_price) * h.quantity
        global_pnl += pnl
        total_holdings_value += value
        global_portfolio.append({
            'symbol': h.symbol, 'quantity': h.quantity, 'current_price': price,
            'total_value': value, 'buy_price': h.buy_price
        })

    total_global_value = user.cash_balance + total_holdings_value
    global_return_pct = (global_pnl / 100000) * 100 if global_pnl else 0
    daily_pnl_global = total_global_value - (user.start_of_day_value or total_global_value)

    # --- Competition Accounts ---
    competition_accounts = []
    memberships = CompetitionMember.query.filter_by(user_id=user.id).all()
    for m in memberships:
        comp = db.session.get(Competition, m.competition_id)
        if not comp: continue
        comp_holdings = CompetitionHolding.query.filter_by(competition_member_id=m.id).all()
        comp_portfolio = []
        total_comp_holdings = 0
        comp_pnl = 0

        for ch in comp_holdings:
            try:
                price = get_current_price(ch.symbol)
            except:
                price = 0
            value = price * ch.quantity
            pnl = (price - ch.buy_price) * ch.quantity
            comp_pnl += pnl
            total_comp_holdings += value
            comp_portfolio.append({
                'symbol': ch.symbol, 'quantity': ch.quantity, 'current_price': price,
                'total_value': value, 'buy_price': ch.buy_price
            })

        total_comp_value = m.cash_balance + total_comp_holdings
        total_pnl = total_comp_value - 100000
        return_pct = (total_pnl / 100000) * 100
        daily_pnl = total_comp_value - (m.start_of_day_value or total_comp_value)

        competition_accounts.append({
            'code': comp.code, 'name': comp.name, 'cash_balance': m.cash_balance,
            'portfolio': comp_portfolio, 'total_value': total_comp_value,
            'pnl': total_pnl, 'return_pct': return_pct, 'daily_pnl': round(daily_pnl, 2)
        })

    # --- Team Competitions ---
    team_competitions = []
    team_memberships = TeamMember.query.filter_by(user_id=user.id).all()
    for tm in team_memberships:
        ct_entries = CompetitionTeam.query.filter_by(team_id=tm.team_id).all()
        for ct in ct_entries:
            comp = db.session.get(Competition, ct.competition_id)
            if not comp: continue
            ct_holdings = CompetitionTeamHolding.query.filter_by(competition_team_id=ct.id).all()
            team_portfolio = []
            total_holdings_value = 0
            team_pnl = 0

            for cht in ct_holdings:
                try:
                    price = get_current_price(cht.symbol)
                except:
                    price = 0
                value = price * cht.quantity
                pnl = (price - cht.buy_price) * cht.quantity
                team_pnl += pnl
                total_holdings_value += value
                team_portfolio.append({
                    'symbol': cht.symbol, 'quantity': cht.quantity, 'current_price': price,
                    'total_value': value, 'buy_price': cht.buy_price
                })

            total_value = ct.cash_balance + total_holdings_value
            total_pnl = total_value - 100000
            return_pct = (total_pnl / 100000) * 100
            daily_pnl = total_value - (ct.start_of_day_value or total_value)

            team_competitions.append({
                'code': comp.code, 'name': comp.name, 'cash_balance': ct.cash_balance,
                'portfolio': team_portfolio, 'total_value': total_value,
                'team_id': ct.team_id, 'pnl': total_pnl, 'return_pct': return_pct,
                'daily_pnl': round(daily_pnl, 2)
            })

    response_data = {
        'username': user.username, 'is_admin': user.is_admin,
        'global_account': {
            'cash_balance': user.cash_balance, 'portfolio': global_portfolio,
            'total_value': total_global_value, 'pnl': global_pnl,
            'return_pct': global_return_pct, 'daily_pnl': round(daily_pnl_global, 2)
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
        price = get_current_price(symbol)
        return jsonify({'symbol': symbol, 'price': price})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/stock_chart/<symbol>', methods=['GET'])
def stock_chart(symbol):
    range_param = request.args.get("range", "1M").upper()
    try:
        if range_param == "1D":
            function = "TIME_SERIES_INTRADAY"
            params = {"function": function, "symbol": symbol, "interval": "5min", "apikey": ALPHA_VANTAGE_API_KEY}
            max_points = 78
        elif range_param in ["1W", "1M"]:
            function = "TIME_SERIES_DAILY_ADJUSTED"
            params = {"function": function, "symbol": symbol, "apikey": ALPHA_VANTAGE_API_KEY}
            max_points = 7 if range_param == "1W" else 30
        elif range_param in ["6M", "1Y"]:
            function = "TIME_SERIES_WEEKLY_ADJUSTED"
            params = {"function": function, "symbol": symbol, "apikey": ALPHA_VANTAGE_API_KEY}
            max_points = 26 if range_param == "6M" else 52
        else:
            function = "TIME_SERIES_DAILY_ADJUSTED"
            params = {"function": function, "symbol": symbol, "apikey": ALPHA_VANTAGE_API_KEY}
            max_points = 30

        response = requests.get("https://www.alphavantage.co/query", params=params, timeout=15)
        if response.status_code != 200:
            return jsonify({"error": "API error"}), 400
        data = response.json()

        ts_key = "Time Series (5min)" if function == "TIME_SERIES_INTRADAY" else \
                 "Time Series (Daily)" if function == "TIME_SERIES_DAILY_ADJUSTED" else \
                 "Weekly Adjusted Time Series"
        if ts_key not in data:
            return jsonify({"error": "No data"}), 404

        time_series = data[ts_key]
        chart_data = []
        for date_str, point in list(time_series.items())[:max_points]:
            close_key = "4. close" if "4. close" in point else "5. adjusted close"
            chart_data.append({"date": date_str, "close": float(point[close_key])})
        chart_data.sort(key=lambda x: x["date"])
        return jsonify(chart_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# --------------------
# Global Trading
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
        return jsonify({'message': f'Error: {str(e)}'}), 400
    cost = quantity * price
    user = User.query.filter_by(username=username).first()
    if not user or user.cash_balance < cost:
        return jsonify({'message': 'Insufficient funds'}), 400
    user.cash_balance -= cost
    existing = Holding.query.filter_by(user_id=user.id, symbol=symbol).first()
    if existing:
        existing.quantity += quantity
    else:
        db.session.add(Holding(user_id=user.id, symbol=symbol, quantity=quantity, buy_price=price))
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
        return jsonify({'message': f'Error: {str(e)}'}), 400
    proceeds = quantity * price
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    holding = Holding.query.filter_by(user_id=user.id, symbol=symbol).first()
    if not holding or holding.quantity < quantity:
        return jsonify({'message': 'Not enough shares'}), 400
    holding.quantity -= quantity
    if holding.quantity == 0:
        db.session.delete(holding)
    user.cash_balance += proceeds
    db.session.commit()
    return jsonify({'message': 'Sell successful', 'cash_balance': user.cash_balance})

@app.route('/reset_global', methods=['POST'])
def reset_global():
    data = request.get_json()
    username = data.get('username')
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    Holding.query.filter_by(user_id=user.id).delete()
    user.cash_balance = 100000
    db.session.commit()
    return jsonify({'message': 'Reset successful.'}), 200

# --------------------
# Competition Endpoints
# --------------------
@app.route('/competition/create', methods=['POST'])
def create_competition():
    data = request.get_json()
    username = data.get('username')
    name = data.get('competition_name')
    start = data.get('start_date')
    end = data.get('end_date')
    limit = data.get('max_position_limit', '').strip()
    featured = data.get('feature_competition', False)
    is_open = data.get('is_open', True)

    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    code = secrets.token_hex(4)
    while Competition.query.filter_by(code=code).first():
        code = secrets.token_hex(4)

    start_date = datetime.strptime(start, "%Y-%m-%d") if start else None
    end_date = datetime.strptime(end, "%Y-%m-%d") if end else None

    comp = Competition(
        code=code, name=name, created_by=user.id, start_date=start_date,
        end_date=end_date, max_position_limit=limit, featured=featured, is_open=is_open
    )
    db.session.add(comp)
    db.session.commit()
    return jsonify({'message': 'Created', 'competition_code': code})

@app.route('/competition/join', methods=['POST'])
def join_competition():
    data = request.get_json()
    username = data.get('username')
    code = data.get('competition_code')
    access = (data.get('access_code') or '').strip()

    user = User.query.filter_by(username=username).first()
    comp = Competition.query.filter_by(code=code).first()
    if not user or not comp:
        return jsonify({"message": "Invalid"}), 400

    if not comp.is_open and access != comp.code:
        return jsonify({"message": "Access denied"}), 403

    if CompetitionMember.query.filter_by(user_id=user.id, competition_id=comp.id).first():
        return jsonify({"message": "Already joined"}), 400

    db.session.add(CompetitionMember(user_id=user.id, competition_id=comp.id, cash_balance=100000))
    db.session.commit()
    return jsonify({"message": f"Joined {comp.name}"}), 200

@app.route('/competition/buy', methods=['POST'])
def competition_buy():
    data = request.get_json()
    username = data.get('username')
    code = data.get('competition_code')
    symbol = data.get('symbol')
    qty = int(data.get('quantity'))

    user = User.query.filter_by(username=username).first()
    comp = Competition.query.filter_by(code=code).first()
    if not user or not comp:
        return jsonify({'message': 'Not found'}), 404

    now = datetime.utcnow()
    if comp.start_date and now < comp.start_date:
        return jsonify({'message': 'Not started'}), 400
    if comp.end_date and now > comp.end_date:
        return jsonify({'message': 'Ended'}), 400

    member = CompetitionMember.query.filter_by(competition_id=comp.id, user_id=user.id).first()
    if not member:
        return jsonify({'message': 'Not member'}), 404

    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': str(e)}), 400

    cost = price * qty
    if member.cash_balance < cost:
        return jsonify({'message': 'Insufficient funds'}), 400

    if not enforce_max_position(member.id, symbol, qty, price, comp):
        return jsonify({'message': 'Max position limit exceeded'}), 400

    member.cash_balance -= cost
    existing = CompetitionHolding.query.filter_by(competition_member_id=member.id, symbol=symbol).first()
    if existing:
        existing.quantity += qty
    else:
        db.session.add(CompetitionHolding(competition_member_id=member.id, symbol=symbol, quantity=qty, buy_price=price))
    db.session.commit()
    return jsonify({'message': 'Buy successful', 'competition_cash': member.cash_balance})

@app.route('/competition/sell', methods=['POST'])
def competition_sell():
    data = request.get_json()
    username = data.get('username')
    code = data.get('competition_code')
    symbol = data.get('symbol')
    qty = int(data.get('quantity'))

    user = User.query.filter_by(username=username).first()
    comp = Competition.query.filter_by(code=code).first()
    if not user or not comp:
        return jsonify({'message': 'Not found'}), 404

    now = datetime.utcnow()
    if comp.start_date and now < comp.start_date:
        return jsonify({'message': 'Not started'}), 400
    if comp.end_date and now > comp.end_date:
        return jsonify({'message': 'Ended'}), 400

    member = CompetitionMember.query.filter_by(competition_id=comp.id, user_id=user.id).first()
    if not member:
        return jsonify({'message': 'Not member'}), 404

    holding = CompetitionHolding.query.filter_by(competition_member_id=member.id, symbol=symbol).first()
    if not holding or holding.quantity < qty:
        return jsonify({'message': 'Not enough shares'}), 400

    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': str(e)}), 400

    proceeds = price * qty
    holding.quantity -= qty
    if holding.quantity == 0:
        db.session.delete(holding)
    member.cash_balance += proceeds
    db.session.commit()
    return jsonify({'message': 'Sell successful', 'competition_cash': member.cash_balance})

# --------------------
# Team Endpoints
# --------------------
@app.route('/team/create', methods=['POST'])
def create_team():
    data = request.get_json()
    username = data.get('username')
    name = data.get('team_name')
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    team = Team(name=name, created_by=user.id)
    db.session.add(team)
    db.session.flush()
    db.session.add(TeamMember(team_id=team.id, user_id=user.id))
    db.session.commit()
    return jsonify({'message': 'Team created', 'team_id': team.id})

@app.route('/team/join', methods=['POST'])
def join_team():
    data = request.get_json()
    username = data.get('username')
    team_id = data.get('team_code')
    user = User.query.filter_by(username=username).first()
    team = Team.query.get(team_id)
    if not user or not team:
        return jsonify({'message': 'Not found'}), 404
    if TeamMember.query.filter_by(team_id=team_id, user_id=user.id).first():
        return jsonify({'message': 'Already in team'}), 200
    db.session.add(TeamMember(team_id=team_id, user_id=user.id))
    db.session.commit()
    return jsonify({'message': 'Joined team'})

@app.route('/team/buy', methods=['POST'])
def team_buy():
    data = request.get_json()
    username = data.get('username')
    team_id = data.get('team_id')
    symbol = data.get('symbol')
    qty = int(data.get('quantity'))
    user = User.query.filter_by(username=username).first()
    team = Team.query.get(team_id)
    if not user or not team or not TeamMember.query.filter_by(team_id=team_id, user_id=user.id).first():
        return jsonify({'message': 'Invalid'}), 403
    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': str(e)}), 400
    cost = price * qty
    if team.cash_balance < cost:
        return jsonify({'message': 'Insufficient funds'}), 400
    team.cash_balance -= cost
    h = TeamHolding.query.filter_by(team_id=team_id, symbol=symbol).first()
    if h:
        h.quantity += qty
    else:
        db.session.add(TeamHolding(team_id=team_id, symbol=symbol, quantity=qty, buy_price=price))
    db.session.commit()
    return jsonify({'message': 'Buy successful', 'team_cash': team.cash_balance})

@app.route('/team/sell', methods=['POST'])
def team_sell():
    data = request.get_json()
    username = data.get('username')
    team_id = data.get('team_id')
    symbol = data.get('symbol')
    qty = int(data.get('quantity'))
    user = User.query.filter_by(username=username).first()
    team = Team.query.get(team_id)
    if not user or not team or not TeamMember.query.filter_by(team_id=team_id, user_id=user.id).first():
        return jsonify({'message': 'Invalid'}), 403
    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': str(e)}), 400
    h = TeamHolding.query.filter_by(team_id=team_id, symbol=symbol).first()
    if not h or h.quantity < qty:
        return jsonify({'message': 'Not enough shares'}), 400
    proceeds = price * qty
    h.quantity -= qty
    if h.quantity == 0:
        db.session.delete(h)
    team.cash_balance += proceeds
    db.session.commit()
    return jsonify({'message': 'Sell successful', 'team_cash': team.cash_balance})

# --------------------
# Competition Team
# --------------------
@app.route('/competition/team/join', methods=['POST'])
def competition_team_join():
    data = request.get_json()
    username = data.get('username')
    team_code = data.get('team_code')
    comp_code = data.get('competition_code')
    user = User.query.filter_by(username=username).first()
    team = Team.query.get(team_code)
    comp = Competition.query.filter_by(code=comp_code).first()
    if not user or not team or not comp or not TeamMember.query.filter_by(team_id=team.id, user_id=user.id).first():
        return jsonify({'message': 'Invalid'}), 400
    if CompetitionTeam.query.filter_by(competition_id=comp.id, team_id=team.id).first():
        return jsonify({'message': 'Already joined'}), 200
    db.session.add(CompetitionTeam(competition_id=comp.id, team_id=team.id, cash_balance=100000))
    db.session.commit()
    return jsonify({'message': 'Team joined competition'})

@app.route('/competition/team/buy', methods=['POST'])
def competition_team_buy():
    data = request.get_json()
    username = data.get('username')
    code = data.get('competition_code')
    team_id = data.get('team_id')
    symbol = data.get('symbol')
    qty = int(data.get('quantity'))
    user = User.query.filter_by(username=username).first()
    comp = Competition.query.filter_by(code=code).first()
    ct = CompetitionTeam.query.filter_by(competition_id=comp.id, team_id=team_id).first()
    if not user or not comp or not ct or not TeamMember.query.filter_by(team_id=team_id, user_id=user.id).first():
        return jsonify({'message': 'Invalid'}), 400
    now = datetime.utcnow()
    if comp.start_date and now < comp.start_date or comp.end_date and now > comp.end_date:
        return jsonify({'message': 'Trading not allowed'}), 400
    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': str(e)}), 400
    cost = price * qty
    if ct.cash_balance < cost:
        return jsonify({'message': 'Insufficient funds'}), 400
    ct.cash_balance -= cost
    h = CompetitionTeamHolding.query.filter_by(competition_team_id=ct.id, symbol=symbol).first()
    if h:
        h.quantity += qty
    else:
        db.session.add(CompetitionTeamHolding(competition_team_id=ct.id, symbol=symbol, quantity=qty, buy_price=price))
    db.session.commit()
    return jsonify({'message': 'Buy successful', 'competition_team_cash': ct.cash_balance})

@app.route('/competition/team/sell', methods=['POST'])
def competition_team_sell():
    data = request.get_json()
    username = data.get('username')
    code = data.get('competition_code')
    team_id = data.get('team_id')
    symbol = data.get('symbol')
    qty = int(data.get('quantity'))
    user = User.query.filter_by(username=username).first()
    comp = Competition.query.filter_by(code=code).first()
    ct = CompetitionTeam.query.filter_by(competition_id=comp.id, team_id=team_id).first()
    if not user or not comp or not ct or not TeamMember.query.filter_by(team_id=team_id, user_id=user.id).first():
        return jsonify({'message': 'Invalid'}), 400
    now = datetime.utcnow()
    if comp.start_date and now < comp.start_date or comp.end_date and now > comp.end_date:
        return jsonify({'message': 'Trading not allowed'}), 400
    h = CompetitionTeamHolding.query.filter_by(competition_team_id=ct.id, symbol=symbol).first()
    if not h or h.quantity < qty:
        return jsonify({'message': 'Not enough shares'}), 400
    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': str(e)}), 400
    proceeds = price * qty
    h.quantity -= qty
    if h.quantity == 0:
        db.session.delete(h)
    ct.cash_balance += proceeds
    db.session.commit()
    return jsonify({'message': 'Sell successful', 'competition_team_cash': ct.cash_balance})

# --------------------
# Admin Endpoints
# --------------------
@app.route('/admin/competitions', methods=['GET'])
def admin_get_competitions():
    admin = User.query.filter_by(username=request.args.get('admin_username')).first()
    if not admin or not admin.is_admin:
        return jsonify({'message': 'Unauthorized'}), 403
    comps = Competition.query.all()
    return jsonify([{
        'code': c.code, 'name': c.name, 'start_date': c.start_date.isoformat() if c.start_date else None,
        'end_date': c.end_date.isoformat() if c.end_date else None, 'featured': c.featured, 'is_open': c.is_open
    } for c in comps])

@app.route('/admin/stats', methods=['GET'])
def admin_stats():
    return jsonify({'total_users': User.query.count(), 'total_competitions': Competition.query.count()})

@app.route('/admin/delete_competition', methods=['POST'])
def admin_delete_competition():
    data = request.get_json()
    admin = User.query.filter_by(username=data.get('admin_username')).first()
    if not admin or not admin.is_admin:
        return jsonify({'message': 'Unauthorized'}), 403
    comp = Competition.query.filter_by(code=data.get('competition_code')).first()
    if not comp:
        return jsonify({'message': 'Not found'}), 404
    try:
        CompetitionHolding.query.filter(
            CompetitionHolding.competition_member_id.in_(
                db.session.query(CompetitionMember.id).filter_by(competition_id=comp.id)
            )
        ).delete(synchronize_session=False)
        CompetitionMember.query.filter_by(competition_id=comp.id).delete()
        CompetitionTeamHolding.query.filter(
            CompetitionTeamHolding.competition_team_id.in_(
                db.session.query(CompetitionTeam.id).filter_by(competition_id=comp.id)
            )
        ).delete(synchronize_session=False)
        CompetitionTeam.query.filter_by(competition_id=comp.id).delete()
        db.session.delete(comp)
        db.session.commit()
        return jsonify({'message': 'Deleted'}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'message': str(e)}), 500

@app.route('/admin/delete_user', methods=['POST'])
def admin_delete_user():
    data = request.get_json()
    admin = User.query.filter_by(username=data.get('admin_username')).first()
    target = User.query.filter_by(username=data.get('target_username')).first()
    if not admin or not admin.is_admin or not target:
        return jsonify({'message': 'Invalid'}), 400
    try:
        CompetitionHolding.query.filter(
            CompetitionHolding.competition_member_id.in_(
                db.session.query(CompetitionMember.id).filter_by(user_id=target.id)
            )
        ).delete(synchronize_session=False)
        CompetitionMember.query.filter_by(user_id=target.id).delete()
        TeamMember.query.filter_by(user_id=target.id).delete()
        db.session.delete(target)
        db.session.commit()
        return jsonify({'message': 'User deleted'}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({'message': str(e)}), 500

@app.route('/admin/update_competition_open', methods=['POST'])
def admin_update_competition_open():
    data = request.get_json()
    admin = User.query.filter_by(username=data.get('admin_username')).first()
    comp = Competition.query.filter_by(code=data.get('competition_code')).first()
    if not admin or not admin.is_admin or not comp:
        return jsonify({'message': 'Invalid'}), 400
    comp.is_open = data.get('is_open')
    db.session.commit()
    return jsonify({'message': 'Updated'})

@app.route('/admin/remove_user_from_competition', methods=['POST'])
def admin_remove_user_from_competition():
    data = request.get_json()
    admin = User.query.filter_by(username=data.get('admin_username')).first()
    target = User.query.filter_by(username=data.get('target_username')).first()
    comp = Competition.query.filter_by(code=data.get('competition_code')).first()
    if not admin or not admin.is_admin or not target or not comp:
        return jsonify({'message': 'Invalid'}), 400
    member = CompetitionMember.query.filter_by(competition_id=comp.id, user_id=target.id).first()
    if not member:
        return jsonify({'message': 'Not in competition'}), 404
    CompetitionHolding.query.filter_by(competition_member_id=member.id).delete()
    db.session.delete(member)
    db.session.commit()
    return jsonify({'message': 'Removed from competition'}), 200

@app.route('/admin/remove_user_from_team', methods=['POST'])
def admin_remove_user_from_team():
    data = request.get_json()
    admin = User.query.filter_by(username=data.get('admin_username')).first()
    target = User.query.filter_by(username=data.get('target_username')).first()
    if not admin or not admin.is_admin or not target:
        return jsonify({'message': 'Invalid'}), 400
    member = TeamMember.query.filter_by(team_id=data.get('team_id'), user_id=target.id).first()
    if not member:
        return jsonify({'message': 'Not in team'}), 404
    db.session.delete(member)
    db.session.commit()
    return jsonify({'message': 'Removed from team'}), 200

@app.route('/users', methods=['GET'])
def get_all_users():
    admin = User.query.filter_by(username=request.args.get('admin_username')).first()
    if not admin or not admin.is_admin:
        return jsonify({'message': 'Unauthorized'}), 403
    return jsonify([{'id': u.id, 'username': u.username, 'is_admin': u.is_admin, 'cash_balance': u.cash_balance} for u in User.query.all()])

@app.route('/competitions', methods=['GET'])
def get_all_competitions():
    return jsonify([{'code': c.code, 'name': c.name, 'featured': c.featured, 'is_open': c.is_open} for c in Competition.query.all()])

@app.route('/featured_competitions', methods=['GET'])
def get_featured_competitions():
    now = datetime.utcnow()
    comps = Competition.query.filter_by(featured=True).all()
    result = []
    for c in comps:
        if not c.end_date or c.end_date >= now:
            result.append({
                'code': c.code, 'name': c.name,
                'start_date': c.start_date.isoformat() if c.start_date else None,
                'end_date': c.end_date.isoformat() if c.end_date else None,
                'is_open': c.is_open
            })
    return jsonify(result), 200

@app.route('/admin/update_featured_status', methods=['POST'])
def update_featured_status():
    data = request.get_json()
    admin = User.query.filter_by(username=data.get('admin_username')).first()
    comp = Competition.query.filter_by(code=data.get('competition_code')).first()
    if not admin or not admin.is_admin or not comp:
        return jsonify({'message': 'Invalid'}), 400
    comp.featured = data.get('feature_competition', False)
    db.session.commit()
    return jsonify({'message': 'Updated'})

# --------------------
# Daily P&L Reset
# --------------------
def reset_daily_pnl_at_open():
    with app.app_context():
        pst = pytz.timezone('America/Los_Angeles')
        now = datetime.now(pst)
        if now.weekday() >= 5 or now.hour != 6 or now.minute < 35:
            return

        for user in User.query.all():
            value = user.cash_balance
            for h in Holding.query.filter_by(user_id=user.id).all():
                try:
                    price = get_current_price(h.symbol)
                except:
                    price = h.buy_price
                value += price * h.quantity
            user.start_of_day_value = value
            db.session.add(user)

        for m in CompetitionMember.query.all():
            value = m.cash_balance
            for h in CompetitionHolding.query.filter_by(competition_member_id=m.id).all():
                try:
                    price = get_current_price(h.symbol)
                except:
                    price = h.buy_price
                value += price * h.quantity
            m.start_of_day_value = value
            db.session.add(m)

        for ct in CompetitionTeam.query.all():
            value = ct.cash_balance
            for h in CompetitionTeamHolding.query.filter_by(competition_team_id=ct.id).all():
                try:
                    price = get_current_price(h.symbol)
                except:
                    price = h.buy_price
                value += price * h.quantity
            ct.start_of_day_value = value
            db.session.add(ct)

        db.session.commit()
        app.logger.info("Daily P&L reset at 6:35 AM PST")

scheduler = BackgroundScheduler()
scheduler.add_job(func=reset_daily_pnl_at_open, trigger="cron", hour=6, minute=35, timezone="America/Los_Angeles")
scheduler.start()

@app.route('/quick_pics', methods=['GET'])
def quick_pics():
    now = datetime.utcnow()
    comps = Competition.query.filter(Competition.name == "Quick Pics", Competition.start_date > now).order_by(Competition.start_date).limit(2).all()
    return jsonify([{
        'code': c.code, 'name': c.name, 'start_date': c.start_date.isoformat(),
        'end_date': c.end_date.isoformat(), 'countdown': (c.start_date - now).total_seconds()
    } for c in comps])

@app.route('/competition/<code>/leaderboard', methods=['GET'])
def competition_leaderboard(code):
    comp = Competition.query.filter_by(code=code).first()
    if not comp:
        return jsonify({'message': 'Not found'}), 404
    lb = []
    for m in CompetitionMember.query.filter_by(competition_id=comp.id).all():
        total = m.cash_balance
        for h in CompetitionHolding.query.filter_by(competition_member_id=m.id).all():
            try:
                price = get_current_price(h.symbol)
            except:
                price = 0
            total += price * h.quantity
        user = db.session.get(User, m.user_id)
        lb.append({'name': user.username, 'total_value': total})
    return jsonify(sorted(lb, key=lambda x: x['total_value'], reverse=True))

@app.route('/competition/<code>/team_leaderboard', methods=['GET'])
def competition_team_leaderboard(code):
    comp = Competition.query.filter_by(code=code).first()
    if not comp:
        return jsonify({'message': 'Not found'}), 404
    lb = []
    for ct in CompetitionTeam.query.filter_by(competition_id=comp.id).all():
        total = ct.cash_balance
        for h in CompetitionTeamHolding.query.filter_by(competition_team_id=ct.id).all():
            try:
                price = get_current_price(h.symbol)
            except:
                price = 0
            total += price * h.quantity
        team = db.session.get(Team, ct.team_id)
        lb.append({'name': team.name, 'total_value': total})
    return jsonify(sorted(lb, key=lambda x: x['total_value'], reverse=True))

@app.route('/admin/set_admin', methods=['POST'])
def set_admin():
    data = request.get_json()
    if data.get('secret') != "Timb3000!":
        return jsonify({'message': 'Unauthorized'}), 403
    user = User.query.filter_by(username=data.get('username')).first()
    if not user:
        return jsonify({'message': 'Not found'}), 404
    user.is_admin = True
    db.session.commit()
    return jsonify({'message': f"{user.username} is admin."})

# --------------------
# Run
# --------------------
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)