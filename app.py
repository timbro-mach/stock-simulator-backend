from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from flask_sqlalchemy import SQLAlchemy
import requests, secrets
from datetime import datetime, timedelta
import os
from dateutil import tz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger  # Added missing import
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, origins=[
    "https://stock-simulator-frontend.vercel.app",
    "https://simulator.gostockpro.com",
    "http://localhost:3000"  # Added for local development
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

logger.info(f"✅ Connected to database: {app.config['SQLALCHEMY_DATABASE_URI']}")

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
    max_position_limit = db.Column(db.String(10), nullable=True)
    is_open = db.Column(db.Boolean, default=True)

class CompetitionMember(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    competition_id = db.Column(db.Integer, db.ForeignKey('competition.id'), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    cash_balance = db.Column(db.Float, default=100000)
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
# Helper Function: Fetch current price from Alpha Vantage
# --------------------
def get_current_price(symbol):
    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&entitlement=realtime&apikey={ALPHA_VANTAGE_API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if "Global Quote" not in data or not data["Global Quote"]:
            raise Exception(f"No data found for symbol {symbol}")
        global_quote = data["Global Quote"]
        if "05. price" not in global_quote:
            raise Exception(f"No price information available for symbol {symbol}")
        return float(global_quote["05. price"])
    except Exception as e:
        logger.error(f"Error fetching price for {symbol}: {str(e)}")
        raise

# --------------------
# Endpoints for Registration and Login
# --------------------
@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    email = data.get('email')
    if not username or not password:
        return jsonify({'message': 'Username and password are required'}), 400
    if User.query.filter_by(username=username).first():
        return jsonify({'message': 'User already exists'}), 400
    if email and User.query.filter_by(email=email).first():
        return jsonify({'message': 'Email already in use'}), 400
    try:
        new_user = User(username=username, email=email)
        new_user.set_password(password)
        db.session.add(new_user)
        db.session.commit()
        logger.info(f"User {username} registered successfully")
        return jsonify({'message': 'User created successfully'})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error registering user {username}: {str(e)}")
        return jsonify({'message': 'Failed to register user'}), 500

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    if not username or not password:
        return jsonify({'message': 'Username and password are required'}), 400
    user = User.query.filter_by(username=username).first()
    if user and user.check_password(password):
        memberships = CompetitionMember.query.filter_by(user_id=user.id).all()
        competition_accounts = []
        for m in memberships:
            comp = db.session.get(Competition, m.competition_id)
            if comp:
                competition_accounts.append({
                    'code': comp.code,
                    'name': comp.name,
                    'competition_cash': m.cash_balance,
                    'total_value': m.cash_balance,
                    'portfolio': [],
                    'start_date': comp.start_date.isoformat() if comp.start_date else None,
                    'end_date': comp.end_date.isoformat() if comp.end_date else None
                })

        team_memberships = TeamMember.query.filter_by(user_id=user.id).all()
        teams = []
        for tm in team_memberships:
            team = db.session.get(Team, tm.team_id)
            if team:
                teams.append({
                    'team_id': team.id,
                    'team_name': team.name,
                    'team_cash': team.cash_balance
                })
        logger.info(f"User {username} logged in successfully")
        return jsonify({
            'message': 'Login successful',
            'username': user.username,
            'cash_balance': user.cash_balance,
            'is_admin': user.is_admin,
            'global_account': {'cash_balance': user.cash_balance},
            'competition_accounts': competition_accounts,
            'teams': teams
        })
    else:
        logger.warning(f"Failed login attempt for {username}")
        return jsonify({'message': 'Invalid credentials'}), 401

# --------------------
# Endpoint for Global User Data
# --------------------
@app.route('/user', methods=['GET'])
def get_user():
    username = request.args.get('username')
    if not username:
        return jsonify({'message': 'Username is required'}), 400
    user = User.query.filter_by(username=username).first()
    if not user:
        logger.error(f"User {username} not found")
        return jsonify({'message': 'User not found'}), 404

    try:
        holdings = Holding.query.filter_by(user_id=user.id).all()
        global_portfolio = []
        total_global = user.cash_balance
        global_pnl = 0
        for h in holdings:
            try:
                price = get_current_price(h.symbol)
            except Exception:
                price = 0
            value = price * h.quantity
            pnl = (price - h.buy_price) * h.quantity
            global_pnl += pnl
            total_global += value
            global_portfolio.append({
                'symbol': h.symbol,
                'quantity': h.quantity,
                'current_price': price,
                'total_value': value,
                'buy_price': h.buy_price
            })

        competition_accounts = []
        memberships = CompetitionMember.query.filter_by(user_id=user.id).all()
        for m in memberships:
            comp = db.session.get(Competition, m.competition_id)
            if comp:
                comp_holdings = CompetitionHolding.query.filter_by(competition_member_id=m.id).all()
                comp_portfolio = []
                total_comp_holdings = 0
                comp_pnl = 0
                for ch in comp_holdings:
                    try:
                        price = get_current_price(ch.symbol)
                    except Exception:
                        price = 0
                    value = price * ch.quantity
                    pnl = (price - ch.buy_price) * ch.quantity
                    comp_pnl += pnl
                    total_comp_holdings += value
                    comp_portfolio.append({
                        'symbol': ch.symbol,
                        'quantity': ch.quantity,
                        'current_price': price,
                        'total_value': value,
                        'buy_price': ch.buy_price
                    })
                total_comp_value = m.cash_balance + total_comp_holdings
                competition_accounts.append({
                    'code': comp.code,
                    'name': comp.name,
                    'competition_cash': m.cash_balance,
                    'portfolio': comp_portfolio,
                    'total_value': total_comp_value,
                    'pnl': comp_pnl
                })

        team_memberships = TeamMember.query.filter_by(user_id=user.id).all()
        team_competitions = []
        for tm in team_memberships:
            ct_entries = CompetitionTeam.query.filter_by(team_id=tm.team_id).all()
            for ct in ct_entries:
                comp = db.session.get(Competition, ct.competition_id)
                if comp:
                    ct_holdings = CompetitionTeamHolding.query.filter_by(competition_team_id=ct.id).all()
                    comp_team_portfolio = []
                    total_holdings = 0
                    team_pnl = 0
                    for cht in ct_holdings:
                        try:
                            price = get_current_price(cht.symbol)
                        except Exception:
                            price = 0
                        value = price * cht.quantity
                        pnl = (price - cht.buy_price) * cht.quantity
                        team_pnl += pnl
                        total_holdings += value
                        comp_team_portfolio.append({
                            'symbol': cht.symbol,
                            'quantity': cht.quantity,
                            'current_price': price,
                            'total_value': value,
                            'buy_price': cht.buy_price
                        })
                    total_value = ct.cash_balance + total_holdings
                    team_competitions.append({
                        'code': comp.code,
                        'name': comp.name,
                        'competition_cash': ct.cash_balance,
                        'portfolio': comp_team_portfolio,
                        'total_value': total_value,
                        'team_id': ct.team_id,
                        'pnl': team_pnl
                    })

        logger.info(f"Fetched user data for {username}")
        return jsonify({
            'username': user.username,
            'is_admin': user.is_admin,
            'global_account': {
                'cash_balance': user.cash_balance,
                'portfolio': global_portfolio,
                'total_value': total_global,
                'pnl': global_pnl
            },
            'competition_accounts': competition_accounts,
            'team_competitions': team_competitions
        })
    except Exception as e:
        logger.error(f"Error fetching user data for {username}: {str(e)}")
        return jsonify({'message': 'Failed to fetch user data'}), 500

# --------------------
# Stock Endpoints
# --------------------
@app.route('/stock/<symbol>', methods=['GET'])
def get_stock(symbol):
    try:
        logger.info(f"Fetching current price for {symbol}")
        price = get_current_price(symbol)
        return jsonify({'symbol': symbol, 'price': price})
    except Exception as e:
        return jsonify({'error': f'Failed to fetch data for symbol {symbol}: {str(e)}'}), 400

@app.route('/stock_chart/<symbol>', methods=['GET'])
def stock_chart(symbol):
    range_param = request.args.get("range", "1M").upper()
    try:
        if range_param == "1D":
            function = "TIME_SERIES_INTRADAY"
            params = {
                "function": function,
                "symbol": symbol,
                "interval": "5min",
                "apikey": ALPHA_VANTAGE_API_KEY,
            }
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

        url = "https://www.alphavantage.co/query"
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        if function == "TIME_SERIES_INTRADAY":
            ts_key = next((k for k in data.keys() if "Time Series" in k), None)
        elif function == "TIME_SERIES_DAILY_ADJUSTED":
            ts_key = "Time Series (Daily)"
        elif function == "TIME_SERIES_WEEKLY_ADJUSTED":
            ts_key = "Weekly Adjusted Time Series"
        else:
            ts_key = None

        if not ts_key or ts_key not in data:
            return jsonify({"error": f"No valid data found for symbol {symbol}"}), 404

        time_series = data[ts_key]
        chart_data = []
        for date_str, data_point in list(time_series.items())[:max_points]:
            chart_data.append({
                "date": date_str,
                "close": float(data_point.get("4. close") or data_point.get("5. adjusted close") or 0)
            })

        chart_data.sort(key=lambda x: x["date"])
        logger.info(f"Fetched chart data for {symbol} with range {range_param}")
        return jsonify(chart_data)
    except Exception as e:
        logger.error(f"Error fetching chart data for {symbol}: {str(e)}")
        return jsonify({"error": f"Failed to fetch chart data for {symbol}: {str(e)}"}), 400

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
        user = User.query.filter_by(username=username).first()
        if not user:
            return jsonify({'message': 'User not found'}), 404
        cost = quantity * price
        if user.cash_balance < cost:
            return jsonify({'message': 'Insufficient funds'}), 400
        user.cash_balance -= cost
        existing = Holding.query.filter_by(user_id=user.id, symbol=symbol).first()
        if existing:
            existing.quantity += quantity
        else:
            new_hold = Holding(user_id=user.id, symbol=symbol, quantity=quantity, buy_price=price)
            db.session.add(new_hold)
        db.session.commit()
        logger.info(f"User {username} bought {quantity} shares of {symbol}")
        return jsonify({'message': 'Buy successful', 'cash_balance': user.cash_balance})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error processing buy for {username} on {symbol}: {str(e)}")
        return jsonify({'message': f'Failed to process buy: {str(e)}'}), 500

@app.route('/sell', methods=['POST'])
def sell_stock():
    data = request.get_json()
    username = data.get('username')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))
    try:
        price = get_current_price(symbol)
        user = User.query.filter_by(username=username).first()
        if not user:
            return jsonify({'message': 'User not found'}), 404
        holding = Holding.query.filter_by(user_id=user.id, symbol=symbol).first()
        if not holding or holding.quantity < quantity:
            return jsonify({'message': 'Not enough shares to sell'}), 400
        proceeds = quantity * price
        holding.quantity -= quantity
        if holding.quantity == 0:
            db.session.delete(holding)
        user.cash_balance += proceeds
        db.session.commit()
        logger.info(f"User {username} sold {quantity} shares of {symbol}")
        return jsonify({'message': 'Sell successful', 'cash_balance': user.cash_balance})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error processing sell for {username} on {symbol}: {str(e)}")
        return jsonify({'message': f'Failed to process sell: {str(e)}'}), 500

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
    feature_competition = data.get('featured', False)
    is_open = data.get('is_open', True)

    logger.info(f"Creating competition with payload: {data}")

    try:
        user = User.query.filter_by(username=username).first()
        if not user:
            logger.error(f"User {username} not found")
            return jsonify({'message': 'User not found'}), 404

        if feature_competition and not user.is_admin:
            logger.warning(f"Non-admin {username} attempted to feature competition")
            return jsonify({'message': 'Only admins can feature competitions'}), 403

        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d") if start_date_str else None
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else None
            if start_date and end_date and start_date > end_date:
                logger.error("Invalid dates: start_date after end_date")
                return jsonify({'message': 'Start date must be before end date'}), 400
        except ValueError as e:
            logger.error(f"Invalid date format: {str(e)}")
            return jsonify({'message': 'Invalid date format'}), 400

        code = secrets.token_hex(4)
        while Competition.query.filter_by(code=code).first():
            code = secrets.token_hex(4)

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
        logger.info(f"Created competition {code} with featured={feature_competition}")
        return jsonify({'message': 'Competition created successfully', 'competition_code': code})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error creating competition for {username}: {str(e)}")
        return jsonify({'message': 'Failed to create competition'}), 500

@app.route('/competition/join', methods=['POST'])
def join_competition():
    data = request.get_json()
    username = data.get('username')
    competition_code = data.get('competition_code')
    try:
        user = User.query.filter_by(username=username).first()
        if not user:
            return jsonify({'message': 'User not found'}), 404
        comp = Competition.query.filter_by(code=competition_code).first()
        if not comp:
            return jsonify({'message': 'Competition not found'}), 404
        if not comp.is_open:
            return jsonify({'message': 'Competition is restricted, use code to join'}), 403
        existing = CompetitionMember.query.filter_by(competition_id=comp.id, user_id=user.id).first()
        if existing:
            return jsonify({'message': 'User already joined this competition'}), 200
        new_member = CompetitionMember(competition_id=comp.id, user_id=user.id, cash_balance=100000)
        db.session.add(new_member)
        db.session.commit()
        logger.info(f"User {username} joined competition {competition_code}")
        return jsonify({'message': 'Successfully joined competition'})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error joining competition {competition_code} for {username}: {str(e)}")
        return jsonify({'message': 'Failed to join competition'}), 500

@app.route('/competition/buy', methods=['POST'])
def competition_buy():
    data = request.get_json()
    username = data.get('username')
    competition_code = data.get('competition_code')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))

    try:
        user = User.query.filter_by(username=username).first()
        if not user:
            return jsonify({'message': 'User not found'}), 404
        comp = Competition.query.filter_by(code=competition_code).first()
        if not comp:
            return jsonify({'message': 'Competition not found'}), 404

        now = datetime.utcnow()
        if comp.start_date and now < comp.start_date:
            return jsonify({'message': 'Competition has not started yet. No trades allowed.'}), 400
        if comp.end_date and now > comp.end_date:
            return jsonify({'message': 'Competition has ended. No trades allowed.'}), 400

        member = CompetitionMember.query.filter_by(competition_id=comp.id, user_id=user.id).first()
        if not member:
            return jsonify({'message': 'User is not a member of this competition'}), 404

        price = get_current_price(symbol)
        cost = price * quantity
        if member.cash_balance < cost:
            return jsonify({'message': 'Insufficient funds in competition account'}), 400

        member.cash_balance -= cost
        existing_holding = CompetitionHolding.query.filter_by(competition_member_id=member.id, symbol=symbol).first()
        if existing_holding:
            existing_holding.quantity += quantity
        else:
            new_holding = CompetitionHolding(
                competition_member_id=member.id,
                symbol=symbol,
                quantity=quantity,
                buy_price=price
            )
            db.session.add(new_holding)

        db.session.commit()
        logger.info(f"User {username} bought {quantity} shares of {symbol} in competition {competition_code}")
        return jsonify({'message': 'Competition buy successful', 'competition_cash': member.cash_balance})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error processing competition buy for {username} on {symbol}: {str(e)}")
        return jsonify({'message': f'Failed to process buy: {str(e)}'}), 500

@app.route('/competition/sell', methods=['POST'])
def competition_sell():
    data = request.get_json()
    username = data.get('username')
    competition_code = data.get('competition_code')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))

    try:
        user = User.query.filter_by(username=username).first()
        if not user:
            return jsonify({'message': 'User not found'}), 404
        comp = Competition.query.filter_by(code=competition_code).first()
        if not comp:
            return jsonify({'message': 'Competition not found'}), 404

        now = datetime.utcnow()
        if comp.start_date and now < comp.start_date:
            return jsonify({'message': 'Competition has not started yet. No trading allowed.'}), 400
        if comp.end_date and now > comp.end_date:
            return jsonify({'message': 'Competition has ended. No trading allowed.'}), 400

        member = CompetitionMember.query.filter_by(competition_id=comp.id, user_id=user.id).first()
        if not member:
            return jsonify({'message': 'User is not a member of this competition'}), 404

        holding = CompetitionHolding.query.filter_by(competition_member_id=member.id, symbol=symbol).first()
        if not holding or holding.quantity < quantity:
            return jsonify({'message': 'Not enough shares to sell in competition account'}), 400

        price = get_current_price(symbol)
        proceeds = price * quantity
        holding.quantity -= quantity
        if holding.quantity == 0:
            db.session.delete(holding)
        member.cash_balance += proceeds
        db.session.commit()
        logger.info(f"User {username} sold {quantity} shares of {symbol} in competition {competition_code}")
        return jsonify({'message': 'Competition sell successful', 'competition_cash': member.cash_balance})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error processing competition sell for {username} on {symbol}: {str(e)}")
        return jsonify({'message': f'Failed to process sell: {str(e)}'}), 500

# --------------------
# Team Endpoints
# --------------------
@app.route('/team/create', methods=['POST'])
def create_team():
    data = request.get_json()
    username = data.get('username')
    team_name = data.get('team_name')
    try:
        user = User.query.filter_by(username=username).first()
        if not user:
            return jsonify({'message': 'User not found'}), 404
        team = Team(name=team_name, created_by=user.id)
        db.session.add(team)
        db.session.flush()  # Ensure team.id is available
        team_member = TeamMember(team_id=team.id, user_id=user.id)
        db.session.add(team_member)
        db.session.commit()
        logger.info(f"User {username} created team {team_name} with ID {team.id}")
        return jsonify({'message': 'Team created successfully', 'team_id': team.id, 'team_code': team.id})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error creating team for {username}: {str(e)}")
        return jsonify({'message': 'Failed to create team'}), 500

@app.route('/team/join', methods=['POST'])
def join_team():
    data = request.get_json()
    username = data.get('username')
    team_code = data.get('team_code')
    try:
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
        logger.info(f"User {username} joined team {team.id}")
        return jsonify({'message': 'Joined team successfully'})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error joining team {team_code} for {username}: {str(e)}")
        return jsonify({'message': 'Failed to join team'}), 500

@app.route('/team/buy', methods=['POST'])
def team_buy():
    data = request.get_json()
    username = data.get('username')
    team_id = data.get('team_id')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))
    try:
        user = User.query.filter_by(username=username).first()
        if not user:
            return jsonify({'message': 'User not found'}), 404
        team = Team.query.get(team_id)
        if not team:
            return jsonify({'message': 'Team not found'}), 404
        if not TeamMember.query.filter_by(team_id=team_id, user_id=user.id).first():
            return jsonify({'message': 'User is not a member of this team'}), 403
        price = get_current_price(symbol)
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
        logger.info(f"Team {team_id} bought {quantity} shares of {symbol} by {username}")
        return jsonify({'message': 'Team buy successful', 'team_cash': team.cash_balance})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error processing team buy for {username} on {symbol}: {str(e)}")
        return jsonify({'message': f'Failed to process buy: {str(e)}'}), 500

@app.route('/team/sell', methods=['POST'])
def team_sell():
    data = request.get_json()
    username = data.get('username')
    team_id = data.get('team_id')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))
    try:
        user = User.query.filter_by(username=username).first()
        if not user:
            return jsonify({'message': 'User not found'}), 404
        team = Team.query.get(team_id)
        if not team:
            return jsonify({'message': 'Team not found'}), 404
        if not TeamMember.query.filter_by(team_id=team_id, user_id=user.id).first():
            return jsonify({'message': 'User is not a member of this team'}), 403
        price = get_current_price(symbol)
        proceeds = price * quantity
        holding = TeamHolding.query.filter_by(team_id=team_id, symbol=symbol).first()
        if not holding or holding.quantity < quantity:
            return jsonify({'message': 'Not enough shares to sell'}), 400
        holding.quantity -= quantity
        if holding.quantity == 0:
            db.session.delete(holding)
        team.cash_balance += proceeds
        db.session.commit()
        logger.info(f"Team {team_id} sold {quantity} shares of {symbol} by {username}")
        return jsonify({'message': 'Team sell successful', 'team_cash': team.cash_balance})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error processing team sell for {username} on {symbol}: {str(e)}")
        return jsonify({'message': f'Failed to process sell: {str(e)}'}), 500

# --------------------
# Competition Team Endpoints
# --------------------
@app.route('/competition/team/join', methods=['POST'])
def competition_team_join():
    data = request.get_json()
    username = data.get('username')
    team_code = data.get('team_code')
    competition_code = data.get('competition_code')
    try:
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
        logger.info(f"Team {team.id} joined competition {competition_code} by {username}")
        return jsonify({'message': 'Team successfully joined competition'})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error joining team {team_code} to competition {competition_code}: {str(e)}")
        return jsonify({'message': 'Failed to join competition'}), 500

@app.route('/competition/team/buy', methods=['POST'])
def competition_team_buy():
    data = request.get_json()
    username = data.get('username')
    competition_code = data.get('competition_code')
    team_id = data.get('team_id')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))

    try:
        user = User.query.filter_by(username=username).first()
        if not user:
            return jsonify({'message': 'User not found'}), 404
        comp = Competition.query.filter_by(code=competition_code).first()
        if not comp:
            return jsonify({'message': 'Competition not found'}), 404

        now = datetime.utcnow()
        if comp.start_date and now < comp.start_date:
            return jsonify({'message': 'Competition has not started yet. No trading allowed.'}), 400
        if comp.end_date and now > comp.end_date:
            return jsonify({'message': 'Competition has ended. No trading allowed.'}), 400

        comp_team = CompetitionTeam.query.filter_by(competition_id=comp.id, team_id=team_id).first()
        if not comp_team:
            return jsonify({'message': 'Team is not part of this competition'}), 404
        if not TeamMember.query.filter_by(team_id=team_id, user_id=user.id).first():
            return jsonify({'message': 'User is not a member of this team'}), 403

        price = get_current_price(symbol)
        cost = price * quantity
        if comp_team.cash_balance < cost:
            return jsonify({'message': 'Insufficient funds in competition team account'}), 400

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

        db.session.commit()
        logger.info(f"Team {team_id} bought {quantity} shares of {symbol} in competition {competition_code}")
        return jsonify({
            'message': 'Competition team buy successful',
            'competition_team_cash': comp_team.cash_balance
        })
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error processing competition team buy for {username} on {symbol}: {str(e)}")
        return jsonify({'message': f'Failed to process buy: {str(e)}'}), 500

@app.route('/competition/team/sell', methods=['POST'])
def competition_team_sell():
    data = request.get_json()
    username = data.get('username')
    competition_code = data.get('competition_code')
    team_id = data.get('team_id')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))

    try:
        user = User.query.filter_by(username=username).first()
        if not user:
            return jsonify({'message': 'User not found'}), 404
        comp = Competition.query.filter_by(code=competition_code).first()
        if not comp:
            return jsonify({'message': 'Competition not found'}), 404

        now = datetime.utcnow()
        if comp.start_date and now < comp.start_date:
            return jsonify({'message': 'Competition has not started yet. No trading allowed.'}), 400
        if comp.end_date and now > comp.end_date:
            return jsonify({'message': 'Competition has ended. No trading allowed.'}), 400

        comp_team = CompetitionTeam.query.filter_by(competition_id=comp.id, team_id=team_id).first()
        if not comp_team:
            return jsonify({'message': 'Team is not part of this competition'}), 404
        if not TeamMember.query.filter_by(team_id=team_id, user_id=user.id).first():
            return jsonify({'message': 'User is not a member of this team'}), 403

        holding = CompetitionTeamHolding.query.filter_by(
            competition_team_id=comp_team.id,
            symbol=symbol
        ).first()
        if not holding or holding.quantity < quantity:
            return jsonify({'message': 'Not enough shares to sell in competition team account'}), 400

        price = get_current_price(symbol)
        proceeds = price * quantity
        holding.quantity -= quantity
        if holding.quantity == 0:
            db.session.delete(holding)
        comp_team.cash_balance += proceeds

        db.session.commit()
        logger.info(f"Team {team_id} sold {quantity} shares of {symbol} in competition {competition_code}")
        return jsonify({
            'message': 'Competition team sell successful',
            'competition_team_cash': comp_team.cash_balance
        })
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error processing competition team sell for {username} on {symbol}: {str(e)}")
        return jsonify({'message': f'Failed to process sell: {str(e)}'}), 500

# --------------------
# Admin Endpoints
# --------------------
@app.route('/admin/competitions', methods=['GET'])
def admin_get_all_competitions():
    admin_username = request.args.get('admin_username')
    try:
        admin_user = User.query.filter_by(username=admin_username).first()
        if not admin_user or not admin_user.is_admin:
            logger.warning(f"Unauthorized access to /admin/competitions by {admin_username}")
            return jsonify({'message': 'Not authorized'}), 403
        competitions = Competition.query.all()
        competitions_data = [{
            'code': comp.code,
            'name': comp.name,
            'featured': comp.featured,
            'is_open': comp.is_open,
            'start_date': comp.start_date.isoformat() if comp.start_date else None,
            'end_date': comp.end_date.isoformat() if comp.end_date else None
        } for comp in competitions]
        logger.info(f"Returning {len(competitions_data)} competitions for admin {admin_username}")
        return jsonify(competitions_data)
    except Exception as e:
        logger.error(f"Error fetching competitions for admin {admin_username}: {str(e)}")
        return jsonify({'message': 'Failed to fetch competitions'}), 500

@app.route('/admin/delete_competition', methods=['POST'])
def admin_delete_competition():
    data = request.get_json()
    admin_username = data.get('admin_username')
    code = data.get('competition_code')
    try:
        admin_user = User.query.filter_by(username=admin_username).first()
        if not admin_user or not admin_user.is_admin:
            logger.warning(f"Unauthorized attempt to delete competition by {admin_username}")
            return jsonify({'message': 'Not authorized'}), 403
        comp = Competition.query.filter_by(code=code).first()
        if not comp:
            logger.error(f"Competition {code} not found")
            return jsonify({'message': 'Competition not found'}), 404
        db.session.delete(comp)
        db.session.commit()
        logger.info(f"Competition {code} deleted by admin {admin_username}")
        return jsonify({'message': 'Competition deleted successfully'})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error deleting competition {code} by {admin_username}: {str(e)}")
        return jsonify({'message': 'Failed to delete competition'}), 500

@app.route('/admin/delete_user', methods=['POST'])
def admin_delete_user():
    data = request.get_json()
    admin_username = data.get('admin_username')
    target_username = data.get('target_username')
    try:
        admin_user = User.query.filter_by(username=admin_username).first()
        if not admin_user or not admin_user.is_admin:
            logger.warning(f"Unauthorized attempt to delete user by {admin_username}")
            return jsonify({'message': 'Not authorized'}), 403
        target_user = User.query.filter_by(username=target_username).first()
        if not target_user:
            logger.error(f"Target user {target_username} not found")
            return jsonify({'message': 'User not found'}), 404
        db.session.delete(target_user)
        db.session.commit()
        logger.info(f"User {target_username} deleted by admin {admin_username}")
        return jsonify({'message': 'User deleted successfully'})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error deleting user {target_username} by {admin_username}: {str(e)}")
        return jsonify({'message': 'Failed to delete user'}), 500

@app.route('/admin/update_competition_open', methods=['POST'])
def admin_update_competition_open():
    data = request.get_json()
    admin_username = data.get('admin_username')
    competition_code = data.get('competition_code')
    is_open = data.get('is_open')
    try:
        admin_user = User.query.filter_by(username=admin_username).first()
        if not admin_user or not admin_user.is_admin:
            logger.warning(f"Unauthorized attempt to update competition open status by {admin_username}")
            return jsonify({'message': 'Not authorized'}), 403
        comp = Competition.query.filter_by(code=competition_code).first()
        if not comp:
            logger.error(f"Competition {competition_code} not found")
            return jsonify({'message': 'Competition not found'}), 404
        comp.is_open = is_open
        db.session.commit()
        logger.info(f"Competition {competition_code} open status set to {is_open} by {admin_username}")
        return jsonify({'message': f'Competition {competition_code} open status updated to {is_open}.'})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error updating competition {competition_code} open status: {str(e)}")
        return jsonify({'message': 'Failed to update competition status'}), 500

@app.route('/admin/remove_user_from_competition', methods=['POST'])
def remove_user_from_competition():
    data = request.get_json()
    admin_username = data.get('admin_username')
    target_username = data.get('target_username')
    competition_code = data.get('competition_code')
    try:
        admin_user = User.query.filter_by(username=admin_username).first()
        if not admin_user or not admin_user.is_admin:
            logger.warning(f"Unauthorized attempt to remove user from competition by {admin_username}")
            return jsonify({'message': 'Not authorized'}), 403
        target_user = User.query.filter_by(username=target_username).first()
        if not target_user:
            logger.error(f"Target user {target_username} not found")
            return jsonify({'message': 'Target user not found'}), 404
        comp = Competition.query.filter_by(code=competition_code).first()
        if not comp:
            logger.error(f"Competition {competition_code} not found")
            return jsonify({'message': 'Competition not found'}), 404
        membership = CompetitionMember.query.filter_by(competition_id=comp.id, user_id=target_user.id).first()
        if not membership:
            logger.error(f"User {target_username} not a member of competition {competition_code}")
            return jsonify({'message': 'User is not a member of this competition'}), 404
        db.session.delete(membership)
        db.session.commit()
        logger.info(f"User {target_username} removed from competition {competition_code} by {admin_username}")
        return jsonify({'message': f'{target_username} has been removed from competition {competition_code}.'})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error removing {target_username} from competition {competition_code}: {str(e)}")
        return jsonify({'message': 'Failed to remove user from competition'}), 500

@app.route('/admin/remove_user_from_team', methods=['POST'])
def remove_user_from_team():
    data = request.get_json()
    admin_username = data.get('admin_username')
    target_username = data.get('target_username')
    team_id = data.get('team_id')
    try:
        admin_user = User.query.filter_by(username=admin_username).first()
        if not admin_user or not admin_user.is_admin:
            logger.warning(f"Unauthorized attempt to remove user from team by {admin_username}")
            return jsonify({'message': 'Not authorized'}), 403
        target_user = User.query.filter_by(username=target_username).first()
        if not target_user:
            logger.error(f"Target user {target_username} not found")
            return jsonify({'message': 'Target user not found'}), 404
        membership = TeamMember.query.filter_by(team_id=team_id, user_id=target_user.id).first()
        if not membership:
            logger.error(f"User {target_username} not a member of team {team_id}")
            return jsonify({'message': 'User is not a member of this team'}), 404
        db.session.delete(membership)
        db.session.commit()
        logger.info(f"User {target_username} removed from team {team_id} by {admin_username}")
        return jsonify({'message': f'{target_username} has been removed from team {team_id}.'})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error removing {target_username} from team {team_id}: {str(e)}")
        return jsonify({'message': 'Failed to remove user from team'}), 500

@app.route('/admin/update_featured_status', methods=['POST'])
def update_featured_status():
    data = request.get_json()
    admin_username = data.get("admin_username")
    competition_code = data.get("competition_code")
    feature_competition = data.get("feature_competition", False)
    try:
        admin_user = User.query.filter_by(username=admin_username).first()
        if not admin_user or not admin_user.is_admin:
            logger.warning(f"Unauthorized attempt to update featured status by {admin_username}")
            return jsonify({"message": "Not authorized"}), 403
        comp = Competition.query.filter_by(code=competition_code).first()
        if not comp:
            logger.error(f"Competition {competition_code} not found")
            return jsonify({"message": "Competition not found"}), 404
        comp.featured = feature_competition
        db.session.commit()
        status = "featured" if feature_competition else "unfeatured"
        logger.info(f"Competition {competition_code} {status} by {admin_username}")
        return jsonify({"message": f"Competition {competition_code} successfully {status}."})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error updating featured status for {competition_code}: {str(e)}")
        return jsonify({'message': 'Failed to update featured status'}), 500

@app.route('/users', methods=['GET'])
def get_all_users():
    admin_username = request.args.get('admin_username')
    try:
        admin_user = User.query.filter_by(username=admin_username).first()
        if not admin_user or not admin_user.is_admin:
            logger.warning(f"Unauthorized access to /users by {admin_username}")
            return jsonify({'message': 'Not authorized'}), 403
        users = User.query.all()
        users_data = [{
            'id': user.id,
            'username': user.username,
            'is_admin': user.is_admin,
            'cash_balance': user.cash_balance
        } for user in users]
        logger.info(f"Admin {admin_username} fetched {len(users_data)} users")
        return jsonify(users_data)
    except Exception as e:
        logger.error(f"Error fetching users for admin {admin_username}: {str(e)}")
        return jsonify({'message': 'Failed to fetch users'}), 500

@app.route('/competitions', methods=['GET'])
def get_all_competitions():
    try:
        competitions = Competition.query.all()
        competitions_data = [{
            'code': comp.code,
            'name': comp.name,
            'featured': comp.featured,
            'is_open': comp.is_open
        } for comp in competitions]
        logger.info(f"Returning {len(competitions_data)} competitions")
        return jsonify(competitions_data)
    except Exception as e:
        logger.error(f"Error fetching competitions: {str(e)}")
        return jsonify({'message': 'Failed to fetch competitions'}), 500

@app.route('/featured_competitions', methods=['GET'])
def featured_competitions():
    try:
        now = datetime.utcnow()
        comps = Competition.query.filter(
            Competition.featured == True,
            (Competition.end_date == None) | (Competition.end_date >= now)
        ).all()
        result = []
        for comp in comps:
            join_instructions = "Join directly" if comp.is_open else "Use code to join"
            result.append({
                "code": comp.code,
                "name": comp.name,
                "join": join_instructions,
                "start_date": comp.start_date.isoformat() if comp.start_date else None,
                "end_date": comp.end_date.isoformat() if comp.end_date else None,
                "is_open": comp.is_open,
                "featured": comp.featured
            })
        logger.info(f"Returning {len(comps)} featured competitions")
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error fetching featured competitions: {str(e)}")
        return jsonify({'message': 'Failed to fetch featured competitions'}), 500

@app.route('/admin/set_admin', methods=['POST'])
def set_admin():
    data = request.get_json()
    secret = data.get('secret')
    username = data.get('username')
    try:
        if secret != "Timb3000!":
            logger.warning(f"Invalid secret for set_admin by {username}")
            return jsonify({'message': 'Not authorized'}), 403
        user = User.query.filter_by(username=username).first()
        if not user:
            logger.error(f"User {username} not found for set_admin")
            return jsonify({'message': 'User not found'}), 404
        user.is_admin = True
        db.session.commit()
        logger.info(f"User {username} set as admin")
        return jsonify({'message': f"{username} is now an admin."})
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error setting admin for {username}: {str(e)}")
        return jsonify({'message': 'Failed to set admin status'}), 500

@app.route('/quick_pics', methods=['GET'])
def quick_pics():
    try:
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
        logger.info(f"Returning {len(result)} Quick Pics competitions")
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error fetching Quick Pics competitions: {str(e)}")
        return jsonify({'message': 'Failed to fetch Quick Pics competitions'}), 500

def schedule_quick_pics_for_today():
    with app.app_context():
        try:
            now = datetime.utcnow()
            from_zone = tz.gettz('UTC')
            to_zone = tz.gettz('America/Los_Angeles')
            utc = now.replace(tzinfo=from_zone)
            pst_now = utc.astimezone(to_zone)

            if pst_now.weekday() >= 5:
                logger.info("Weekend detected, skipping Quick Pics creation.")
                return

            base_date = pst_now.replace(hour=7, minute=0, second=0, microsecond=0)
            for i in range(6):
                start_pst = base_date + timedelta(hours=i)
                end_pst = start_pst + timedelta(hours=1)
                start_utc = start_pst.astimezone(from_zone).replace(tzinfo=None)
                end_utc = end_pst.astimezone(from_zone).replace(tzinfo=None)

                code = secrets.token_hex(4)
                while Competition.query.filter_by(code=code).first():
                    code = secrets.token_hex(4)
                quick_comp = Competition(
                    code=code,
                    name="Quick Pics",
                    created_by=1,
                    start_date=start_utc,
                    end_date=end_utc,
                    featured=True,
                    max_position_limit="",
                    is_open=True
                )
                db.session.add(quick_comp)
                db.session.commit()
                logger.info(f"Created Quick Pics competition {code} from {start_pst} - {end_pst}")
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error scheduling Quick Pics: {str(e)}")

# --------------------
# Leaderboard Endpoints
# --------------------
@app.route('/competition/<code>/leaderboard', methods=['GET'])
def competition_leaderboard(code):
    try:
        comp = Competition.query.filter_by(code=code).first()
        if not comp:
            logger.error(f"Competition {code} not found")
            return jsonify({'message': 'Competition not found'}), 404
        leaderboard = []
        members = CompetitionMember.query.filter_by(competition_id=comp.id).all()
        for m in members:
            total = m.cash_balance
            choldings = CompetitionHolding.query.filter_by(competition_member_id=m.id).all()
            for h in choldings:
                try:
                    price = get_current_price(h.symbol)
                except Exception:
                    price = 0
                total += price * h.quantity
            user = db.session.get(User, m.user_id)
            leaderboard.append({'name': user.username, 'total_value': total})
        leaderboard_sorted = sorted(leaderboard, key=lambda x: x['total_value'], reverse=True)
        logger.info(f"Fetched leaderboard for competition {code}")
        return jsonify(leaderboard_sorted)
    except Exception as e:
        logger.error(f"Error fetching leaderboard for {code}: {str(e)}")
        return jsonify({'message': 'Failed to fetch leaderboard'}), 500

@app.route('/competition/<code>/team_leaderboard', methods=['GET'])
def competition_team_leaderboard(code):
    try:
        comp = Competition.query.filter_by(code=code).first()
        if not comp:
            logger.error(f"Competition {code} not found")
            return jsonify({'message': 'Competition not found'}), 404
        leaderboard = []
        comp_teams = CompetitionTeam.query.filter_by(competition_id=comp.id).all()
        for ct in comp_teams:
            total = ct.cash_balance
            tholdings = CompetitionTeamHolding.query.filter_by(competition_team_id=ct.id).all()
            for h in tholdings:
                try:
                    price = get_current_price(h.symbol)
                except Exception:
                    price = 0
                total += price * h.quantity
            team = db.session.get(Team, ct.team_id)
            leaderboard.append({'name': team.name, 'total_value': total})
        leaderboard_sorted = sorted(leaderboard, key=lambda x: x['total_value'], reverse=True)
        logger.info(f"Fetched team leaderboard for competition {code}")
        return jsonify(leaderboard_sorted)
    except Exception as e:
        logger.error(f"Error fetching team leaderboard for {code}: {str(e)}")
        return jsonify({'message': 'Failed to fetch team leaderboard'}), 500

# --------------------
# Scheduler Setup
# --------------------
scheduler = BackgroundScheduler()
scheduler.add_job(schedule_quick_pics_for_today, trigger=CronTrigger(hour=0, minute=0, second=0, timezone='America/Los_Angeles'))
scheduler.start()

# --------------------
# Run the app
# --------------------
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)