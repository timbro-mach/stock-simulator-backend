from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from flask_sqlalchemy import SQLAlchemy
import requests
import secrets
from datetime import datetime
import os

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "https://stock-simulator-frontend.vercel.app"}})

# Database setup
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///stock_simulator.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Alpha Vantage API Key
ALPHA_VANTAGE_API_KEY = "2QZ58MHB8CG5PYYJ"

# --------------------
# Models
# --------------------
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    cash_balance = db.Column(db.Float, default=100000)

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

# --------------------
# New Models for Teams
# --------------------
class Team(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), nullable=False)
    created_by = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    cash_balance = db.Column(db.Float, default=100000)  # Global team account cash

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
    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={ALPHA_VANTAGE_API_KEY}"
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
# Endpoints for Global Trading
# --------------------
@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    if User.query.filter_by(username=username).first():
        return jsonify({'message': 'User already exists'}), 400
    new_user = User(username=username)
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
                    'portfolio': []  # To be populated if trades exist.
                })
        # Also retrieve teams that the user is a member of (global team accounts)
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
        return jsonify({
            'message': 'Login successful',
            'username': user.username,
            'cash_balance': user.cash_balance,
            'global_account': {'cash_balance': user.cash_balance},
            'competition_accounts': competition_accounts,
            'teams': teams
        })
    else:
        return jsonify({'message': 'Invalid credentials'}), 401

# New stock endpoint using Alpha Vantage for current price
@app.route('/stock/<symbol>', methods=['GET'])
def get_stock(symbol):
    try:
        app.logger.info(f"Fetching current price for {symbol}")
        price = get_current_price(symbol)
        return jsonify({'symbol': symbol, 'price': price})
    except Exception as e:
        app.logger.error(f"Error fetching data for {symbol}: {e}")
        return jsonify({'error': f'Failed to fetch data for symbol {symbol}: {str(e)}'}), 400

# Stock chart endpoint using Alpha Vantage Daily Time Series
@app.route('/stock_chart/<symbol>', methods=['GET'])
def stock_chart(symbol):
    try:
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={ALPHA_VANTAGE_API_KEY}"
        response = requests.get(url)
        if response.status_code != 200:
            return jsonify({'error': f"Alpha Vantage API error: {response.status_code}"}), 400
        data = response.json()
        if "Time Series (Daily)" not in data:
            return jsonify({'error': f'No time series data found for symbol {symbol}'}), 404
        time_series = data["Time Series (Daily)"]
        chart_data = []
        for date_str, day_data in time_series.items():
            chart_data.append({
                'date': date_str,
                'close': float(day_data["4. close"])
            })
        chart_data.sort(key=lambda x: x['date'])
        return jsonify(chart_data)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch chart data for symbol {symbol}: {str(e)}'}), 400

# Global buy endpoint using Alpha Vantage
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
    db.session.commit()
    return jsonify({'message': 'Buy successful', 'cash_balance': user.cash_balance})

# Global sell endpoint using Alpha Vantage
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
    holding.quantity -= quantity
    if holding.quantity == 0:
        db.session.delete(holding)
    user.cash_balance += proceeds
    db.session.commit()
    return jsonify({'message': 'Sell successful', 'cash_balance': user.cash_balance})

# Global user data endpoint (calculates portfolio values using current prices)
@app.route('/user', methods=['GET'])
def get_user():
    username = request.args.get('username')
    if not username:
        return jsonify({'message': 'Username is required'}), 400
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    holdings = Holding.query.filter_by(user_id=user.id).all()
    global_portfolio = []
    total_global = user.cash_balance
    for h in holdings:
        try:
            price = get_current_price(h.symbol)
        except Exception as e:
            price = 0
        value = price * h.quantity
        total_global += value
        global_portfolio.append({
            'symbol': h.symbol,
            'quantity': h.quantity,
            'current_price': price,
            'total_value': value,
            'buy_price': h.buy_price
        })
    
    memberships = CompetitionMember.query.filter_by(user_id=user.id).all()
    competition_accounts = []
    for m in memberships:
        comp = db.session.get(Competition, m.competition_id)
        if comp:
            comp_holdings = CompetitionHolding.query.filter_by(competition_member_id=m.id).all()
            comp_portfolio = []
            total_comp_holdings = 0
            for ch in comp_holdings:
                try:
                    price = get_current_price(ch.symbol)
                except Exception as e:
                    price = 0
                value = price * ch.quantity
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
                'total_value': total_comp_value
            })

    return jsonify({
        'username': user.username,
        'global_account': {
            'cash_balance': user.cash_balance,
            'portfolio': global_portfolio,
            'total_value': total_global
        },
        'competition_accounts': competition_accounts
    })

# --------------------
# Competition Endpoints
# --------------------
@app.route('/competition/create', methods=['POST'])
def create_competition():
    data = request.get_json()
    username = data.get('username')
    name = data.get('name')
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    code = secrets.token_hex(4)
    while Competition.query.filter_by(code=code).first():
        code = secrets.token_hex(4)
    comp = Competition(code=code, name=name, created_by=user.id)
    db.session.add(comp)
    db.session.commit()
    return jsonify({'message': 'Competition created successfully', 'competition_code': code})

@app.route('/competition/join', methods=['POST'])
def join_competition():
    data = request.get_json()
    username = data.get('username')
    code = data.get('competition_code')
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    comp = Competition.query.filter_by(code=code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404
    existing = CompetitionMember.query.filter_by(competition_id=comp.id, user_id=user.id).first()
    if existing:
        return jsonify({'message': 'User already joined this competition'}), 200
    new_member = CompetitionMember(competition_id=comp.id, user_id=user.id, cash_balance=100000)
    db.session.add(new_member)
    db.session.commit()
    return jsonify({'message': 'Successfully joined competition'})

# Competition buy endpoint (individual)
@app.route('/competition/buy', methods=['POST'])
def competition_buy():
    data = request.get_json()
    username = data.get('username')
    competition_code = data.get('competition_code')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    comp = Competition.query.filter_by(code=competition_code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404
    member = CompetitionMember.query.filter_by(competition_id=comp.id, user_id=user.id).first()
    if not member:
        return jsonify({'message': 'User is not a member of this competition'}), 404
    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': f'Error fetching price for symbol {symbol}: {str(e)}'}), 400
    cost = price * quantity
    if member.cash_balance < cost:
        return jsonify({'message': 'Insufficient funds in competition account'}), 400
    member.cash_balance -= cost
    existing_holding = CompetitionHolding.query.filter_by(competition_member_id=member.id, symbol=symbol).first()
    if existing_holding:
        existing_holding.quantity += quantity
    else:
        new_holding = CompetitionHolding(competition_member_id=member.id, symbol=symbol, quantity=quantity, buy_price=price)
        db.session.add(new_holding)
    db.session.commit()
    return jsonify({'message': 'Competition buy successful', 'competition_cash': member.cash_balance})

# Competition sell endpoint (individual)
@app.route('/competition/sell', methods=['POST'])
def competition_sell():
    data = request.get_json()
    username = data.get('username')
    competition_code = data.get('competition_code')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    comp = Competition.query.filter_by(code=competition_code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404
    member = CompetitionMember.query.filter_by(competition_id=comp.id, user_id=user.id).first()
    if not member:
        return jsonify({'message': 'User is not a member of this competition'}), 404
    holding = CompetitionHolding.query.filter_by(competition_member_id=member.id, symbol=symbol).first()
    if not holding or holding.quantity < quantity:
        return jsonify({'message': 'Not enough shares to sell in competition account'}), 400
    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': f'Error fetching price for symbol {symbol}: {str(e)}'}), 400
    proceeds = price * quantity
    holding.quantity -= quantity
    if holding.quantity == 0:
        db.session.delete(holding)
    member.cash_balance += proceeds
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
    
    # Add the creator as the first team member.
    team_member = TeamMember(team_id=team.id, user_id=user.id)
    db.session.add(team_member)
    db.session.commit()
    
    # Using team id as team_code for simplicity
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
    
    # Ensure the user is a member of the team.
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
    competition_code = data.get('competition_code')
    team_code = data.get('team_code')
    
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    
    # Verify team exists and that the user is a member of the team.
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
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))
    
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    comp = Competition.query.filter_by(code=competition_code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404

    # Verify that the team is participating in this competition.
    comp_team = CompetitionTeam.query.filter_by(competition_id=comp.id, team_id=team_id).first()
    if not comp_team:
        return jsonify({'message': 'Team is not part of this competition'}), 404

    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': f'Error fetching price for symbol {symbol}: {str(e)}'}), 400

    cost = price * quantity
    if comp_team.cash_balance < cost:
        return jsonify({'message': 'Insufficient funds in competition team account'}), 400

    comp_team.cash_balance -= cost
    holding = CompetitionTeamHolding.query.filter_by(competition_team_id=comp_team.id, symbol=symbol).first()
    if holding:
        holding.quantity += quantity
    else:
        new_holding = CompetitionTeamHolding(competition_team_id=comp_team.id, symbol=symbol, quantity=quantity, buy_price=price)
        db.session.add(new_holding)
    db.session.commit()
    return jsonify({'message': 'Competition team buy successful', 'competition_team_cash': comp_team.cash_balance})

@app.route('/competition/team/sell', methods=['POST'])
def competition_team_sell():
    data = request.get_json()
    username = data.get('username')
    competition_code = data.get('competition_code')
    team_id = data.get('team_id')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))
    
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    comp = Competition.query.filter_by(code=competition_code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404

    comp_team = CompetitionTeam.query.filter_by(competition_id=comp.id, team_id=team_id).first()
    if not comp_team:
        return jsonify({'message': 'Team is not part of this competition'}), 404

    try:
        price = get_current_price(symbol)
    except Exception as e:
        return jsonify({'message': f'Error fetching price for symbol {symbol}: {str(e)}'}), 400

    proceeds = price * quantity
    holding = CompetitionTeamHolding.query.filter_by(competition_team_id=comp_team.id, symbol=symbol).first()
    if not holding or holding.quantity < quantity:
        return jsonify({'message': 'Not enough shares to sell in competition team account'}), 400
    holding.quantity -= quantity
    if holding.quantity == 0:
        db.session.delete(holding)
    comp_team.cash_balance += proceeds
    db.session.commit()
    return jsonify({'message': 'Competition team sell successful', 'competition_team_cash': comp_team.cash_balance})

# --------------------
# Unified Competition Leaderboard (Individuals and Teams)
# --------------------
@app.route('/competition/<code>/leaderboard', methods=['GET'])
def competition_leaderboard(code):
    comp = Competition.query.filter_by(code=code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404
    leaderboard = []
    # Process individual competition members.
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
    # Process competition teams.
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
    return jsonify(leaderboard_sorted)

# --------------------
# Competition Member List Endpoint
# --------------------
@app.route('/competition/member', methods=['GET'])
def competition_member():
    username = request.args.get('username')
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    memberships = CompetitionMember.query.filter_by(user_id=user.id).all()
    comps = []
    for m in memberships:
        comp = db.session.get(Competition, m.competition_id)
        if comp:
            comps.append({
                'code': comp.code,
                'name': comp.name,
                'competition_cash': m.cash_balance,
                'created_at': comp.created_at.strftime('%Y-%m-%d %H:%M:%S')
            })
    return jsonify(comps)

# --------------------
# Run the app
# --------------------
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(debug=True, host='0.0.0.0', port=port)
