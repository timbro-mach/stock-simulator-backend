from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from flask_sqlalchemy import SQLAlchemy
import yfinance as yf
import secrets
from datetime import datetime

app = Flask(__name__)
from flask_cors import CORS

CORS(app, resources={r"/*": {"origins": "https://stock-simulator-frontend.vercel.app"}})


# Database setup
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///stock_simulator.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# --------------------
# Models
# --------------------

# Global User model (paper trading account)
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    cash_balance = db.Column(db.Float, default=100000)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

# Global Holding model (for global paper trading)
class Holding(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    symbol = db.Column(db.String(10), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    buy_price = db.Column(db.Float, nullable=False)

# Competition model (defines a group competition)
class Competition(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(16), unique=True, nullable=False)  # Unique competition code
    name = db.Column(db.String(80), nullable=True)  # Optional competition name
    created_by = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# CompetitionMember model represents a competition account.
class CompetitionMember(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    competition_id = db.Column(db.Integer, db.ForeignKey('competition.id'), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    cash_balance = db.Column(db.Float, default=100000)  # Competition starting balance
    __table_args__ = (db.UniqueConstraint('competition_id', 'user_id', name='_competition_user_uc'),)

# CompetitionHolding model stores trades made in a competition.
class CompetitionHolding(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    competition_member_id = db.Column(db.Integer, db.ForeignKey('competition_member.id'), nullable=False)
    symbol = db.Column(db.String(10), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    buy_price = db.Column(db.Float, nullable=False)

# Create tables (if necessary)
with app.app_context():
    db.create_all()

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
                # If no competition trades, total equals competition cash.
                competition_accounts.append({
                    'code': comp.code,
                    'name': comp.name,
                    'competition_cash': m.cash_balance,
                    'total_value': m.cash_balance,
                    'portfolio': []  # Backend should return portfolio details if trades exist.
                })
        return jsonify({
            'message': 'Login successful',
            'username': user.username,
            'cash_balance': user.cash_balance,
            'global_account': { 'cash_balance': user.cash_balance },
            'competition_accounts': competition_accounts
        })
    else:
        return jsonify({'message': 'Invalid credentials'}), 401

@app.route('/stock/<symbol>', methods=['GET'])
def get_stock(symbol):
    try:
        stock = yf.Ticker(symbol)
        data = stock.history(period='1d')
        if data.empty:
            return jsonify({'error': f'No data found for symbol {symbol}'}), 404
        price = float(data['Close'].iloc[-1])
        return jsonify({'symbol': symbol, 'price': price})
    except Exception as e:
        return jsonify({'error': f'Failed to fetch data: {str(e)}'}), 400

# New Global Leaderboard Endpoint (for global account)
# Returns only the current user's global performance.
@app.route('/global_leaderboard', methods=['GET'])
def global_leaderboard():
    username = request.args.get('username')
    if not username:
        return jsonify({'message': 'Username is required'}), 400
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    holdings = Holding.query.filter_by(user_id=user.id).all()
    total = user.cash_balance
    for h in holdings:
        stock = yf.Ticker(h.symbol)
        d = stock.history(period='1d')
        if not d.empty:
            price = float(d['Close'].iloc[-1])
            total += price * h.quantity
    return jsonify([{'username': user.username, 'total_value': total}])

# Global buy endpoint
@app.route('/buy', methods=['POST'])
def buy_stock():
    data = request.get_json()
    username = data.get('username')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))
    stock = yf.Ticker(symbol)
    data_stock = stock.history(period='1d')
    if data_stock.empty:
        return jsonify({'message': f'Invalid symbol: {symbol}'}), 400
    price = float(data_stock['Close'].iloc[-1])
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

# Global sell endpoint
@app.route('/sell', methods=['POST'])
def sell_stock():
    data = request.get_json()
    username = data.get('username')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))
    stock = yf.Ticker(symbol)
    data_stock = stock.history(period='1d')
    if data_stock.empty:
        return jsonify({'message': f'Invalid symbol: {symbol}'}), 400
    price = float(data_stock['Close'].iloc[-1])
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

# Global user data endpoint (returns global portfolio and competition accounts)
@app.route('/user', methods=['GET'])
def get_user():
    username = request.args.get('username')
    if not username:
        return jsonify({'message': 'Username is required'}), 400
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    # Global account details
    holdings = Holding.query.filter_by(user_id=user.id).all()
    global_portfolio = []
    total_global = user.cash_balance
    for h in holdings:
        stock = yf.Ticker(h.symbol)
        d = stock.history(period='1d')
        if not d.empty:
            price = float(d['Close'].iloc[-1])
            value = price * h.quantity
            total_global += value
            global_portfolio.append({
                'symbol': h.symbol,
                'quantity': h.quantity,
                'current_price': price,
                'total_value': value,
                'buy_price': h.buy_price
            })
    
    # Competition accounts details
    memberships = CompetitionMember.query.filter_by(user_id=user.id).all()
    competition_accounts = []
    for m in memberships:
        comp = db.session.get(Competition, m.competition_id)
        if comp:
            comp_holdings = CompetitionHolding.query.filter_by(competition_member_id=m.id).all()
            comp_portfolio = []
            total_comp_holdings = 0
            for ch in comp_holdings:
                stock = yf.Ticker(ch.symbol)
                d = stock.history(period='1d')
                if not d.empty:
                    price = float(d['Close'].iloc[-1])
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

@app.route('/stock_chart/<symbol>', methods=['GET'])
def stock_chart(symbol):
    try:
        stock = yf.Ticker(symbol)
        data = stock.history(period='1y')
        if data.empty:
            return jsonify([]), 404
        chart_data = []
        for date, row in data.iterrows():
            chart_data.append({
                'date': date.strftime('%Y-%m-%d'),
                'close': float(row['Close'])
            })
        return jsonify(chart_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# --------------------
# Competition Endpoints (Competition Trading)
# --------------------

# Create a competition (does NOT add the creator automatically)
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

# Join a competition (creates a new competition account for the user)
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

# Competition buy endpoint
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
    stock = yf.Ticker(symbol)
    data_stock = stock.history(period='1d')
    if data_stock.empty:
        return jsonify({'message': f'Invalid symbol: {symbol}'}), 400
    price = float(data_stock['Close'].iloc[-1])
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

# Competition sell endpoint
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
    stock = yf.Ticker(symbol)
    data_stock = stock.history(period='1d')
    if data_stock.empty:
        return jsonify({'message': f'Invalid symbol: {symbol}'}), 400
    price = float(data_stock['Close'].iloc[-1])
    proceeds = price * quantity
    holding.quantity -= quantity
    if holding.quantity == 0:
        db.session.delete(holding)
    member.cash_balance += proceeds
    db.session.commit()
    return jsonify({'message': 'Competition sell successful', 'competition_cash': member.cash_balance})

# Competition leaderboard (unique to each competition)
@app.route('/competition/<code>/leaderboard', methods=['GET'])
def competition_leaderboard(code):
    comp = Competition.query.filter_by(code=code).first()
    if not comp:
        return jsonify({'message': 'Competition not found'}), 404
    members = CompetitionMember.query.filter_by(competition_id=comp.id).all()
    leaderboard = []
    for m in members:
        total = m.cash_balance
        choldings = CompetitionHolding.query.filter_by(competition_member_id=m.id).all()
        for h in choldings:
            stock = yf.Ticker(h.symbol)
            d = stock.history(period='1d')
            if not d.empty:
                price = float(d['Close'].iloc[-1])
                total += price * h.quantity
        user = db.session.get(User, m.user_id)
        leaderboard.append({'username': user.username, 'total_value': total})
    leaderboard_sorted = sorted(leaderboard, key=lambda x: x['total_value'], reverse=True)
    return jsonify(leaderboard_sorted)

# Get competitions that a user is a member of
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
import os
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(debug=True, host='0.0.0.0', port=port)

