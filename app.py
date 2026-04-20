from flask import Flask, request, jsonify
from werkzeug.exceptions import HTTPException
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, inspect, text
import requests, secrets
from datetime import datetime, timedelta, timezone, date
from dateutil import tz
import logging
logging.basicConfig(level=logging.INFO)
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import os
import hashlib
import msal
import html
import re

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


def _error_payload(message, code):
    return {"message": message, "code": code}


@app.errorhandler(404)
def handle_not_found(err):
    if isinstance(err, HTTPException) and err.description and err.description != "404 Not Found: The requested URL was not found on the server. If you entered the URL manually please check your spelling and try again.":
        message = err.description
    else:
        message = "Not found"
    return jsonify(_error_payload(message, "not_found")), 404

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

class Curriculum(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    competition_id = db.Column(db.Integer, db.ForeignKey('competition.id'), nullable=False, unique=True, index=True)
    enabled = db.Column(db.Boolean, nullable=False, default=False)
    total_weeks = db.Column(db.Integer, nullable=False)
    start_date = db.Column(db.DateTime, nullable=False)
    end_date = db.Column(db.DateTime, nullable=False)
    # Default off for back-compat with live cohorts; new competitions opt in.
    enforce_prerequisites = db.Column(db.Boolean, nullable=False, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

class CurriculumModule(db.Model):
    __tablename__ = 'curriculum_module'
    id = db.Column(db.Integer, primary_key=True)
    curriculum_id = db.Column(db.Integer, db.ForeignKey('curriculum.id'), nullable=False, index=True)
    week_number = db.Column(db.Integer, nullable=False)
    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text, nullable=True)
    lesson_content = db.Column(db.Text, nullable=True)
    unlock_date = db.Column(db.DateTime, nullable=False)
    due_date = db.Column(db.DateTime, nullable=False)
    prerequisite_module_id = db.Column(db.Integer, db.ForeignKey('curriculum_module.id'), nullable=True)
    passing_threshold = db.Column(db.Float, nullable=False, default=70.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    __table_args__ = (db.UniqueConstraint('curriculum_id', 'week_number', name='_curriculum_week_uc'),)

class CurriculumAssignment(db.Model):
    __tablename__ = 'curriculum_assignment'
    id = db.Column(db.Integer, primary_key=True)
    module_id = db.Column(db.Integer, db.ForeignKey('curriculum_module.id'), nullable=False, index=True)
    type = db.Column(db.String(32), nullable=False)  # quiz | assignment | exam
    title = db.Column(db.String(255), nullable=False)
    content_json = db.Column(db.JSON, nullable=False)
    answer_key_json = db.Column(db.JSON, nullable=True)
    points = db.Column(db.Integer, nullable=False, default=100)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

class CurriculumSubmission(db.Model):
    __tablename__ = 'curriculum_submission'
    id = db.Column(db.Integer, primary_key=True)
    assignment_id = db.Column(db.Integer, db.ForeignKey('curriculum_assignment.id'), nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    competition_id = db.Column(db.Integer, db.ForeignKey('competition.id'), nullable=False, index=True)
    answers_json = db.Column(db.JSON, nullable=False)
    # Snapshot of the quiz/exam question order + answer key taken when the student first opens the quiz.
    # Ensures grading uses the exact questions the student saw even if assignment.content_json is edited mid-attempt.
    question_order_json = db.Column(db.JSON, nullable=True)
    score = db.Column(db.Float, nullable=False, default=0.0)
    percentage = db.Column(db.Float, nullable=False, default=0.0)
    submitted_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    auto_graded = db.Column(db.Boolean, nullable=False, default=False)
    feedback_json = db.Column(db.JSON, nullable=True)
    question_1_score = db.Column(db.Float, nullable=True)
    question_2_score = db.Column(db.Float, nullable=True)
    assignment_total_score = db.Column(db.Float, nullable=True)
    graded_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True, index=True)
    graded_at = db.Column(db.DateTime, nullable=True)
    rubric_notes = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    # Duplicate submission rule: keep one active submission per user per assignment and overwrite on re-submit.
    __table_args__ = (db.UniqueConstraint('assignment_id', 'user_id', name='_curriculum_assignment_user_uc'),)

class SubmissionQuestionGrade(db.Model):
    __tablename__ = 'submission_question_grades'
    id = db.Column(db.Integer, primary_key=True)
    submission_id = db.Column(db.Integer, db.ForeignKey('curriculum_submission.id'), nullable=False, index=True)
    question_id = db.Column(db.String(128), nullable=False)
    points_awarded = db.Column(db.Float, nullable=False, default=0.0)
    points_possible = db.Column(db.Float, nullable=False, default=0.0)
    feedback = db.Column(db.Text, nullable=True)
    graded_by = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, index=True)
    graded_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    __table_args__ = (db.UniqueConstraint('submission_id', 'question_id', name='_submission_question_uc'),)

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


class AccountPerformanceHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), nullable=False)
    account_id = db.Column(db.String(80), nullable=False)
    account_type = db.Column(db.String(32), nullable=False)
    date = db.Column(db.Date, nullable=False)
    total_value = db.Column(db.Float, nullable=False)
    cash = db.Column(db.Float, nullable=False)
    total_pnl = db.Column(db.Float, nullable=True)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow, index=True)
    __table_args__ = (
        db.UniqueConstraint('username', 'account_type', 'account_id', 'date', name='_account_performance_daily_uc'),
        db.Index('ix_account_performance_lookup', 'username', 'account_type', 'account_id', 'date'),
    )

with app.app_context():
    db.create_all()


def ensure_schema_compatibility():
    """Best-effort additive schema sync for deployments without migrations.

    Each ALTER runs in its own transaction so one dialect-specific failure does
    not roll back the other additive migrations.
    """
    def _safe_exec(sql):
        try:
            db.session.execute(text(sql))
            db.session.commit()
        except Exception:
            db.session.rollback()
            logger.exception('Schema compatibility step failed for: %s', sql)

    try:
        insp = inspect(db.engine)
        table_names = insp.get_table_names()
        dialect = db.engine.dialect.name  # 'postgresql', 'sqlite', etc.
        bool_false = 'FALSE' if dialect == 'postgresql' else '0'

        if 'competition_team' in table_names:
            existing_cols = {c['name'] for c in insp.get_columns('competition_team')}
            needed = {
                'start_of_day_value': 'DOUBLE PRECISION DEFAULT 100000.0',
                'realized_pnl': 'DOUBLE PRECISION DEFAULT 0.0',
            }
            for col_name, col_type in needed.items():
                if col_name in existing_cols:
                    continue
                db.session.execute(text(f'ALTER TABLE competition_team ADD COLUMN {col_name} {col_type}'))

        if 'account_performance_history' in table_names:
            existing_cols = {c['name'] for c in insp.get_columns('account_performance_history')}
            performance_needed = {
                'total_pnl': 'DOUBLE PRECISION',
                'updated_at': 'TIMESTAMP',
            }
            for col_name, col_type in performance_needed.items():
                if col_name in existing_cols:
                    continue
                db.session.execute(text(f'ALTER TABLE account_performance_history ADD COLUMN {col_name} {col_type}'))

            existing_indexes = {idx['name'] for idx in insp.get_indexes('account_performance_history')}
            if 'ix_account_performance_lookup' not in existing_indexes:
                db.session.execute(text(
                    'CREATE INDEX IF NOT EXISTS ix_account_performance_lookup '
                    'ON account_performance_history (username, account_type, account_id, date)'
                ))
            if '_account_performance_daily_uc' not in existing_indexes:
                db.session.execute(text(
                    'CREATE UNIQUE INDEX IF NOT EXISTS _account_performance_daily_uc '
                    'ON account_performance_history (username, account_type, account_id, date)'
                ))
        if 'curriculum' in table_names:
            existing_cols = {c['name'] for c in insp.get_columns('curriculum')}
            if 'enforce_prerequisites' not in existing_cols:
                # Run in its own transaction and use dialect-appropriate boolean default.
                # Postgres rejects "DEFAULT 0" for BOOLEAN; it requires FALSE. SQLite accepts 0.
                _safe_exec(
                    f'ALTER TABLE curriculum ADD COLUMN enforce_prerequisites BOOLEAN NOT NULL DEFAULT {bool_false}'
                )
        if 'curriculum_module' in table_names:
            existing_cols = {c['name'] for c in insp.get_columns('curriculum_module')}
            if 'lesson_content' not in existing_cols:
                _safe_exec('ALTER TABLE curriculum_module ADD COLUMN lesson_content TEXT')
            if 'prerequisite_module_id' not in existing_cols:
                _safe_exec('ALTER TABLE curriculum_module ADD COLUMN prerequisite_module_id INTEGER')
            if 'passing_threshold' not in existing_cols:
                _safe_exec(
                    'ALTER TABLE curriculum_module ADD COLUMN passing_threshold DOUBLE PRECISION NOT NULL DEFAULT 70.0'
                )
        if 'curriculum_submission' in table_names:
            existing_cols = {c['name'] for c in insp.get_columns('curriculum_submission')}
            submission_needed = {
                'question_1_score': 'DOUBLE PRECISION',
                'question_2_score': 'DOUBLE PRECISION',
                'assignment_total_score': 'DOUBLE PRECISION',
                'graded_by_user_id': 'INTEGER',
                'graded_at': 'TIMESTAMP',
                'rubric_notes': 'TEXT',
                'question_order_json': 'TEXT',
            }
            for col_name, col_type in submission_needed.items():
                if col_name in existing_cols:
                    continue
                _safe_exec(f'ALTER TABLE curriculum_submission ADD COLUMN {col_name} {col_type}')
        if 'submission_question_grades' not in table_names:
            db.session.execute(text(
                'CREATE TABLE submission_question_grades ('
                'id INTEGER PRIMARY KEY, '
                'submission_id INTEGER NOT NULL, '
                'question_id VARCHAR(128) NOT NULL, '
                'points_awarded DOUBLE PRECISION NOT NULL DEFAULT 0.0, '
                'points_possible DOUBLE PRECISION NOT NULL DEFAULT 0.0, '
                'feedback TEXT, '
                'graded_by INTEGER NOT NULL, '
                'graded_at TIMESTAMP NOT NULL, '
                'FOREIGN KEY(submission_id) REFERENCES curriculum_submission (id), '
                'FOREIGN KEY(graded_by) REFERENCES user (id), '
                'UNIQUE(submission_id, question_id)'
                ')'
            ))
        else:
            existing_cols = {c['name'] for c in insp.get_columns('submission_question_grades')}
            sqg_needed = {
                'feedback': 'TEXT',
                'graded_by': 'INTEGER',
                'graded_at': 'TIMESTAMP',
            }
            for col_name, col_type in sqg_needed.items():
                if col_name in existing_cols:
                    continue
                db.session.execute(text(f'ALTER TABLE submission_question_grades ADD COLUMN {col_name} {col_type}'))
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception('Schema compatibility step failed')

with app.app_context():
    ensure_schema_compatibility()


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


BASE_CURRICULUM_TOPICS = [
    ("Introduction to Investing and Markets", "Market structure, asset classes, and long-term investing principles."),
    ("Risk and Return", "Return drivers, volatility, drawdowns, and risk-adjusted thinking."),
    ("Diversification", "Correlation, concentration risk, and portfolio diversification approaches."),
    ("Asset Allocation", "Strategic vs tactical allocation and matching risk profile to goals."),
    ("Stocks, ETFs, and Funds", "Vehicle selection, liquidity, fees, and exposure design."),
    ("Fundamental Analysis", "Business quality, valuation basics, and financial statement signals."),
    ("Technical Analysis", "Trend, momentum, support/resistance, and practical chart interpretation."),
    ("Behavioral Finance", "Cognitive biases, decision hygiene, and process over prediction."),
    ("Portfolio Construction", "Position sizing, rebalancing, and constraints management."),
    ("Market Events, Macroeconomics, and Review", "Rates, inflation, events, and integrated review."),
]
CURRICULUM_CONTENT_VERSION = "2026.04"
MODULE_1_CUSTOM_ETEXT = """## Module 1: Introduction to Investing & Markets
### How the Game Actually Works

### Start Here: Investing Is a Decision Process
Most people think investing is about picking stocks.

It's not.

Investing is a process. And if you get the process right, your results tend to follow.

That process looks like this:
- **Allocation** – How do you divide your money across asset classes?
- **Diversification** – How do you spread risk within those allocations?
- **Position Sizing** – How much do you put into each investment?
- **Implementation** – How do you actually execute (costs, timing, discipline)?

You will see these ideas over and over again—in this course and in your portfolio.

### Market Structure: Where Trades Actually Happen
When you invest, you're operating in the secondary market—where investors trade with each other.

Most trades happen through exchanges like:
- New York Stock Exchange
- NASDAQ

Or through market makers who provide liquidity.

### Why Liquidity Matters (This Shows Up on the Quiz)
Every trade has a bid and ask:
- **Bid** = what buyers will pay
- **Ask** = what sellers want
- **Spread** = the difference

If an asset has low liquidity (not many buyers/sellers):
- Spreads widen
- Costs increase
- Execution gets worse

This is called **liquidity risk**.

Key idea: Thin trading volume can increase your costs—even if the investment idea is good.

### Asset Classes: Your Building Blocks
A portfolio isn't just one investment—it's a combination.

- **Stocks (Growth Engine)**  
  High return potential, high volatility
- **Bonds (Stability + Income)**  
  Lower returns, lower volatility
- **Cash (Flexibility)**  
  Low return, low risk, and opportunity to deploy later

### Asset Allocation: The Big Lever
Asset allocation is the primary driver of long-run portfolio behavior.

Not stock picking. Not timing.

How you split between stocks, bonds, cash, and other assets determines most of your outcome.

### Diversification: Reduce Risk Without Guessing
Diversification means spreading your investments across different assets.

The goal: reduce unsystematic risk without needing to time the market.

Unsystematic risk = company-specific risk (example: one stock crashing).

Diversification doesn't eliminate all risk—but it prevents one bad decision from wrecking your portfolio.

### Position Sizing: Protect Yourself From Being Wrong
You will be wrong sometimes. Everyone is.

The question is: how much does it hurt?

Position sizing limits the damage from any single incorrect thesis.

Example:
- 50% in one stock → dangerous
- 5% in one stock → manageable

This is one of the simplest—and most powerful—risk controls.

### Risk-Adjusted Return: Smarter Than Just "Return"
Not all returns are equal.

If two portfolios both return 8%:
- One is stable
- One swings wildly

Which is better? The stable one.

That's risk-adjusted return: it compares return relative to the risk taken to achieve it.

### Costs Matter More Than You Think
One of the easiest mistakes to overlook: fees.

With ETFs and funds, this shows up as an expense ratio.

Lower recurring costs can materially improve long-run compounding.

Small percentages don't look like much—but over time, they compound against you.

### Rebalancing: Staying on Track
Over time, your portfolio drifts.
- Winners grow
- Losers shrink

Rebalancing means adjusting back to your target weights.

It restores target allocation and enforces buy-low / sell-high behavior.

Without rebalancing, your portfolio slowly becomes something you didn't intend.

### Valuation Discipline: Don't Overpay
Great companies are not always great investments—if you pay too much.

Valuation discipline helps you avoid overpaying for growth expectations.

This is where a lot of investors get into trouble:
- Chasing hot stocks
- Buying after big runs

Price matters.

### Behavioral Bias: Your Biggest Risk
Markets don't just move—people react.

One of the most common mistakes: recency bias.
- Assuming recent performance will continue
- Chasing what just went up

Recency bias leads to performance-chasing decisions.

This is how investors end up buying high and selling low.

### Drawdown Management: The Math of Losing
A key concept most people miss: losses hurt more than gains help.

Down 50% → need +100% to recover.

That's why smaller drawdowns improve your probability of recovery.

Managing downside risk is just as important as generating returns.

### Connecting This to Your Simulator
As you start investing in the simulator, think in this framework:
- What is my asset allocation?
- Am I actually diversified?
- Are my position sizes reasonable?
- What risks am I taking?
- Am I controlling costs and execution?

You're not just placing trades.

You're building a portfolio using a process.

### Bottom Line (Memorize This Section)
If you understand these ideas, you will do well on the quiz—and more importantly, as an investor:
- Diversification reduces unnecessary risk
- Asset allocation drives long-term outcomes
- Position sizing controls damage when wrong
- Risk-adjusted return matters more than raw return
- Costs compound (and hurt if ignored)
- Rebalancing enforces discipline
- Liquidity affects execution
- Behavior often matters more than knowledge
- Managing drawdowns is critical
"""


def _parse_iso_date(value, field_name):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid {field_name}. Expected YYYY-MM-DD format.")


def _first_present(data, *keys, default=None):
    for key in keys:
        if key in data and data.get(key) is not None:
            return data.get(key)
    return default


def _validate_curriculum_window(total_weeks, start_date, end_date):
    if total_weeks is None:
        raise ValueError("curriculumWeeks is required when curriculumEnabled is true.")
    try:
        total_weeks = int(total_weeks)
    except (TypeError, ValueError):
        raise ValueError("curriculumWeeks must be an integer.")
    if total_weeks < 1:
        raise ValueError("curriculumWeeks must be at least 1.")
    if not start_date or not end_date:
        raise ValueError("curriculumStartDate and curriculumEndDate are required when curriculumEnabled is true.")
    if end_date < start_date:
        raise ValueError("curriculumEndDate cannot be before curriculumStartDate.")
    return total_weeks


def _build_curriculum_topics(total_weeks):
    if total_weeks <= len(BASE_CURRICULUM_TOPICS):
        topics = []
        base_count = len(BASE_CURRICULUM_TOPICS)
        for i in range(total_weeks):
            start_idx = int(i * base_count / total_weeks)
            end_idx = int((i + 1) * base_count / total_weeks) - 1
            if end_idx < start_idx:
                end_idx = start_idx
            slice_topics = BASE_CURRICULUM_TOPICS[start_idx:end_idx + 1]
            title = " + ".join([t[0] for t in slice_topics[:2]]) if len(slice_topics) > 1 else slice_topics[0][0]
            description = " ".join([t[1] for t in slice_topics])
            topics.append((title, description))
        return topics

    topics = list(BASE_CURRICULUM_TOPICS)
    advanced_topics = [
        ("Sector Rotation and Relative Strength", "Comparing sectors, cycle positioning, and tactical tilts."),
        ("Earnings and Event-Driven Strategy", "Interpreting earnings quality and event risk management."),
        ("Risk Management Playbook", "Stops, hedging concepts, and downside scenario planning."),
        ("Factor Investing and Style Premia", "Value, quality, momentum, and multi-factor blends."),
        ("Case Study Portfolio Lab", "Structured case analysis and thesis defense with evidence."),
        ("Strategy Review and Reflection", "Performance attribution, mistakes log, and process upgrades."),
    ]
    for i in range(total_weeks - len(BASE_CURRICULUM_TOPICS)):
        topics.append(advanced_topics[i % len(advanced_topics)])
    return topics


def _build_module_schedule(total_weeks, start_date, end_date):
    total_seconds = max((end_date - start_date).total_seconds(), 0)
    module_span = total_seconds / total_weeks if total_weeks else 0
    schedule = []
    for idx in range(total_weeks):
        unlock = start_date + timedelta(seconds=module_span * idx)
        due = start_date + timedelta(seconds=module_span * (idx + 1))
        if idx == total_weeks - 1 or due > end_date:
            due = end_date
        if due < unlock:
            due = unlock
        schedule.append((unlock, due))
    return schedule


def _module_teaching_plan(module_title, module_description):
    lower_title = module_title.lower()

    plan = {
        "hook": "Investing improves when you replace hot takes with a repeatable process that survives different market regimes.",
        "quiz_focus": [
            "Applying core terms to realistic portfolio decisions",
            "Choosing the best action under risk, cost, and diversification tradeoffs",
            "Interpreting basic return/allocation math in plain language",
            "Diagnosing weak decision process vs strong decision process",
        ],
        "core_terms": [
            ("Expected return", "A probability-weighted estimate, not a guarantee."),
            ("Risk", "The range and severity of outcomes, especially downside."),
            ("Process", "A checklist-driven decision workflow that is repeatable under pressure."),
        ],
        "likely_confusion": "Students often grade themselves by outcome instead of process quality.",
        "scenario": "After a strong week, you feel pressure to add risk. The disciplined move is to re-check thesis strength, valuation context, position limits, and downside scenario before placing a trade.",
        "walkthrough": [
            "State the thesis in one sentence with a clear time horizon.",
            "List 2-3 concrete data points that support the thesis.",
            "Define risk controls: sizing, invalidation trigger, and portfolio impact.",
            "Document what evidence would make you reduce, exit, or add.",
        ],
        "deep_dive_paragraphs": [
            "Expected return is a forward-looking, probability-weighted estimate—it is not a forecast of what will happen and it is certainly not last month's return. Good investors build a mental distribution of outcomes (base case, upside, downside) and compare the prize with the penalty. If the downside scenario is large enough to force a bad decision under pressure, the position is too big even if the base case looks attractive.",
            "Risk is more than volatility; it is the full range and severity of outcomes you must live with. Drawdowns, liquidity gaps, factor crowding, and behavioral response all count. The working definition for this course: risk is anything that can force you off your plan before the thesis has time to play out. Strong risk controls are planned in advance, written down, and mechanical—so emotion cannot overwrite them when markets are loud.",
            "Process is the system that turns those ideas into repeatable decisions. A usable process names the thesis, lists the evidence required, picks a position size connected to downside, and defines the invalidation point before the trade is placed. Process is what makes a track record interpretable: you can tell whether a win came from edge or from luck, and whether a loss was a bad outcome or a bad decision.",
        ],
        "worked_example": (
            "A student opens a starter position at 4% of portfolio in a quality compounder with a 12-month thesis, requires three out of four evidence checks (operating margin trend, free cash flow conversion, insider behavior, relative strength) to add, and pre-commits to trimming to 2% on a break of the 200-day trend with no fundamental offset. When the stock drops 9% on a peer guide-down, the pre-written plan says 'hold, because fundamentals did not change'; only a break of the trend AND a fundamental shock trips the trim rule. The student writes the outcome in their trade log with the rule applied, not their feelings."
        ),
        "assignment_q1": [
            "Select one real trade from this module and evaluate decision quality (high/medium/low) using at least four eText criteria: thesis clarity, evidence quality, sizing discipline, and risk controls.",
            "Identify the single most material risk in that trade (market, sector, company, valuation, liquidity, or behavioral). Explain whether your controls were pre-planned, reactive, or missing—and the consequence of that choice.",
            "Rewrite the trade as a higher-quality plan you would execute now: include updated thesis, position size, invalidation rule, and one trigger for scaling up/down.",
        ],
        "assignment_q2": [
            "Compute your portfolio return for a clearly defined window and compare it to a relevant benchmark ETF. Show formula, inputs, and final excess return.",
            "Break the portfolio into at least four sleeves (e.g., core beta, thematic tilt, single-name ideas, cash/defensive). Calculate each sleeve weight and evaluate concentration risk.",
            "Estimate contribution to return from your top two positions and explain whether performance came mostly from thesis edge, factor exposure, or concentration luck.",
        ],
        "application_prompt": "State one action you will apply in your next simulator decision and why it should improve process quality.",
        "quiz_bank": None,
    }

    if "introduction to investing" in lower_title or "markets" in lower_title:
        plan.update({
            "hook": "Most new investors over-focus on ticker selection. Real progress starts with objective, horizon, and implementation discipline.",
            "core_terms": [
                ("Primary vs secondary market", "Primary issues raise capital; secondary markets transfer ownership."),
                ("Asset class", "A bucket of exposures with similar risk/return behavior."),
                ("Compounding", "Returns generating additional returns over long horizons."),
            ],
            "likely_confusion": "Students confuse activity with progress; more trades is not automatically better investing.",
            "scenario": "A student rotates through trending names and trails a broad ETF. Your task is to diagnose why process failed.",
            "deep_dive_paragraphs": [
                "Markets exist to price claims on future cash flows. In primary markets, companies and governments raise capital by issuing new shares or bonds. In secondary markets—where almost all simulator activity happens—existing claims change hands. Understanding the distinction matters: secondary-market price changes transfer ownership but do not move capital to the underlying business. A stock rising 10% in your portfolio does not feed the company's balance sheet.",
                "Asset classes group securities by shared risk/return behavior: public equity for growth and long-term compounding, investment-grade fixed income for capital preservation and income, cash equivalents for liquidity, and alternatives (real estate, commodities, private assets) for additional diversification. New investors frequently pick individual names before deciding what share of capital each asset class should hold. Reversing that order—allocation first, security selection second—removes most of the avoidable mistakes.",
                "Compounding is the slow and unglamorous core of long-horizon investing. A 7% real annual return doubles purchasing power roughly every ten years; a 9% real return doubles it roughly every eight. The gap between those two paths is an enormous terminal-wealth difference created almost entirely by discipline—saving consistently, keeping costs low, staying invested through drawdowns, and avoiding large behavioral errors during stress.",
            ],
            "worked_example": (
                "Two simulator accounts start with $100,000. Account A buys SPY and holds. Account B trades 4–6 single names per week, chasing short-term news. After three months, Account A tracks the index with trivial friction; Account B lags by 280 bps after commissions, taxes, and two concentrated losses. The difference is not IQ; it is process: Account A accepted market-beta as a baseline and protected it from turnover, while Account B paid ongoing costs in exchange for an inconsistent edge.",
            ),
            "quiz_bank": [
                ("Which statement best describes the primary vs secondary market distinction?", [
                    "Primary markets raise new capital for issuers; secondary markets transfer existing claims between investors.",
                    "Primary markets only trade government bonds; secondary markets only trade stocks.",
                    "Primary markets are weekdays and secondary markets are weekends.",
                    "There is no operational difference between them.",
                ], "Primary markets raise new capital for issuers; secondary markets transfer existing claims between investors."),
                ("Why is allocation typically decided before security selection?", [
                    "Because asset-class mix controls most of the risk/return profile over long horizons.",
                    "Because individual stocks are always riskier than indexes.",
                    "Because only allocation affects taxes.",
                    "Because security selection is banned in retirement accounts.",
                ], "Because asset-class mix controls most of the risk/return profile over long horizons."),
                ("Compounding is most accurately described as:", [
                    "Returns earned on prior returns, amplifying growth over long horizons.",
                    "A guaranteed fixed return paid annually by the exchange.",
                    "The fee brokers charge for holding positions overnight.",
                    "Only relevant inside tax-advantaged accounts.",
                ], "Returns earned on prior returns, amplifying growth over long horizons."),
                ("Which action most directly reduces avoidable cost drag on a long-horizon portfolio?", [
                    "Lowering turnover and preferring low-expense-ratio core vehicles.",
                    "Increasing turnover to capture more idiosyncratic moves.",
                    "Using only leveraged ETFs for core exposure.",
                    "Rebalancing daily with market orders.",
                ], "Lowering turnover and preferring low-expense-ratio core vehicles."),
                ("A student buys AAPL at $190 and it rises to $210. Which statement is correct about that gain?", [
                    "The gain is unrealized until sold and does not transfer cash to Apple's balance sheet.",
                    "The $20 moves directly from Apple's treasury to the student.",
                    "The gain is realized automatically at market close.",
                    "Apple must pay the student a dividend equal to the gain.",
                ], "The gain is unrealized until sold and does not transfer cash to Apple's balance sheet."),
                ("Which asset class is typically held mainly for liquidity rather than growth?", [
                    "Cash and short-duration Treasuries (e.g., BIL).",
                    "Small-cap emerging-market equity.",
                    "Long-duration growth equity.",
                    "Commodities futures.",
                ], "Cash and short-duration Treasuries (e.g., BIL)."),
                ("An investor has a 20-year horizon and stable income. What is the most common early mistake?", [
                    "Holding too much cash and missing long-horizon compounding.",
                    "Holding too many long-duration Treasuries.",
                    "Owning broad equity index funds.",
                    "Rebalancing once per year.",
                ], "Holding too much cash and missing long-horizon compounding."),
                ("Which best describes 'time in the market' versus 'timing the market' for long-horizon investors?", [
                    "Time in the market generally dominates because missing a few best days materially lowers terminal wealth.",
                    "Timing the market is easy because trends are always obvious.",
                    "Neither matters if you pick any index fund.",
                    "Timing dominates because transaction costs are zero.",
                ], "Time in the market generally dominates because missing a few best days materially lowers terminal wealth."),
                ("A diversified equity ETF is 'tax efficient' primarily because:", [
                    "Its low turnover limits realized capital-gain distributions.",
                    "It pays no dividends by law.",
                    "It is exempt from federal tax.",
                    "It uses only options to generate returns.",
                ], "Its low turnover limits realized capital-gain distributions."),
                ("Which is the strongest argument for starting with a broad index core?", [
                    "It produces market-beta at low cost and avoids single-name selection risk early.",
                    "It guarantees outperformance of any single stock.",
                    "It eliminates all market risk.",
                    "It requires no rebalancing ever.",
                ], "It produces market-beta at low cost and avoids single-name selection risk early."),
                ("If a new investor opens 12 positions of 1-2% each across one theme, the portfolio is best described as:", [
                    "Nominally diversified but concentrated in a single risk factor.",
                    "Fully diversified because of the position count.",
                    "Low-risk because each position is small.",
                    "Hedged because it holds multiple tickers.",
                ], "Nominally diversified but concentrated in a single risk factor."),
                ("'Investing without a written plan' is risky mainly because:", [
                    "Emotion overwrites decisions in volatile conditions when no pre-committed rules exist.",
                    "Brokers charge extra for unplanned orders.",
                    "Market makers reject orders without a plan.",
                    "The SEC requires a written plan.",
                ], "Emotion overwrites decisions in volatile conditions when no pre-committed rules exist."),
                ("Bid-ask spread is best defined as:", [
                    "The difference between the highest price buyers will pay and the lowest price sellers will accept.",
                    "The broker's annual fee.",
                    "The gap between open and close price.",
                    "The spread between dividend and yield.",
                ], "The difference between the highest price buyers will pay and the lowest price sellers will accept."),
                ("Which statement best characterizes 'process over prediction' in this course?", [
                    "Consistent application of rules under uncertainty beats confident one-off forecasts.",
                    "Forecasts are irrelevant in all markets.",
                    "Rules should be rewritten after every losing trade.",
                    "Only short-term traders benefit from process.",
                ], "Consistent application of rules under uncertainty beats confident one-off forecasts."),
                ("A 'position size' of 5% of portfolio on a stock with a 20% expected drawdown produces roughly what portfolio-level loss in that drawdown?", [
                    "About 1% of total portfolio value.",
                    "About 5% of total portfolio value.",
                    "About 20% of total portfolio value.",
                    "Zero loss because diversification eliminates it.",
                ], "About 1% of total portfolio value."),
                ("Which is true about fees and compounding over long horizons?", [
                    "Small recurring fee differences compound into large terminal-wealth gaps.",
                    "Fees are irrelevant to long-horizon returns.",
                    "Higher fees guarantee better performance.",
                    "Fees only affect tax-deferred accounts.",
                ], "Small recurring fee differences compound into large terminal-wealth gaps."),
                ("A student complains their ETF 'did nothing this week' and wants to trade. The strongest eText response is:", [
                    "Action is not edge; absence of a plan-triggered signal means no trade is required.",
                    "Trade immediately to feel engaged.",
                    "Double position size to force a result.",
                    "Switch to a leveraged ETF.",
                ], "Action is not edge; absence of a plan-triggered signal means no trade is required."),
                ("Which investor behavior best protects long-horizon returns during a 20%+ drawdown?", [
                    "Following a pre-written rebalancing and rule-based allocation policy rather than news-reactive selling.",
                    "Selling all equity exposure on the first red week.",
                    "Switching to 100% concentrated single names for catch-up.",
                    "Ignoring the portfolio entirely for a decade.",
                ], "Following a pre-written rebalancing and rule-based allocation policy rather than news-reactive selling."),
                ("A 'thesis' in this course is best described as:", [
                    "A testable one-sentence claim about why the investment should work, with a horizon and evidence.",
                    "A price target alone.",
                    "A recommendation from a popular news source.",
                    "A promise that the position cannot lose money.",
                ], "A testable one-sentence claim about why the investment should work, with a horizon and evidence."),
                ("Why is documenting your horizon up front important?", [
                    "Because volatility only matters relative to the horizon over which you must hold.",
                    "Because horizon is regulated by the exchange.",
                    "Because horizon determines tax rate.",
                    "Because horizon replaces the need for diversification.",
                ], "Because volatility only matters relative to the horizon over which you must hold."),
            ],
        })
    elif "risk and return" in lower_title:
        plan.update({
            "hook": "Return is attractive, but risk determines whether your process is durable.",
            "core_terms": [
                ("Volatility", "The variability of returns around average, often measured by standard deviation."),
                ("Drawdown", "Peak-to-trough decline that tests both capital and psychology."),
                ("Risk-adjusted return", "Return evaluated relative to the risk required to earn it (e.g., Sharpe)."),
            ],
            "likely_confusion": "Strong trailing returns are often mistaken for low future risk after extended rallies.",
            "scenario": "Two portfolios both return 10%, but one suffered a -28% drawdown and one only -8%. Decide which process is stronger and why.",
            "deep_dive_paragraphs": [
                "Volatility measures how much returns fluctuate around their average. Higher volatility means wider possible outcomes in both directions, but it is not the same as risk. Risk is about what you are forced to do when outcomes land in the left tail: sell at the worst possible time, abandon the plan, or size down permanently. Two assets with the same average return can feel completely different to hold if one volatilizes twice as much.",
                "Drawdowns are the single most important statistic students ignore. A -50% drawdown requires a +100% gain to recover; a -20% drawdown requires only +25%. A portfolio designed to recover from a -20% event is structurally different from one that tolerates -50%. Sizing, diversification, and leverage decisions should all trace back to the drawdown you can endure without changing behavior.",
                "Risk-adjusted return metrics (Sharpe, Sortino, Calmar) evaluate return per unit of risk. A portfolio returning 10% with 8% volatility is usually more defensible than one returning 13% with 30% volatility, because the first is easier to stick with and leverage responsibly. The goal is not max return; it is max return you can actually achieve given your behavior under stress.",
            ],
            "worked_example": (
                "Portfolio A: +10% return, 12% annualized volatility, max drawdown -8%. Portfolio B: +12% return, 28% annualized volatility, max drawdown -34%. Sharpe (rf=2%) is ~0.67 for A and ~0.36 for B. A real investor is twice as likely to stay in A through the full cycle—meaning the better risk-adjusted process is more likely to compound. Picking B on headline return is the classic rookie error."
            ),
            "quiz_bank": [
                ("Volatility is best defined as:", [
                    "The variability of returns around their mean.",
                    "The average return earned over a year.",
                    "The dividend yield of the portfolio.",
                    "The number of trades placed per week.",
                ], "The variability of returns around their mean."),
                ("A portfolio drops -25% from its peak. To fully recover, it must gain:", [
                    "About 33%.",
                    "About 25%.",
                    "About 15%.",
                    "Exactly 25%.",
                ], "About 33%."),
                ("Risk in this course is most accurately described as:", [
                    "The range and severity of outcomes, especially on the downside, that can force you off your plan.",
                    "Only the standard deviation of returns.",
                    "The likelihood of making money.",
                    "Whatever the news says is scary this week.",
                ], "The range and severity of outcomes, especially on the downside, that can force you off your plan."),
                ("Two portfolios both return 10%. Portfolio X had max drawdown -8%, Portfolio Y had -28%. Which statement is best supported?", [
                    "X likely has a stronger risk-adjusted process and is easier to stay invested in.",
                    "Y is clearly better because drawdown is irrelevant.",
                    "They are equivalent because returns match.",
                    "X must be lower risk-adjusted because it had less volatility.",
                ], "X likely has a stronger risk-adjusted process and is easier to stay invested in."),
                ("Sharpe ratio primarily measures:", [
                    "Excess return earned per unit of volatility.",
                    "Total nominal return.",
                    "Total turnover.",
                    "Dividend yield.",
                ], "Excess return earned per unit of volatility."),
                ("Why is a 'tolerable drawdown' set before entering a position?", [
                    "So position sizing and invalidation rules match the behavioral capacity of the investor.",
                    "Because exchanges require it.",
                    "To guarantee no loss.",
                    "To remove the need for diversification.",
                ], "So position sizing and invalidation rules match the behavioral capacity of the investor."),
                ("If an asset has 20% annual volatility and a 10% expected return, a one-standard-deviation range of one-year outcomes is roughly:", [
                    "-10% to +30%.",
                    "+10% only.",
                    "-20% to +20%.",
                    "-40% to +60%.",
                ], "-10% to +30%."),
                ("Which pair is most likely to be confused but is not the same?", [
                    "Low trailing volatility and low forward risk after a long rally.",
                    "Bid price and ask price.",
                    "Cash and margin.",
                    "Market order and limit order.",
                ], "Low trailing volatility and low forward risk after a long rally."),
                ("Leverage tends to do which of the following to drawdowns?", [
                    "Amplifies them beyond the unlevered exposure.",
                    "Eliminates them.",
                    "Caps them at -10%.",
                    "Converts them into gains.",
                ], "Amplifies them beyond the unlevered exposure."),
                ("Which statement about correlation during crises is most accurate?", [
                    "Correlations between risk assets tend to rise sharply in crises, reducing diversification benefit.",
                    "Correlations fall to zero in crises.",
                    "Correlations become irrelevant in crises.",
                    "Correlations invert permanently after a crisis.",
                ], "Correlations between risk assets tend to rise sharply in crises, reducing diversification benefit."),
                ("A strategy with 15% return, 35% volatility, and 50% max drawdown is weaker than one with 11% return, 10% volatility, and 14% drawdown mainly because:", [
                    "The first strategy's risk profile is much harder to hold and is likely to cause plan-breaking behavior.",
                    "Higher return is always better.",
                    "Drawdown is irrelevant to investors.",
                    "The first strategy must have lower fees.",
                ], "The first strategy's risk profile is much harder to hold and is likely to cause plan-breaking behavior."),
                ("Sortino ratio differs from Sharpe because it:", [
                    "Penalizes only downside volatility rather than total volatility.",
                    "Uses turnover instead of returns.",
                    "Ignores returns entirely.",
                    "Uses Beta instead of volatility.",
                ], "Penalizes only downside volatility rather than total volatility."),
                ("A 'risk-free rate' in this course most often refers to:", [
                    "Short-duration government bill yields used as a baseline.",
                    "The best single-stock return last year.",
                    "The average dividend yield of the S&P 500.",
                    "Zero under all conditions.",
                ], "Short-duration government bill yields used as a baseline."),
                ("Why do behavioral biases hurt risk-adjusted returns more than nominal returns?", [
                    "Because panic selling in drawdowns converts temporary losses into permanent ones.",
                    "Because biases only affect dividend reinvestment.",
                    "Because biases increase volatility but not losses.",
                    "Because biases only matter in bull markets.",
                ], "Because panic selling in drawdowns converts temporary losses into permanent ones."),
                ("The most durable way to reduce drawdown risk is typically:", [
                    "Lower position sizing on the most volatile holdings and diversify across uncorrelated return drivers.",
                    "Concentrate into last year's best performer.",
                    "Use maximum leverage on high-conviction ideas.",
                    "Ignore correlation.",
                ], "Lower position sizing on the most volatile holdings and diversify across uncorrelated return drivers."),
                ("Volatility drag means that:", [
                    "Arithmetic average returns overstate compounded (geometric) returns when volatility is high.",
                    "Volatility increases compounded returns.",
                    "Volatility is the same as the average return.",
                    "Drag only affects bond portfolios.",
                ], "Arithmetic average returns overstate compounded (geometric) returns when volatility is high."),
                ("If a stock has a daily return with mean 0.05% and std 2%, the one-day 1-in-6 (approx) downside move is roughly:", [
                    "About -2%.",
                    "About -20%.",
                    "About -0.05%.",
                    "Zero because expected return is positive.",
                ], "About -2%."),
                ("A 'tail risk' is best described as:", [
                    "Low-probability, high-magnitude outcomes whose impact dominates averages.",
                    "The last trade of the day.",
                    "The last position in a portfolio.",
                    "The dividend paid in the fourth quarter.",
                ], "Low-probability, high-magnitude outcomes whose impact dominates averages."),
                ("Which action reflects strong risk management after a 15% drawdown?", [
                    "Re-check thesis, rebalance to target weights, and execute a pre-committed plan without sizing up impulsively.",
                    "Double every position to recover faster.",
                    "Sell all equity and hold cash permanently.",
                    "Switch to untested strategies.",
                ], "Re-check thesis, rebalance to target weights, and execute a pre-committed plan without sizing up impulsively."),
                ("Risk-adjusted return matters more than absolute return for long-horizon investors mainly because:", [
                    "It measures the return you can realistically earn while sticking to the plan.",
                    "Brokers charge fees only on risk-adjusted returns.",
                    "The IRS taxes only risk-adjusted returns.",
                    "It is easier to compute.",
                ], "It measures the return you can realistically earn while sticking to the plan."),
            ],
        })
    elif "diversification" in lower_title:
        plan.update({
            "hook": "Diversification is about different risk behaviors, not just more ticker symbols.",
            "core_terms": [
                ("Correlation", "How assets move relative to one another; ranges from -1 to +1."),
                ("Idiosyncratic risk", "Single-company risk that can be diversified away."),
                ("Concentration", "Overdependence on one position, sector, or factor."),
            ],
            "likely_confusion": "Owning many names in one theme is still concentrated risk.",
            "scenario": "Your portfolio holds eight names, but most risk is concentrated in one growth factor exposure.",
            "deep_dive_paragraphs": [
                "Diversification reduces the variance of outcomes when the added holdings have different risk drivers. If your portfolio already holds MSFT and you add NVDA, AMD, and AVGO, you are adding names but not diversifying—you are still almost entirely exposed to one factor (large-cap US semiconductors and AI capex). Real diversification comes from adding exposures that behave differently in the environments that hurt your existing holdings.",
                "Correlation ranges from -1 (always moves opposite) to +1 (always moves together). Two assets at +0.95 correlation offer almost no diversification benefit; two assets at +0.2 can meaningfully reduce portfolio volatility without sacrificing expected return. But correlations are unstable, especially in crises, where most risk assets rise toward +1.0 together. Planning for that 'crisis correlation' is more important than optimizing to historical averages.",
                "Idiosyncratic (single-name) risk decays quickly with diversification—adding even 10–20 uncorrelated positions removes most of it. Systematic (market) risk does not diversify away at the equity level. The practical implication: you do not need 50 stocks, but you do need exposures that do not all depend on the same macro factor to work.",
            ],
            "worked_example": (
                "A student holds 6 stocks: AAPL, MSFT, GOOGL, AMZN, META, NVDA at ~15% weights each. Pairwise correlations average ~0.75. Portfolio 'effective' holdings (via correlation math) are closer to 2.5 independent bets, not 6. Swapping two positions for an energy holding (XOM) and a long-duration Treasury (IEF) drops average correlation to ~0.45 and meaningfully lowers drawdown exposure without hurting expected return materially."
            ),
            "quiz_bank": [
                ("Diversification is most accurately described as:", [
                    "Combining holdings whose return drivers differ so portfolio variance is lower than the weighted average of parts.",
                    "Owning the maximum possible number of tickers.",
                    "Buying one stock from every sector in the S&P 500.",
                    "Holding equal dollar amounts of every position.",
                ], "Combining holdings whose return drivers differ so portfolio variance is lower than the weighted average of parts."),
                ("Correlation of +1.0 between two assets means:", [
                    "They move together in lockstep, offering no diversification benefit.",
                    "They always move in opposite directions.",
                    "They are guaranteed to return the same amount annually.",
                    "They have no relationship.",
                ], "They move together in lockstep, offering no diversification benefit."),
                ("Idiosyncratic risk is:", [
                    "Single-name or single-company risk that can be diversified away with enough uncorrelated holdings.",
                    "Identical to systematic market risk.",
                    "Risk that compounds over time automatically.",
                    "Risk from currency hedging only.",
                ], "Single-name or single-company risk that can be diversified away with enough uncorrelated holdings."),
                ("Owning eight semiconductor stocks instead of one is best described as:", [
                    "Diversifying idiosyncratic single-name risk but remaining concentrated in one sector/factor.",
                    "Fully diversifying both single-name and sector risk.",
                    "Equivalent to owning a broad-market ETF.",
                    "Eliminating all systemic risk.",
                ], "Diversifying idiosyncratic single-name risk but remaining concentrated in one sector/factor."),
                ("'Crisis correlation' describes:", [
                    "The tendency for risk-asset correlations to rise toward +1 during market stress, reducing diversification.",
                    "The correlation between cash and gold.",
                    "The correlation between the VIX and price.",
                    "A permanent new correlation regime.",
                ], "The tendency for risk-asset correlations to rise toward +1 during market stress, reducing diversification."),
                ("Roughly how many uncorrelated single-stock positions capture most of the available idiosyncratic-risk reduction?", [
                    "10–20.",
                    "Exactly 3.",
                    "At least 500.",
                    "None; diversification does not reduce idiosyncratic risk.",
                ], "10–20."),
                ("A student owns SPY, QQQ, and IVV. This is an example of:", [
                    "Overlapping exposures—the three funds largely hold the same underlying companies.",
                    "Global diversification across continents.",
                    "Adding defensive bond exposure.",
                    "Hedged cash.",
                ], "Overlapping exposures—the three funds largely hold the same underlying companies."),
                ("Adding long-duration Treasuries to an all-equity portfolio generally:", [
                    "Reduces portfolio volatility in most environments because of lower equity correlation.",
                    "Increases expected return materially.",
                    "Guarantees no drawdown in any environment.",
                    "Has no effect on volatility.",
                ], "Reduces portfolio volatility in most environments because of lower equity correlation."),
                ("Which statement about concentration risk is most accurate?", [
                    "A single position above roughly 10–20% of portfolio substantially raises single-event risk.",
                    "Concentration risk only applies to holdings above 50%.",
                    "Concentration is never a problem if the position is profitable.",
                    "Concentration is only relevant to bonds.",
                ], "A single position above roughly 10–20% of portfolio substantially raises single-event risk."),
                ("Systematic risk is best described as:", [
                    "Market-wide risk that cannot be diversified away by adding more stocks.",
                    "Risk from a single company's quarterly earnings.",
                    "Risk caused by trading errors.",
                    "Risk specific to one portfolio manager.",
                ], "Market-wide risk that cannot be diversified away by adding more stocks."),
                ("Why can correlations be unstable across regimes?", [
                    "Return drivers (rates, liquidity, sentiment) change over cycles and shift how assets co-move.",
                    "Exchanges reset correlations annually.",
                    "Correlations are always stable.",
                    "Only volatility is unstable; correlation is constant.",
                ], "Return drivers (rates, liquidity, sentiment) change over cycles and shift how assets co-move."),
                ("A portfolio of AAPL, MSFT, and GOOGL has average pairwise correlation of about 0.8. The portfolio is best described as:", [
                    "Nominally diversified but heavily dependent on a common mega-cap US tech factor.",
                    "Fully diversified because it has three names.",
                    "International and factor-neutral.",
                    "Low-risk by construction.",
                ], "Nominally diversified but heavily dependent on a common mega-cap US tech factor."),
                ("Which addition offers the strongest diversification to a US-large-cap-growth-heavy book?", [
                    "Global value or short-duration bonds whose drivers differ from US large-cap growth.",
                    "Another US large-cap growth ETF.",
                    "A leveraged growth ETF.",
                    "Additional single-name US mega-caps.",
                ], "Global value or short-duration bonds whose drivers differ from US large-cap growth."),
                ("'Position weight' is best defined as:", [
                    "Position market value divided by total portfolio value.",
                    "Number of shares held.",
                    "Dividend yield of the position.",
                    "Beta of the position.",
                ], "Position market value divided by total portfolio value."),
                ("Top-3 concentration (sum of top three weights) of 60% implies:", [
                    "Portfolio outcomes are heavily driven by three positions, concentrating idiosyncratic risk.",
                    "The portfolio is well diversified.",
                    "The portfolio is index-tracking.",
                    "Concentration is low.",
                ], "Portfolio outcomes are heavily driven by three positions, concentrating idiosyncratic risk."),
                ("Which pair most likely has low long-run correlation useful for diversification?", [
                    "Broad equity and investment-grade long-duration Treasuries.",
                    "SPY and IVV.",
                    "QQQ and XLK.",
                    "VOO and VTI.",
                ], "Broad equity and investment-grade long-duration Treasuries."),
                ("'Naive diversification' error most commonly means:", [
                    "Adding positions without checking that their return drivers actually differ from existing holdings.",
                    "Using equal-weighted portfolios.",
                    "Holding too few asset classes.",
                    "Refusing to hold bonds.",
                ], "Adding positions without checking that their return drivers actually differ from existing holdings."),
                ("Which of the following is NOT a typical diversification lever?", [
                    "Taking full margin leverage on a single high-conviction idea.",
                    "Adding uncorrelated asset classes.",
                    "Spreading sector exposure.",
                    "Rebalancing to target weights.",
                ], "Taking full margin leverage on a single high-conviction idea."),
                ("Why are equal-weighted portfolios often more diversified than cap-weighted ones?", [
                    "Because cap-weighting concentrates risk in the largest names as they grow.",
                    "Because equal-weighted portfolios are tax-free.",
                    "Because they pay higher dividends.",
                    "Because they have zero turnover.",
                ], "Because cap-weighting concentrates risk in the largest names as they grow."),
                ("If your portfolio's return is explained mostly by one factor (e.g., US growth), which is the best diagnostic step?", [
                    "Run a factor decomposition and add exposure with a different primary driver.",
                    "Add more of the same factor to double down.",
                    "Reduce total equity to zero and hold cash.",
                    "Ignore factor analysis—only individual names matter.",
                ], "Run a factor decomposition and add exposure with a different primary driver."),
            ],
        })
    elif "asset allocation" in lower_title:
        plan.update({
            "hook": "Allocation is your portfolio operating system; security selection runs on top of it.",
            "core_terms": [
                ("Strategic allocation", "Long-term target mix linked to goals and risk tolerance."),
                ("Tactical tilt", "Temporary deviation based on valuation, momentum, or macro evidence."),
                ("Rebalancing band", "A pre-defined threshold for restoring target weights."),
            ],
            "likely_confusion": "Students often change allocation because of headlines, not because objective or risk capacity changed.",
            "scenario": "After a rally, equities rise from 60% to 74% of your mix. Decide whether to rebalance and defend the tradeoff.",
            "deep_dive_paragraphs": [
                "Strategic allocation is the long-term target mix that matches the investor's objectives, horizon, and behavioral capacity. Classic examples are 60/40 equity/bond, 80/20 growth-oriented, or risk-parity mixes that target equal risk contribution rather than equal dollars. Strategic allocation should change only when the underlying objective, horizon, or risk capacity changes—not in response to the last six months of performance.",
                "Tactical tilts temporarily over- or under-weight parts of the strategic mix when valuation, momentum, or macro conditions provide durable evidence. The discipline is that tilts are bounded (e.g., +/- 10% of target weight), time-bound, and thesis-bound: if the evidence weakens, the tilt comes off. Undisciplined tactical trading turns into drift into whatever is currently comfortable.",
                "Rebalancing is the mechanical restoration of target weights. Threshold rebalancing (e.g., when any sleeve drifts +/- 5% from target) typically outperforms calendar rebalancing because it only trades when the portfolio has actually drifted. Rebalancing also forces a contrarian behavior—selling what has appreciated, buying what has lagged—which improves compounded risk-adjusted returns over long horizons.",
            ],
            "worked_example": (
                "Target 60% equity / 30% bonds / 10% cash with +/- 5% bands. After a 20% equity rally with bonds flat, allocation drifts to 67% / 25% / 8%. Rebalancing trades ~7% from equity back to bonds/cash and restores target. A student who 'lets winners run' holds 67% equity into the next drawdown and suffers a larger-than-planned loss because the risk profile silently migrated."
            ),
            "quiz_bank": [
                ("Strategic allocation is best described as:", [
                    "The long-term target mix linked to objectives, horizon, and risk capacity.",
                    "The trade placed when the market gaps.",
                    "The weekly changes to holdings.",
                    "The sector tilts applied daily.",
                ], "The long-term target mix linked to objectives, horizon, and risk capacity."),
                ("A tactical tilt differs from strategic allocation because it is:", [
                    "Temporary, evidence-based, and bounded in size.",
                    "Permanent and unconstrained.",
                    "Required by the broker.",
                    "Identical to strategic allocation.",
                ], "Temporary, evidence-based, and bounded in size."),
                ("Rebalancing is most valuable because it:", [
                    "Restores the risk profile the investor actually chose, enforcing 'sell high, buy low' mechanically.",
                    "Maximizes turnover for tax purposes.",
                    "Guarantees higher returns.",
                    "Replaces the need for diversification.",
                ], "Restores the risk profile the investor actually chose, enforcing 'sell high, buy low' mechanically."),
                ("A portfolio with 60% equity drifts to 72% after a rally. The disciplined response is to:", [
                    "Rebalance back toward target unless the investor's objectives or risk capacity have changed.",
                    "Let it run indefinitely because winners should be held.",
                    "Sell 100% of equity immediately.",
                    "Switch to a single stock portfolio.",
                ], "Rebalance back toward target unless the investor's objectives or risk capacity have changed."),
                ("Which is the strongest justification for changing strategic allocation?", [
                    "A material change in objectives, horizon, or risk capacity.",
                    "A three-month period of equity strength.",
                    "A single earnings miss from a large holding.",
                    "A news cycle about interest rates.",
                ], "A material change in objectives, horizon, or risk capacity."),
                ("Calendar rebalancing means rebalancing:", [
                    "On a fixed schedule (e.g., quarterly) regardless of drift.",
                    "Only when a single stock changes price.",
                    "Never; calendar rebalancing is prohibited.",
                    "Only after presidential elections.",
                ], "On a fixed schedule (e.g., quarterly) regardless of drift."),
                ("Threshold rebalancing means rebalancing:", [
                    "Whenever any sleeve drifts outside a pre-defined band.",
                    "Only in leap years.",
                    "Only when return is positive.",
                    "On every trading day.",
                ], "Whenever any sleeve drifts outside a pre-defined band."),
                ("Which allocation best fits a long-horizon, high-risk-capacity investor?", [
                    "Higher equity weight with diversified bond/cash sleeves.",
                    "100% short-duration Treasuries.",
                    "100% single-stock concentrated.",
                    "Cash only.",
                ], "Higher equity weight with diversified bond/cash sleeves."),
                ("A risk-parity allocation targets:", [
                    "Equal risk contribution from each sleeve rather than equal dollars.",
                    "Equal dollar weights regardless of risk.",
                    "Only equities.",
                    "Only the highest-volatility asset.",
                ], "Equal risk contribution from each sleeve rather than equal dollars."),
                ("Which is the main behavioral risk of abandoning a strategic allocation during a drawdown?", [
                    "Locking in losses and missing the recovery that strategic allocation was designed to endure.",
                    "Paying too little in fees.",
                    "Over-diversifying.",
                    "Over-concentrating in bonds.",
                ], "Locking in losses and missing the recovery that strategic allocation was designed to endure."),
                ("Tactical tilt should come off when:", [
                    "The evidence that justified the tilt weakens or the bounded time window expires.",
                    "It becomes more profitable.",
                    "A news headline appears.",
                    "Never; tilts are permanent.",
                ], "The evidence that justified the tilt weakens or the bounded time window expires."),
                ("A 60/40 portfolio under 30-year horizon should mainly respond to which of the following?", [
                    "Changes in the investor's goals, horizon, or risk capacity.",
                    "Daily market moves.",
                    "Quarterly earnings of a single stock.",
                    "Weekly fund flows.",
                ], "Changes in the investor's goals, horizon, or risk capacity."),
                ("Which statement about 'buckets' allocation is most accurate?", [
                    "Segmenting capital by purpose/horizon (e.g., liquidity, growth, legacy) helps match each bucket's risk profile to its goal.",
                    "Bucketing always increases total return.",
                    "Bucketing eliminates need for rebalancing.",
                    "Bucketing is required by exchanges.",
                ], "Segmenting capital by purpose/horizon (e.g., liquidity, growth, legacy) helps match each bucket's risk profile to its goal."),
                ("A 'glidepath' strategy means:", [
                    "Gradually reducing equity exposure as the goal horizon approaches.",
                    "Increasing leverage over time.",
                    "Picking the highest-momentum stock each year.",
                    "Holding only one asset class.",
                ], "Gradually reducing equity exposure as the goal horizon approaches."),
                ("Why is 'rebalancing is a risk-control tool, not a return-enhancement tool' a useful framing?", [
                    "Because its primary job is restoring intended risk, with any return benefit secondary and inconsistent.",
                    "Because rebalancing always loses money.",
                    "Because rebalancing guarantees higher returns.",
                    "Because rebalancing removes all risk.",
                ], "Because its primary job is restoring intended risk, with any return benefit secondary and inconsistent."),
                ("Portfolio drift most often happens because:", [
                    "Different sleeves earn different returns, moving weights away from target.",
                    "Brokers randomly rebalance accounts.",
                    "Exchanges re-weight portfolios monthly.",
                    "It is mandated by regulation.",
                ], "Different sleeves earn different returns, moving weights away from target."),
                ("An investor says 'I'll rebalance when it feels right.' The biggest risk is:", [
                    "Emotion overwriting policy, which means the portfolio rebalances in the wrong direction under stress.",
                    "Rebalancing too frequently.",
                    "Paying too low taxes.",
                    "Diversifying too aggressively.",
                ], "Emotion overwriting policy, which means the portfolio rebalances in the wrong direction under stress."),
                ("Tax-aware rebalancing often uses which technique?", [
                    "Directing new contributions or dividends into under-weighted sleeves before selling appreciated lots.",
                    "Selling only the highest-gain lots.",
                    "Ignoring tax impact entirely.",
                    "Refusing to rebalance above 5% drift.",
                ], "Directing new contributions or dividends into under-weighted sleeves before selling appreciated lots."),
                ("A student with a 6-month horizon for a car purchase should allocate that capital mostly to:", [
                    "Cash or short-duration investment-grade instruments.",
                    "Small-cap equity.",
                    "Long-duration growth ETFs.",
                    "Concentrated single names.",
                ], "Cash or short-duration investment-grade instruments."),
                ("Which ingredient is most important when setting a strategic allocation?", [
                    "Matching risk capacity and behavior, not just historical returns.",
                    "Copying whatever a popular influencer holds.",
                    "Choosing the mix with the highest trailing return.",
                    "Picking allocations at random.",
                ], "Matching risk capacity and behavior, not just historical returns."),
            ],
        })
    elif "stocks, etfs, and funds" in lower_title:
        plan.update({
            "hook": "Implementation vehicle matters: the same idea can be expressed efficiently or expensively.",
            "core_terms": [
                ("Expense ratio", "Recurring annual cost that compounds against net returns."),
                ("Bid-ask spread", "Execution friction paid when entering or exiting positions."),
                ("Tracking difference", "Gap between fund return and benchmark return after costs/frictions."),
            ],
            "likely_confusion": "Low management fee alone does not guarantee low total implementation cost.",
            "scenario": "You want AI exposure: compare concentrated single-name exposure versus diversified ETF implementation.",
            "deep_dive_paragraphs": [
                "Individual stocks deliver full exposure to a specific company's cash flows, governance, and idiosyncratic events. That is a feature when you have an edge on a name and a bug when you don't. ETFs package many stocks into one ticker, trading intraday like equities but offering pooled diversification and typically lower fees than actively managed mutual funds. Mutual funds still dominate employer retirement plans and some active strategies, but their structure (end-of-day NAV execution, potential capital-gain distributions) is usually less efficient than ETFs for taxable accounts.",
                "Total cost of ownership goes beyond the expense ratio. It includes bid-ask spread on each trade, tracking difference versus benchmark, capital-gain distributions from portfolio turnover, and opportunity cost of cash drag inside the fund. Two 'low-cost' index ETFs can differ by 5–15 bps in realized total cost even with identical management fees. The lower-cost choice compounds ahead over decades.",
                "Vehicle selection should match the use case: broad beta is almost always best delivered by a large, low-cost, high-volume ETF; sector or thematic exposure often needs a narrower ETF; active strategies should be held in tax-advantaged wrappers when possible. Single-stock picks make sense when you have conviction-level evidence and an explicit sizing rule; otherwise, ETFs deliver the same exposure with lower variance.",
            ],
            "worked_example": (
                "A student wants 'AI exposure.' Option A: buy NVDA at 10% of portfolio. Option B: buy SMH (semi ETF) at 10% of portfolio. Option C: buy QQQ at 10% of portfolio. NVDA gives concentrated single-name exposure (high variance, earnings risk); SMH gives diversified semi exposure (moderate variance); QQQ gives broad large-cap growth including AI names (lower variance). Without firm-specific edge, Option B or C is usually superior for the stated thesis."
            ),
            "quiz_bank": [
                ("Expense ratio is best described as:", [
                    "The recurring annual fee deducted from the fund's NAV, compounding against net returns.",
                    "A one-time commission at purchase.",
                    "The bid-ask spread on the fund.",
                    "The fund's dividend yield.",
                ], "The recurring annual fee deducted from the fund's NAV, compounding against net returns."),
                ("Bid-ask spread represents:", [
                    "The gap between the best buy price and the best sell price, which is a cost paid per trade.",
                    "The fund's annual management fee.",
                    "The difference between open and close price.",
                    "A tax imposed by the exchange.",
                ], "The gap between the best buy price and the best sell price, which is a cost paid per trade."),
                ("Tracking difference measures:", [
                    "The gap between a fund's actual return and its benchmark's return after costs and frictions.",
                    "The volatility of the fund's dividends.",
                    "The size of the fund's cash holdings.",
                    "The fund's trading volume.",
                ], "The gap between a fund's actual return and its benchmark's return after costs and frictions."),
                ("Which statement about ETFs vs mutual funds in taxable accounts is most accurate?", [
                    "ETFs are generally more tax-efficient because their in-kind creation/redemption mechanism limits realized capital gains.",
                    "Mutual funds are always more tax-efficient.",
                    "They are identical in every dimension.",
                    "ETFs have no tax treatment at all.",
                ], "ETFs are generally more tax-efficient because their in-kind creation/redemption mechanism limits realized capital gains."),
                ("A student wants broad US large-cap exposure. The strongest default is typically:", [
                    "A large, high-volume, low-expense-ratio broad-market ETF.",
                    "A single mega-cap stock.",
                    "A leveraged 3x fund.",
                    "A single-sector thematic ETF.",
                ], "A large, high-volume, low-expense-ratio broad-market ETF."),
                ("Which vehicle typically has the highest realized cost of ownership for passive core exposure?", [
                    "An actively managed mutual fund with high turnover and a high expense ratio.",
                    "A broad-market ETF.",
                    "A passive index ETF.",
                    "A Treasury bill.",
                ], "An actively managed mutual fund with high turnover and a high expense ratio."),
                ("Why is AUM (assets under management) relevant to ETF selection?", [
                    "Higher AUM usually correlates with tighter spreads, better liquidity, and lower closure risk.",
                    "Higher AUM always means higher fees.",
                    "AUM determines the fund's dividend policy.",
                    "AUM is irrelevant to execution costs.",
                ], "Higher AUM usually correlates with tighter spreads, better liquidity, and lower closure risk."),
                ("A concentrated single-stock position is most appropriate when:", [
                    "The investor has firm-specific evidence, explicit sizing rules, and tolerance for idiosyncratic risk.",
                    "There is no thesis but conviction is high.",
                    "The stock has risen recently.",
                    "There is a popular social-media narrative.",
                ], "The investor has firm-specific evidence, explicit sizing rules, and tolerance for idiosyncratic risk."),
                ("Leveraged ETFs (e.g., 3x daily) are problematic for long-horizon holders because:", [
                    "Daily reset causes path-dependent decay in volatile markets.",
                    "They pay no dividends.",
                    "They are banned by exchanges.",
                    "They have zero tracking error.",
                ], "Daily reset causes path-dependent decay in volatile markets."),
                ("Which is the most common hidden cost of holding an actively managed fund?", [
                    "Capital-gain distributions generated by portfolio turnover.",
                    "Bid-ask spread on NAV.",
                    "Dividend reinvestment penalties.",
                    "Regulatory fees paid by investors.",
                ], "Capital-gain distributions generated by portfolio turnover."),
                ("A sector ETF differs from a broad-market ETF mainly because:", [
                    "It deliberately concentrates exposure in one sector, increasing both potential edge and variance.",
                    "It is more tax-efficient.",
                    "It has no expense ratio.",
                    "It is always cheaper to trade.",
                ], "It deliberately concentrates exposure in one sector, increasing both potential edge and variance."),
                ("Why is trading volume relevant to ETF implementation?", [
                    "Higher volume supports tighter bid-ask spreads and better execution for large orders.",
                    "Higher volume means higher taxes.",
                    "Volume determines dividend yield.",
                    "Volume is irrelevant.",
                ], "Higher volume supports tighter bid-ask spreads and better execution for large orders."),
                ("Which comparison best illustrates 'total cost of ownership' logic?", [
                    "A 3-bp ETF with a 1-bp spread can cost more than a 5-bp ETF with a 0.5-bp spread if you trade often.",
                    "Only expense ratio matters.",
                    "Bid-ask spread only matters for bonds.",
                    "Capital-gain distributions are irrelevant.",
                ], "A 3-bp ETF with a 1-bp spread can cost more than a 5-bp ETF with a 0.5-bp spread if you trade often."),
                ("A student plans to hold the position for 10 years. Execution-cost logic says:", [
                    "A small initial spread amortizes over time; expense ratio dominates total cost.",
                    "Spread dominates over 10 years.",
                    "Expense ratio is irrelevant for long holds.",
                    "Taxes matter more than fees.",
                ], "A small initial spread amortizes over time; expense ratio dominates total cost."),
                ("An actively managed fund beating its benchmark over three years is best evaluated by:", [
                    "Checking risk-adjusted return, factor exposure, and repeatability of the process.",
                    "Looking only at trailing return.",
                    "Buying immediately regardless of cost.",
                    "Avoiding any active fund.",
                ], "Checking risk-adjusted return, factor exposure, and repeatability of the process."),
                ("Which is a reasonable tactical reason to prefer a single stock over an ETF?", [
                    "A specific, testable thesis with firm-level evidence and bounded position size.",
                    "The stock's recent momentum with no fundamental support.",
                    "A tip from an anonymous online source.",
                    "Preference for the company's logo.",
                ], "A specific, testable thesis with firm-level evidence and bounded position size."),
                ("When a mutual fund makes a large year-end capital-gain distribution, the shareholder:", [
                    "Receives a taxable event even if they did not sell shares.",
                    "Earns a guaranteed return equal to the distribution.",
                    "Owes no taxes under any circumstance.",
                    "Cannot reinvest the distribution.",
                ], "Receives a taxable event even if they did not sell shares."),
                ("Why does QQQ vs VOO vs VTI ownership often overlap heavily?", [
                    "All three hold most US mega-cap names with overlapping weights.",
                    "None of them hold US equities.",
                    "Only VTI owns stocks.",
                    "They hold entirely different universes.",
                ], "All three hold most US mega-cap names with overlapping weights."),
                ("A student wants 'clean technology' exposure. The disciplined step is to:", [
                    "Compare 2–3 thematic ETFs on holdings, top weights, methodology, and expense ratio before choosing.",
                    "Pick the ETF with the most catchy name.",
                    "Buy three clean-tech ETFs at equal weight without checking overlap.",
                    "Ignore methodology entirely.",
                ], "Compare 2–3 thematic ETFs on holdings, top weights, methodology, and expense ratio before choosing."),
                ("'Closet indexer' describes an active fund that:", [
                    "Closely tracks its benchmark while charging active fees, offering poor value.",
                    "Is closed to new investors.",
                    "Only invests in closed-end funds.",
                    "Has zero expense ratio.",
                ], "Closely tracks its benchmark while charging active fees, offering poor value."),
            ],
        })
    elif "fundamental analysis" in lower_title:
        plan.update({
            "hook": "Fundamental analysis asks: what is this business worth, and where is the market likely mispricing it?",
            "core_terms": [
                ("Revenue quality", "Sustainable growth with sound unit economics beats one-time spikes."),
                ("Margin structure", "Operating leverage amplifying both upside and downside."),
                ("Valuation multiple", "Price paid relative to earnings, cash flow, or sales."),
            ],
            "likely_confusion": "Great businesses can still be poor investments when valuation assumptions are stretched.",
            "scenario": "Two firms grow similarly, but one converts cash better with lower leverage. Determine investability and required margin of safety.",
            "deep_dive_paragraphs": [
                "Fundamental analysis evaluates a business's ability to generate durable cash flow and the price you pay to own that cash flow. The three primary layers are revenue quality (Is growth durable? Are customers sticky? Are unit economics sound?), margin structure (Does incremental revenue flow to operating profit, or does cost scale in parallel?), and balance-sheet resilience (Can the business survive a downturn without dilutive financing?). Strong analysis integrates all three before forming a view on price.",
                "Valuation multiples (P/E, EV/EBITDA, EV/Sales, FCF yield) compress a lot of information into one ratio. They are most useful when compared across peers, across the firm's own history, and against expected growth rates. A 30x P/E can be cheap for a compounder growing 25% per year with expanding margins and expensive for a cyclical firm near a margin peak. Use multiples to ask questions, not to declare verdicts.",
                "Margin of safety is the gap between your estimate of intrinsic value and the price you pay. It exists because estimates are uncertain: demand might be cyclical, margins might compress, competitive dynamics might shift. A disciplined investor only acts when the price-to-value gap is wide enough to absorb reasonable forecasting error, and sizes down (or skips the trade) when it is not.",
            ],
            "worked_example": (
                "Firm A: revenue growth 15%, operating margin 30% (expanding), FCF margin 22%, net cash positive, P/E 28. Firm B: revenue growth 15%, operating margin 10% (flat), FCF margin 4%, net debt 3x EBITDA, P/E 14. Firm B looks cheaper on multiple, but A converts growth to free cash flow and has a resilient balance sheet. After a downside scenario, A's cash flow holds; B's could force dilutive financing. The disciplined buy is often A, despite the headline multiple."
            ),
            "quiz_bank": [
                ("Revenue quality is best evaluated by:", [
                    "Durability of growth, customer retention, pricing power, and unit economics.",
                    "Year-over-year revenue change only.",
                    "Social-media mention count.",
                    "Analyst sentiment scores alone.",
                ], "Durability of growth, customer retention, pricing power, and unit economics."),
                ("A firm's operating margin expanding as revenue grows indicates:", [
                    "Positive operating leverage: incremental revenue flows through to profit.",
                    "The firm is over-investing.",
                    "The firm must be mispriced.",
                    "Revenue quality is falling.",
                ], "Positive operating leverage: incremental revenue flows through to profit."),
                ("FCF (free cash flow) yield measures:", [
                    "Annual free cash flow divided by market cap (or EV) as a price-to-value proxy.",
                    "Trailing dividend yield only.",
                    "Realized return on the stock.",
                    "Rate of share buybacks.",
                ], "Annual free cash flow divided by market cap (or EV) as a price-to-value proxy."),
                ("A high P/E is best interpreted when compared to:", [
                    "Peers, the firm's own history, and the expected growth rate.",
                    "The dividend of a bond ETF.",
                    "The VIX level.",
                    "Nothing; P/E is absolute.",
                ], "Peers, the firm's own history, and the expected growth rate."),
                ("Margin of safety is best described as:", [
                    "The gap between estimated intrinsic value and current price, buffering forecasting error.",
                    "A guarantee the investment cannot lose money.",
                    "The broker's margin requirement.",
                    "The volatility of the stock.",
                ], "The gap between estimated intrinsic value and current price, buffering forecasting error."),
                ("Which statement about unit economics is most accurate?", [
                    "Profitable unit economics mean each customer generates more lifetime value than the cost to acquire and serve them.",
                    "Unit economics only matter for consumer firms.",
                    "Unit economics are the same as gross margin.",
                    "Unit economics are irrelevant for public companies.",
                ], "Profitable unit economics mean each customer generates more lifetime value than the cost to acquire and serve them."),
                ("A net-cash balance sheet is valuable mainly because it:", [
                    "Allows the firm to survive downturns without dilutive financing or distressed covenants.",
                    "Guarantees faster growth.",
                    "Eliminates all competitive risk.",
                    "Makes taxes lower.",
                ], "Allows the firm to survive downturns without dilutive financing or distressed covenants."),
                ("Which is a common valuation pitfall for fast-growing firms?", [
                    "Using a single trailing P/E without evaluating growth durability and reinvestment needs.",
                    "Using forward estimates.",
                    "Using peer comparisons.",
                    "Using margin analysis.",
                ], "Using a single trailing P/E without evaluating growth durability and reinvestment needs."),
                ("A DCF primarily values a firm by:", [
                    "Discounting projected future cash flows back to present value at a required rate of return.",
                    "Multiplying last year's revenue by a fixed number.",
                    "Using only accounting book value.",
                    "Averaging price targets from analysts.",
                ], "Discounting projected future cash flows back to present value at a required rate of return."),
                ("Why is 'margin structure' more informative than a single gross-margin snapshot?", [
                    "It shows how margins evolve with scale, competition, and mix over time.",
                    "It only measures cost of goods.",
                    "It is less informative.",
                    "It predicts macro regimes.",
                ], "It shows how margins evolve with scale, competition, and mix over time."),
                ("An analyst sees revenue up 20% but accounts receivable up 45%. The most disciplined interpretation is:", [
                    "Possible channel stuffing or aggressive revenue recognition; investigate cash-conversion cycle.",
                    "Clear evidence of superior growth.",
                    "Irrelevant because only net income matters.",
                    "Always a buy signal.",
                ], "Possible channel stuffing or aggressive revenue recognition; investigate cash-conversion cycle."),
                ("Which is a common reason a great business is still a poor investment?", [
                    "The market has already priced in expected growth, leaving no margin of safety.",
                    "Good businesses are never poor investments.",
                    "Because the CEO is popular.",
                    "Because dividends are high.",
                ], "The market has already priced in expected growth, leaving no margin of safety."),
                ("A cyclical firm near a record operating margin is risky to value on trailing multiples because:", [
                    "Margins tend to mean-revert through the cycle, making 'cheap' multiples a value trap.",
                    "Multiples never mean-revert.",
                    "Cyclical firms do not have margins.",
                    "Regulators ban cyclical analysis.",
                ], "Margins tend to mean-revert through the cycle, making 'cheap' multiples a value trap."),
                ("Which pair is most consistent with durable compounding?", [
                    "Expanding margins and consistent reinvestment at high returns on capital.",
                    "Falling margins and rising capex without reinvestment discipline.",
                    "Dividend cuts and rising leverage.",
                    "One-time gains driving revenue.",
                ], "Expanding margins and consistent reinvestment at high returns on capital."),
                ("Why is ROIC (return on invested capital) useful for quality assessment?", [
                    "It measures how well the firm converts capital deployed into economic profit.",
                    "It is a macro indicator.",
                    "It equals the dividend yield.",
                    "It measures only marketing spend.",
                ], "It measures how well the firm converts capital deployed into economic profit."),
                ("Share-based compensation (SBC) is best treated in valuation as:", [
                    "A real cost that dilutes owners, even if added back in non-GAAP metrics.",
                    "A non-cash item that can be ignored entirely.",
                    "A bonus return to shareholders.",
                    "Irrelevant to free cash flow analysis.",
                ], "A real cost that dilutes owners, even if added back in non-GAAP metrics."),
                ("A firm with deteriorating FCF despite rising GAAP earnings likely has:", [
                    "Working-capital or accruals issues that warrant investigation.",
                    "Perfect fundamentals.",
                    "A stronger balance sheet.",
                    "No cost of capital.",
                ], "Working-capital or accruals issues that warrant investigation."),
                ("Which is the most reliable reason to raise a valuation estimate?", [
                    "Durable improvements in revenue quality, margin structure, or capital efficiency.",
                    "A stock price that has risen recently.",
                    "A news article calling the stock cheap.",
                    "Higher social-media sentiment.",
                ], "Durable improvements in revenue quality, margin structure, or capital efficiency."),
                ("The 'reversal of the narrative' risk for a high-multiple stock means:", [
                    "If growth or margin narrative weakens, the multiple and earnings can both compress (double hit).",
                    "Multiples expand automatically.",
                    "The stock cannot fall below book value.",
                    "The dividend cannot be cut.",
                ], "If growth or margin narrative weakens, the multiple and earnings can both compress (double hit)."),
                ("A disciplined fundamental thesis includes which of the following?", [
                    "Clear claim, evidence for revenue/margin/balance sheet, horizon, and invalidation triggers.",
                    "Only a price target.",
                    "Only a tip from a forum.",
                    "Only trailing P/E.",
                ], "Clear claim, evidence for revenue/margin/balance sheet, horizon, and invalidation triggers."),
            ],
        })
    elif "technical analysis" in lower_title:
        plan.update({
            "hook": "Technical analysis is probability management using price, participation, and structure.",
            "core_terms": [
                ("Trend", "Directional persistence across time horizons."),
                ("Support/resistance", "Price zones where supply-demand balance often shifts."),
                ("Momentum", "The strength and persistence of price movement."),
            ],
            "likely_confusion": "Students treat chart setups as certainty instead of conditional probability.",
            "scenario": "A stock breaks resistance on strong volume, then retests breakout level. Decide add/hold/exit using invalidation logic.",
            "deep_dive_paragraphs": [
                "Technical analysis reads the market's collective behavior through price, volume, and breadth. It does not predict the future; it estimates conditional probabilities. A breakout above resistance on rising volume with confirming breadth is a different setup than a breakout on thin volume. The trader's job is to distinguish high-conviction conditions from noise and size positions accordingly.",
                "Trend is the directional persistence of price over a chosen horizon. Multi-timeframe trend alignment (daily up, weekly up) strengthens a thesis; conflicting timeframes weaken it. Moving averages (50-day, 200-day) are useful structural landmarks, not magic: price above a rising 200-day is a different regime from price below a falling 200-day, and sizing/risk rules should differ accordingly.",
                "Support and resistance are zones where supply-demand history has tipped the balance. They work because many market participants reference the same levels, making them partial self-fulfilling. A break of resistance that holds on a retest is higher-probability than a single spike; a break that fails is a common failure mode, and pre-defining the invalidation level (e.g., close back below support) keeps losses bounded.",
            ],
            "worked_example": (
                "A stock trends up above a rising 200-day moving average, pulls back to a clear prior resistance level (now support), and reclaims it on rising volume. Plan: enter at 3% size, stop under the support zone (about -6% away), target a prior swing high (+18%), risk-reward ~1:3. If the retest fails and the stock closes back below support, the stop triggers and the thesis is invalidated. The decision is pre-committed; the outcome does not change whether the plan was well-run."
            ),
            "quiz_bank": [
                ("A 'trend' in technical analysis is best described as:", [
                    "Directional persistence of price over a chosen horizon with higher highs and higher lows (or vice versa).",
                    "Any single-day move.",
                    "The dividend yield.",
                    "An accounting metric.",
                ], "Directional persistence of price over a chosen horizon with higher highs and higher lows (or vice versa)."),
                ("Support and resistance zones work partially because:", [
                    "Many market participants reference similar levels, making them partially self-fulfilling.",
                    "They are enforced by exchanges.",
                    "They are randomly assigned daily.",
                    "They come from fundamentals only.",
                ], "Many market participants reference similar levels, making them partially self-fulfilling."),
                ("A break of resistance on rising volume is typically stronger than one on falling volume because:", [
                    "Volume confirms participation, raising the conditional probability of follow-through.",
                    "Falling volume guarantees a reversal.",
                    "Volume has no relationship to probability.",
                    "Volume is only relevant for bonds.",
                ], "Volume confirms participation, raising the conditional probability of follow-through."),
                ("A 'failed breakout' is best described as:", [
                    "Price breaking a level and then reversing back inside, often producing sharper moves in the opposite direction.",
                    "A stock that cannot be sold.",
                    "A stock that pays no dividend.",
                    "A stock that only trades at even prices.",
                ], "Price breaking a level and then reversing back inside, often producing sharper moves in the opposite direction."),
                ("Moving averages are best used as:", [
                    "Structural landmarks for trend context, not as magic decision signals.",
                    "Exact entry triggers with no other confirmation.",
                    "Volume indicators.",
                    "Fundamental metrics.",
                ], "Structural landmarks for trend context, not as magic decision signals."),
                ("Momentum in this course is:", [
                    "The strength and persistence of price movement over a chosen horizon.",
                    "A valuation multiple.",
                    "A sentiment survey.",
                    "A Treasury yield.",
                ], "The strength and persistence of price movement over a chosen horizon."),
                ("An invalidation level (stop) should be placed where:", [
                    "Price action would prove the thesis wrong (e.g., break of key support).",
                    "At the exact entry price.",
                    "Anywhere that is convenient.",
                    "It is not necessary.",
                ], "Price action would prove the thesis wrong (e.g., break of key support)."),
                ("Multi-timeframe alignment means:", [
                    "Checking that the trade's direction is consistent on longer and shorter horizons.",
                    "Using only one timeframe.",
                    "Trading only on monthly charts.",
                    "Ignoring timeframes entirely.",
                ], "Checking that the trade's direction is consistent on longer and shorter horizons."),
                ("Risk-reward ratio of 1:3 implies the trade:", [
                    "Risks one unit for a potential three units of gain at planned targets.",
                    "Has zero risk.",
                    "Cannot lose more than 30%.",
                    "Is guaranteed to succeed.",
                ], "Risks one unit for a potential three units of gain at planned targets."),
                ("Which statement about chart patterns is most accurate?", [
                    "They are conditional probabilities, not certainties, and should be sized accordingly.",
                    "They always play out exactly as drawn.",
                    "They are random and useless.",
                    "They replace the need for a stop.",
                ], "They are conditional probabilities, not certainties, and should be sized accordingly."),
                ("Breadth (e.g., advance-decline line) is useful because:", [
                    "It shows whether a trend is broad-based or driven by a narrow group of leaders.",
                    "It determines fees.",
                    "It measures dividends.",
                    "It is irrelevant to trends.",
                ], "It shows whether a trend is broad-based or driven by a narrow group of leaders."),
                ("A pullback to a prior breakout level that holds on reduced volume is best interpreted as:", [
                    "A potential continuation setup if volume reappears on the next leg up.",
                    "A guaranteed reversal.",
                    "Unrelated to trend health.",
                    "Only relevant for mutual funds.",
                ], "A potential continuation setup if volume reappears on the next leg up."),
                ("RSI (relative strength index) is commonly used to:", [
                    "Gauge short-term momentum extremes and potential mean-reversion pressure.",
                    "Measure dividend yield.",
                    "Calculate operating margin.",
                    "Replace stop placement.",
                ], "Gauge short-term momentum extremes and potential mean-reversion pressure."),
                ("A 'trend follower' tends to:", [
                    "Buy strength and cut weakness, with defined invalidation and position sizing.",
                    "Always buy the biggest loser.",
                    "Ignore trends.",
                    "Hold only cash.",
                ], "Buy strength and cut weakness, with defined invalidation and position sizing."),
                ("Technical analysis should typically be combined with:", [
                    "Fundamentals and risk management to form a complete decision.",
                    "Social-media tips only.",
                    "Dividend yield only.",
                    "Nothing; it stands alone.",
                ], "Fundamentals and risk management to form a complete decision."),
                ("The main risk of technical setups during news events is:", [
                    "Exogenous information can override the pattern, producing sharp, gap-level invalidations.",
                    "Volume drops to zero.",
                    "Technical patterns are illegal during news.",
                    "Patterns always complete faster.",
                ], "Exogenous information can override the pattern, producing sharp, gap-level invalidations."),
                ("'Volume precedes price' is best interpreted as:", [
                    "A rise in participation often signals conviction that supports subsequent price moves.",
                    "Volume always happens after price.",
                    "Volume has no relationship to conviction.",
                    "Only price matters in setups.",
                ], "A rise in participation often signals conviction that supports subsequent price moves."),
                ("A gap up on earnings that closes below the prior day's high is often:", [
                    "A failed continuation setup warranting caution or tighter risk.",
                    "A guaranteed buy.",
                    "Irrelevant to technicals.",
                    "A signal to double position size.",
                ], "A failed continuation setup warranting caution or tighter risk."),
                ("Which is the most common technical mistake for new traders?", [
                    "Treating patterns as certainties and ignoring invalidation and position sizing.",
                    "Using multiple timeframes.",
                    "Checking breadth.",
                    "Using a stop-loss.",
                ], "Treating patterns as certainties and ignoring invalidation and position sizing."),
                ("A disciplined technical trade plan includes:", [
                    "Entry, stop, target, position size, horizon, and invalidation evidence.",
                    "Entry price only.",
                    "Target only.",
                    "Social-media confirmation only.",
                ], "Entry, stop, target, position size, horizon, and invalidation evidence."),
            ],
        })
    elif "behavioral finance" in lower_title:
        plan.update({
            "hook": "Your decision habits under stress can dominate portfolio outcomes.",
            "core_terms": [
                ("Loss aversion", "Loss pain can drive irrational risk-seeking or paralysis."),
                ("Recency bias", "Overweighting recent outcomes in future expectations."),
                ("Confirmation bias", "Filtering information to protect your prior view."),
            ],
            "likely_confusion": "Biases do not disappear with experience; in many cases they become more subtle.",
            "scenario": "You keep averaging down to 'get back to even.' Diagnose the bias and design a stronger pre-commitment rule.",
            "deep_dive_paragraphs": [
                "Behavioral finance studies the systematic ways human decisions deviate from rational analysis under uncertainty. The goal is not to 'remove emotion' (impossible) but to build rules and environments that make good behavior the default under stress. A trading log, pre-committed stops, position-size caps, and scheduled review cadence all function as behavioral scaffolding.",
                "Loss aversion makes a given loss feel roughly twice as painful as an equivalent gain feels good. This asymmetry drives several documented behaviors: holding losers too long (to avoid realizing loss), selling winners too early (to lock in gain), and taking excessive risk to 'get even.' The countermeasure is to make entry, exit, and sizing rules mechanical and visible so the pain of loss is not negotiated in real time.",
                "Recency and confirmation bias corrupt the information stream. Recent outcomes feel more representative than they are, and we unconsciously filter new evidence to protect prior conclusions. Disciplined investors counter this by seeking disconfirming evidence explicitly, tracking base rates, and writing a thesis in advance so post-hoc rationalization becomes easier to detect.",
            ],
            "worked_example": (
                "A student loses 8% on a position and feels pressured to double down to 'get back to even.' Behavioral diagnosis: loss aversion plus sunk-cost fallacy. Pre-committed rule (written before entry): 'I only add to winners above the 50-day moving average with fundamental improvement; I never add to a position below its stop level.' Applying the rule: student does not average down; trims per plan and redeploys capital into a setup that currently meets criteria. Process is preserved; emotional damage does not compound."
            ),
            "quiz_bank": [
                ("Loss aversion describes the tendency to:", [
                    "Feel the pain of losses roughly twice as strongly as the pleasure of equivalent gains.",
                    "Treat all outcomes symmetrically.",
                    "Avoid all risk at all times.",
                    "Always choose the highest expected return.",
                ], "Feel the pain of losses roughly twice as strongly as the pleasure of equivalent gains."),
                ("Recency bias is best defined as:", [
                    "Overweighting recent events when forming expectations about the future.",
                    "Weighting all history equally.",
                    "Ignoring recent events entirely.",
                    "A bias specific to bond traders.",
                ], "Overweighting recent events when forming expectations about the future."),
                ("Confirmation bias causes investors to:", [
                    "Seek information that supports their existing view and discount information that contradicts it.",
                    "Always seek disconfirming evidence.",
                    "Reject every news source.",
                    "Never read research.",
                ], "Seek information that supports their existing view and discount information that contradicts it."),
                ("The strongest counter-measure to emotional decision making is:", [
                    "Pre-committed written rules applied mechanically under stress.",
                    "Meditation alone.",
                    "Ignoring the market entirely.",
                    "Trusting instinct in the moment.",
                ], "Pre-committed written rules applied mechanically under stress."),
                ("Anchoring bias describes:", [
                    "Over-weighting an initial reference point (e.g., purchase price) in subsequent decisions.",
                    "Investing only in marine industries.",
                    "Using only forward estimates.",
                    "Never using base rates.",
                ], "Over-weighting an initial reference point (e.g., purchase price) in subsequent decisions."),
                ("Overconfidence typically shows up as:", [
                    "Excess position sizing, over-trading, and underestimation of uncertainty.",
                    "Under-allocating to equities.",
                    "Refusing to place any trade.",
                    "Always holding cash.",
                ], "Excess position sizing, over-trading, and underestimation of uncertainty."),
                ("The disposition effect is:", [
                    "Selling winners too early and holding losers too long.",
                    "Selling losers too early and holding winners too long.",
                    "Holding cash in all conditions.",
                    "Trading only bonds.",
                ], "Selling winners too early and holding losers too long."),
                ("'Sunk cost fallacy' in investing means:", [
                    "Holding or adding to a position because of past costs rather than current evidence.",
                    "Ignoring all past costs.",
                    "Selling all losers immediately.",
                    "Ignoring transaction costs.",
                ], "Holding or adding to a position because of past costs rather than current evidence."),
                ("Averaging down to 'get back to even' is most often a symptom of:", [
                    "Loss aversion combined with sunk-cost fallacy.",
                    "Disciplined process.",
                    "Correct risk management.",
                    "Strong fundamental analysis.",
                ], "Loss aversion combined with sunk-cost fallacy."),
                ("A trading log improves decision quality mainly because it:", [
                    "Creates an objective record that reveals patterns the memory edits out.",
                    "Guarantees profits.",
                    "Replaces a broker.",
                    "Reduces taxes.",
                ], "Creates an objective record that reveals patterns the memory edits out."),
                ("Pre-mortem analysis asks the investor to:", [
                    "Imagine the trade has failed and identify what went wrong before placing it.",
                    "Never review trades after the fact.",
                    "Analyze only winning trades.",
                    "Ignore downside scenarios.",
                ], "Imagine the trade has failed and identify what went wrong before placing it."),
                ("Which is the most durable bias mitigation?", [
                    "Written rules, checklists, and environment design that prevent in-the-moment negotiation.",
                    "Trying harder to be rational.",
                    "Using a larger monitor.",
                    "Avoiding all reading.",
                ], "Written rules, checklists, and environment design that prevent in-the-moment negotiation."),
                ("Herding in markets describes:", [
                    "Following the crowd rather than independent analysis, often chasing performance.",
                    "Owning livestock companies.",
                    "Diversifying into 10 ETFs.",
                    "Using only momentum strategies.",
                ], "Following the crowd rather than independent analysis, often chasing performance."),
                ("Availability bias is:", [
                    "Overweighting information that is easy to recall (e.g., vivid news) versus base rates.",
                    "Always available information.",
                    "A technical indicator.",
                    "A bond rating.",
                ], "Overweighting information that is easy to recall (e.g., vivid news) versus base rates."),
                ("Which statement about experience and bias is most accurate?", [
                    "Biases do not disappear with experience and often become subtler and harder to detect.",
                    "Biases disappear after 10 trades.",
                    "Experience guarantees bias-free decisions.",
                    "Biases only affect beginners.",
                ], "Biases do not disappear with experience and often become subtler and harder to detect."),
                ("A strong debiasing habit for new investors is to:", [
                    "Write down thesis, evidence, size, and invalidation before every trade and review them after.",
                    "Trade larger size to overcome hesitation.",
                    "Trade only on tips.",
                    "Avoid all written records.",
                ], "Write down thesis, evidence, size, and invalidation before every trade and review them after."),
                ("'Narrative bias' refers to:", [
                    "Weighting a compelling story over evidence and base rates.",
                    "Preferring nonfiction investment books.",
                    "Using only quantitative signals.",
                    "Ignoring quarterly reports.",
                ], "Weighting a compelling story over evidence and base rates."),
                ("Overtrading often increases:", [
                    "Transaction costs, taxes, and behavioral errors without improving expected return.",
                    "Risk-adjusted return.",
                    "Diversification benefit.",
                    "Compound return.",
                ], "Transaction costs, taxes, and behavioral errors without improving expected return."),
                ("Which is the best outcome of a disciplined post-trade review?", [
                    "Identifying process gaps distinct from outcome noise and encoding one concrete upgrade.",
                    "Celebrating only wins.",
                    "Ignoring losses.",
                    "Adjusting rules after every single trade regardless of evidence.",
                ], "Identifying process gaps distinct from outcome noise and encoding one concrete upgrade."),
                ("A commitment device in investing is:", [
                    "A structural mechanism (automatic rebalancing, stop orders, position-size caps) that enforces intended behavior.",
                    "A discretionary override.",
                    "A news subscription.",
                    "A margin loan.",
                ], "A structural mechanism (automatic rebalancing, stop orders, position-size caps) that enforces intended behavior."),
            ],
        })
    elif "portfolio construction" in lower_title:
        plan.update({
            "hook": "Great ideas still fail if sizing and interaction risk are weak.",
            "core_terms": [
                ("Position sizing", "Capital allocation per idea based on conviction and downside."),
                ("Risk budget", "How much uncertainty your plan can absorb."),
                ("Rebalancing", "Systematic weight maintenance to control drift and concentration."),
            ],
            "likely_confusion": "Students optimize entry timing but ignore cross-position correlation and portfolio-level risk.",
            "scenario": "You have five strong ideas but only enough risk budget for two full positions and three starter positions.",
            "deep_dive_paragraphs": [
                "Portfolio construction is the translation of ideas into sized, risk-bounded positions that interact sensibly. A portfolio is not a list of favorite stocks; it is a system whose volatility, drawdown potential, and factor exposures should be consistent with the investor's plan. Strong construction ensures that no single position—or correlated group of positions—can produce a portfolio-level loss larger than planned.",
                "Position sizing should scale with conviction and inverse to downside. A standard framework: starter positions for new or lower-conviction ideas (~2–3% of portfolio), core positions for confirmed ideas with defined invalidation (~4–6%), and full positions for highest-conviction, risk-bounded ideas (~7–10% with caps). Beyond roughly 10%, single-position risk dominates portfolio outcomes and requires explicit rationale.",
                "Risk budgeting sets the total volatility or drawdown the plan can absorb, then allocates it across ideas by expected contribution. Rebalancing mechanically keeps drift from concentrating risk—especially after winners appreciate or losers shrink. The interaction: when correlations rise across positions, real portfolio risk is higher than the sum of single-name risks, and sizes should come down.",
            ],
            "worked_example": (
                "Five ideas (A–E). Conviction: A, B high; C medium; D, E starter. Planned sizing: A=8%, B=8%, C=5%, D=3%, E=3% (total 27% active; rest in core beta). Correlation check: A and B are both semi names with ~0.85 correlation; combined risk is larger than sum of independent. Adjustment: trim A and B to 6% each so that combined single-factor exposure stays within planned cap; use freed 4% to fund C or a defensive sleeve. Construction changes the plan without diluting the ideas."
            ),
            "quiz_bank": [
                ("Position sizing in this course should primarily scale with:", [
                    "Conviction and downside, with explicit caps at the individual and factor level.",
                    "Recent price momentum only.",
                    "Number of analyst ratings.",
                    "Size of the ticker.",
                ], "Conviction and downside, with explicit caps at the individual and factor level."),
                ("A 'risk budget' for a portfolio is best described as:", [
                    "The total volatility or drawdown the plan is willing to absorb, allocated across positions.",
                    "The number of trades allowed per week.",
                    "The fund's expense ratio.",
                    "The dividend yield.",
                ], "The total volatility or drawdown the plan is willing to absorb, allocated across positions."),
                ("Rebalancing serves which primary purpose in portfolio construction?", [
                    "Keeping realized risk close to planned risk by controlling drift and concentration.",
                    "Maximizing turnover and fees.",
                    "Forcing losers into winners.",
                    "Guaranteeing higher returns.",
                ], "Keeping realized risk close to planned risk by controlling drift and concentration."),
                ("Which is a sensible starter-position size for a lower-conviction idea?", [
                    "About 2–3% of portfolio.",
                    "20% of portfolio.",
                    "40% of portfolio.",
                    "80% of portfolio.",
                ], "About 2–3% of portfolio."),
                ("Why do highly correlated positions require smaller individual sizing than their conviction suggests?", [
                    "Because combined factor exposure makes portfolio risk larger than the sum of independent single-name risks.",
                    "Because correlations are always stable.",
                    "Because the exchange requires it.",
                    "Because they cannot be sold.",
                ], "Because combined factor exposure makes portfolio risk larger than the sum of independent single-name risks."),
                ("Which portfolio structure is most brittle under stress?", [
                    "Concentrated in a single factor with no diversifying sleeve and no pre-committed invalidation.",
                    "Diversified across factors with defined sizing and stops.",
                    "Risk-parity weighted.",
                    "Equal-weighted across sectors.",
                ], "Concentrated in a single factor with no diversifying sleeve and no pre-committed invalidation."),
                ("A 'core-satellite' construction typically means:", [
                    "Holding broad low-cost beta as the core and layering smaller active tilts as satellites.",
                    "Holding only single stocks.",
                    "Holding only bonds.",
                    "Holding only cash.",
                ], "Holding broad low-cost beta as the core and layering smaller active tilts as satellites."),
                ("Single-position caps exist because:", [
                    "They bound the damage any one idea can do to portfolio-level outcomes.",
                    "They maximize turnover.",
                    "They guarantee alpha.",
                    "They are mandated by exchanges.",
                ], "They bound the damage any one idea can do to portfolio-level outcomes."),
                ("An investor with a 15% max drawdown tolerance should typically:", [
                    "Cap single-name positions and avoid concentration in a single factor.",
                    "Hold only leveraged ETFs.",
                    "Concentrate into the best performer.",
                    "Ignore drawdown entirely.",
                ], "Cap single-name positions and avoid concentration in a single factor."),
                ("Which metric most directly measures concentration risk?", [
                    "Sum of top-3 (or top-5) position weights.",
                    "Annual dividend yield.",
                    "Number of ETFs owned.",
                    "Rolling 30-day return.",
                ], "Sum of top-3 (or top-5) position weights."),
                ("Why do winners need active trimming under disciplined construction?", [
                    "Appreciation silently raises portfolio risk beyond the originally accepted size.",
                    "Winners always continue winning.",
                    "Trimming reduces total fees.",
                    "Trimming is required by exchanges.",
                ], "Appreciation silently raises portfolio risk beyond the originally accepted size."),
                ("A 'volatility target' portfolio typically:", [
                    "Adjusts exposure up or down to keep portfolio volatility near a chosen level.",
                    "Holds fixed dollar exposure forever.",
                    "Uses only leveraged ETFs.",
                    "Never adjusts exposure.",
                ], "Adjusts exposure up or down to keep portfolio volatility near a chosen level."),
                ("Which is the main danger of sizing by conviction alone (ignoring downside)?", [
                    "A single high-conviction idea with large downside can cause outsized portfolio damage.",
                    "It always underperforms.",
                    "It increases fees.",
                    "It is banned by brokers.",
                ], "A single high-conviction idea with large downside can cause outsized portfolio damage."),
                ("When adding a new position, construction discipline requires:", [
                    "Checking impact on portfolio factor exposures, correlations, and total risk.",
                    "Only checking the new position's expected return.",
                    "Only checking the broker's fee.",
                    "Ignoring portfolio-level impact.",
                ], "Checking impact on portfolio factor exposures, correlations, and total risk."),
                ("A 'risk-parity' construction targets:", [
                    "Equal risk contribution from each sleeve rather than equal dollar weight.",
                    "Equal dollars regardless of risk.",
                    "100% in the highest-volatility asset.",
                    "Only Treasuries.",
                ], "Equal risk contribution from each sleeve rather than equal dollar weight."),
                ("Why is 'invalidation before entry' a construction principle, not just a trading tactic?", [
                    "It bounds the portfolio-level loss each position can inflict and forces honest sizing.",
                    "It maximizes turnover.",
                    "It guarantees profit.",
                    "It is only relevant to options.",
                ], "It bounds the portfolio-level loss each position can inflict and forces honest sizing."),
                ("A 5-position portfolio that is 90% tech is best described as:", [
                    "A concentrated single-factor bet, not a diversified portfolio.",
                    "A risk-parity portfolio.",
                    "A Treasury-heavy mix.",
                    "A defensive dividend portfolio.",
                ], "A concentrated single-factor bet, not a diversified portfolio."),
                ("Cash in portfolio construction is usefully framed as:", [
                    "Optionality: the ability to act when better risk-adjusted setups appear.",
                    "Pure drag that should always be zero.",
                    "A dividend-paying asset.",
                    "A leveraged asset.",
                ], "Optionality: the ability to act when better risk-adjusted setups appear."),
                ("'Drawdown control' construction typically uses which combination?", [
                    "Position-size caps, factor diversification, and pre-committed exit rules.",
                    "Leverage, concentration, and discretionary overrides.",
                    "No rules; pure discretion.",
                    "Only sector rotation.",
                ], "Position-size caps, factor diversification, and pre-committed exit rules."),
                ("Which metric best summarizes 'how well the portfolio was constructed' at the total level?", [
                    "Risk-adjusted return combined with realized drawdown relative to plan.",
                    "Number of trades.",
                    "Weekly change in volatility index.",
                    "Total dividend received.",
                ], "Risk-adjusted return combined with realized drawdown relative to plan."),
            ],
        })
    elif "market events" in lower_title or "macroeconomics" in lower_title:
        plan.update({
            "hook": "Macro sets the environment your portfolio must survive, even when stock selection is strong.",
            "core_terms": [
                ("Inflation regime", "Different inflation environments reward different assets."),
                ("Rate sensitivity", "How valuation and financing conditions react to interest-rate changes."),
                ("Scenario planning", "Pre-defining responses across plausible macro paths."),
            ],
            "likely_confusion": "Students either ignore macro completely or overreact to every headline.",
            "scenario": "A hot inflation print lifts yields quickly. Identify vulnerable holdings and response actions without panic-trading.",
            "deep_dive_paragraphs": [
                "Macro provides the weather in which every portfolio operates. Rates, inflation, growth, and liquidity conditions shift which assets compound easily and which struggle. The goal is not to predict the next release; it is to understand which environments favor which holdings and to plan your response to a few plausible paths before they happen. Well-constructed portfolios survive multiple regimes; brittle ones only work in one.",
                "Interest rates are the single most important macro variable for long-duration assets. A growth stock whose value comes from cash flows far in the future is more sensitive to discount-rate changes than a utility whose cash flows are near-term. The same rate move can compress or expand valuation multiples across the market, often more than underlying earnings changes. Understanding duration at the portfolio level is as important as understanding duration in bonds.",
                "Scenario planning replaces reaction with preparation. A disciplined approach names 3–4 plausible regimes (disinflation with soft landing, sticky inflation with rate pressure, recession with rate cuts, growth re-acceleration), lists the winning and losing asset groups in each, and defines the first portfolio move if evidence confirms one regime is more likely. When the scenario arrives, the response is already scripted.",
            ],
            "worked_example": (
                "A portfolio is 70% US large-cap growth with ~2.5% fixed-income hedge. Hot CPI print lifts 10Y yields 30 bps in two days. Pre-planned scenario A (sticky inflation) response: trim the highest-duration sleeve by ~10%, rotate ~5% of capital into an energy/materials sleeve that tends to outperform in that regime, keep stop rules on individual names. The student does not predict inflation; they execute the prepared response. The move is bounded, reversible if evidence weakens, and avoids the panic-trade alternative."
            ),
            "quiz_bank": [
                ("Which macro variable most directly affects the valuation of long-duration growth equities?", [
                    "The level and path of real interest rates.",
                    "The dividend yield on consumer staples.",
                    "Weekly options volume.",
                    "The number of IPOs last month.",
                ], "The level and path of real interest rates."),
                ("Sticky inflation typically pressures which asset group most?", [
                    "Long-duration growth equities and long-duration Treasuries.",
                    "Short-duration Treasuries and commodities.",
                    "Energy equities.",
                    "Bank deposits.",
                ], "Long-duration growth equities and long-duration Treasuries."),
                ("Scenario planning in this course is best described as:", [
                    "Pre-defining plausible regimes and the portfolio responses before they occur.",
                    "Predicting the exact next release.",
                    "Reacting only to today's headlines.",
                    "Ignoring macro entirely.",
                ], "Pre-defining plausible regimes and the portfolio responses before they occur."),
                ("An inverted yield curve (2Y above 10Y) has historically been associated with:", [
                    "Elevated recession probability over the following 12–24 months.",
                    "Immediate market tops.",
                    "Guaranteed market rallies.",
                    "No macro information.",
                ], "Elevated recession probability over the following 12–24 months."),
                ("Commodities (e.g., energy, materials) often outperform when:", [
                    "Inflation is sticky and real rates are low or falling.",
                    "Growth is high and inflation is falling.",
                    "Rates are at zero and disinflation persists.",
                    "All equities are rallying.",
                ], "Inflation is sticky and real rates are low or falling."),
                ("The Fed raising short rates is most likely to:", [
                    "Pressure rate-sensitive and long-duration equity and bond prices, at least initially.",
                    "Guarantee higher equity returns.",
                    "Have no effect on valuation multiples.",
                    "Only affect emerging markets.",
                ], "Pressure rate-sensitive and long-duration equity and bond prices, at least initially."),
                ("A 'soft landing' macro regime typically means:", [
                    "Inflation cools toward target without a severe recession and rates can be held or cut gradually.",
                    "Inflation spirals and rates are raised aggressively.",
                    "A crash in equities over three months.",
                    "No change in any variable.",
                ], "Inflation cools toward target without a severe recession and rates can be held or cut gradually."),
                ("Duration risk in a bond portfolio describes:", [
                    "Sensitivity of bond prices to changes in interest rates.",
                    "Trading volume of the bond.",
                    "Dividend schedule.",
                    "Coupon payment date.",
                ], "Sensitivity of bond prices to changes in interest rates."),
                ("During a clear recession regime, investors often rotate toward:", [
                    "Defensives, quality balance sheets, and higher-grade fixed income.",
                    "High-beta small-caps exclusively.",
                    "Leveraged growth ETFs.",
                    "Pure cash with no rebalancing.",
                ], "Defensives, quality balance sheets, and higher-grade fixed income."),
                ("A disciplined response to a hot CPI surprise is to:", [
                    "Execute a pre-planned scenario response with bounded size, not a reactive discretionary trade.",
                    "Sell all equities immediately.",
                    "Double growth exposure.",
                    "Ignore macro entirely.",
                ], "Execute a pre-planned scenario response with bounded size, not a reactive discretionary trade."),
                ("Which asset is most commonly used as a stagflation-era hedge?", [
                    "Energy or broad commodity exposure.",
                    "Long-duration zero-coupon Treasuries.",
                    "Unhedged cash.",
                    "High-yield debt.",
                ], "Energy or broad commodity exposure."),
                ("Why is portfolio duration a useful macro lens?", [
                    "It summarizes how sensitive overall portfolio value is to a change in discount rates.",
                    "It measures dividend yield.",
                    "It replaces diversification.",
                    "It is only relevant to options.",
                ], "It summarizes how sensitive overall portfolio value is to a change in discount rates."),
                ("Geopolitical events create portfolio risk primarily through:", [
                    "Volatility spikes, liquidity stress, and sector-level dispersion (energy, defense, shipping).",
                    "Universal rally in all assets.",
                    "Permanent erasure of diversification benefit.",
                    "Predictable 10% moves.",
                ], "Volatility spikes, liquidity stress, and sector-level dispersion (energy, defense, shipping)."),
                ("Pre-defined 'first actions' for scenarios reduce:", [
                    "Behavioral error by removing in-the-moment discretion under stress.",
                    "Returns in calm markets.",
                    "Overall dividend yield.",
                    "Fund expense ratios.",
                ], "Behavioral error by removing in-the-moment discretion under stress."),
                ("An earnings season with multiple guide-downs typically signals:", [
                    "Emerging fundamental weakness that may extend beyond a single sector.",
                    "Guaranteed bull market.",
                    "Permanent margin compression in all stocks.",
                    "No macro signal at all.",
                ], "Emerging fundamental weakness that may extend beyond a single sector."),
                ("A diversifying sleeve in a macro-aware portfolio typically contains:", [
                    "Assets whose drivers differ from the core (e.g., short-duration bonds, commodities, cash).",
                    "Only more US large-cap growth names.",
                    "Triple-leveraged ETFs.",
                    "A single-sector ETF.",
                ], "Assets whose drivers differ from the core (e.g., short-duration bonds, commodities, cash)."),
                ("'Macro overrides micro' is a principle most useful when:", [
                    "A regime change is clearly underway and most individual stock setups are swamped by broad factor moves.",
                    "Nothing is changing.",
                    "Single-name earnings are the only driver.",
                    "Macro variables are flat.",
                ], "A regime change is clearly underway and most individual stock setups are swamped by broad factor moves."),
                ("Why is reading the yield curve useful for multi-asset portfolios?", [
                    "It reflects market expectations of growth, inflation, and policy paths simultaneously.",
                    "It predicts the next day's single-stock move.",
                    "It measures credit quality only.",
                    "It has no market information.",
                ], "It reflects market expectations of growth, inflation, and policy paths simultaneously."),
                ("During a severe liquidity event, the most durable portfolio behavior is to:", [
                    "Execute pre-planned rebalancing and risk rules rather than liquidate at the worst prices.",
                    "Abandon the plan and sell whatever is liquid.",
                    "Triple equity exposure.",
                    "Ignore the event entirely.",
                ], "Execute pre-planned rebalancing and risk rules rather than liquidate at the worst prices."),
                ("A strong end-of-course macro habit is to:", [
                    "Write and rehearse 3–4 scenario playbooks and review them quarterly as conditions evolve.",
                    "Predict the single most likely macro outcome and commit fully.",
                    "Avoid reading any macro data.",
                    "Trade every CPI release.",
                ], "Write and rehearse 3–4 scenario playbooks and review them quarterly as conditions evolve."),
            ],
        })

    return plan


def _lesson_content_for_module(module_title, module_description, week_number):
    if week_number == 1 and module_title.lower().startswith("introduction to investing"):
        return MODULE_1_CUSTOM_ETEXT

    plan = _module_teaching_plan(module_title, module_description)
    terms_block = "\n".join([f"- **{term}:** {meaning}" for term, meaning in plan["core_terms"]])
    walkthrough_block = "\n".join([f"{idx}. {step}" for idx, step in enumerate(plan["walkthrough"], start=1)])
    deep_dive_block = "\n\n".join(plan.get("deep_dive_paragraphs") or [])
    worked_example = plan.get("worked_example") or ""

    return (
        f"## Week {week_number} eText: {module_title}\n\n"
        f"_Curriculum content version: {CURRICULUM_CONTENT_VERSION}_\n\n"
        f"{module_description} {plan['hook']}\n\n"
        "You are not being graded on bold predictions; you are being graded on decision quality you can repeat. "
        "In this course, strong investing means a clear thesis, measurable evidence, and disciplined risk controls.\n\n"
        "### Core concepts you need to own\n"
        f"{terms_block}\n\n"
        "### Deep dive\n"
        f"{deep_dive_block}\n\n"
        "### Why this matters in your simulator account\n"
        f"{plan['scenario']}\n\n"
        "### Worked example\n"
        f"{worked_example}\n\n"
        "### Step-by-step decision workflow\n"
        f"{walkthrough_block}\n\n"
        "### Frequent mistake to avoid\n"
        f"{plan['likely_confusion']} Write your thesis, size, and invalidation *before* execution so your process does not get rewritten by emotion.\n\n"
        "### Quant toolkit\n"
        "- **Holding period return:** (Ending Value - Beginning Value + Cash Flows) / Beginning Value\n"
        "- **Portfolio weight:** Position Market Value / Total Portfolio Value\n"
        "- **Contribution to return:** Position Weight × Position Return\n"
        "- **Excess return vs benchmark:** Portfolio Return - Benchmark Return\n"
        "- **Concentration check (Top-3 weight):** Sum of top 3 position weights\n"
        "Every number needs interpretation: What does it imply for risk, and what action follows?\n\n"
        "### Exit ticket\n"
        f"{plan['application_prompt']}"
    )


def _render_inline_markdown_to_html(text):
    escaped = html.escape(str(text))
    return re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)


def _lesson_content_html(lesson_content):
    if not lesson_content:
        return ""

    lines = str(lesson_content).splitlines()
    html_parts = []
    paragraph_buffer = []
    list_items = []

    def flush_paragraph():
        if not paragraph_buffer:
            return
        text = " ".join(paragraph_buffer).strip()
        if text:
            html_parts.append(f"<p>{_render_inline_markdown_to_html(text)}</p>")
        paragraph_buffer.clear()

    def flush_list():
        if not list_items:
            return
        html_parts.append("<ul>")
        for item in list_items:
            html_parts.append(f"<li>{_render_inline_markdown_to_html(item)}</li>")
        html_parts.append("</ul>")
        list_items.clear()

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            flush_paragraph()
            flush_list()
            continue

        heading_match = re.match(r"^(#{1,6})\s+(.*)$", line)
        if heading_match:
            flush_paragraph()
            flush_list()
            level = len(heading_match.group(1))
            heading_text = heading_match.group(2).strip()
            html_parts.append(f"<h{level}>{_render_inline_markdown_to_html(heading_text)}</h{level}>")
            continue

        bullet_match = re.match(r"^-\s+(.*)$", line)
        if bullet_match:
            flush_paragraph()
            list_items.append(bullet_match.group(1).strip())
            continue

        flush_list()
        paragraph_buffer.append(line)

    flush_paragraph()
    flush_list()
    return "\n".join(html_parts)


def _assignment_content_for_module(module_title):
    if module_title == "Week 1: Introduction to Investing and Markets":
        return {
            "instructions": (
                "Week 1 Assignment (20 pts)\n\n"
                "Getting Started in the Simulator"
            ),
            "questions": [
                {
                    "id": "a1",
                    "kind": "quantitative",
                    "points": 10,
                    "prompt": "Part 1: First Trades (Quantitative – 10 pts)",
                    "sections": [
                        {"id": "a", "instruction": "Place the following trades in the simulator:"},
                        {"id": "b", "instruction": "$10,000 in SPY"},
                        {"id": "c", "instruction": "$10,000 in BIL"},
                        {"id": "d", "instruction": "$5,000 in QQQ"},
                        {"id": "e", "instruction": "Then answer:"},
                        {"id": "f", "instruction": "What was your cost basis for each position?"},
                        {"id": "g", "instruction": "What percentage of your portfolio does each position represent?"},
                        {"id": "h", "instruction": "(Show your math.)"},
                    ],
                },
                {
                    "id": "a2",
                    "kind": "qualitative",
                    "points": 10,
                    "prompt": "Part 2: Add 2 Stocks (Qualitative – 10 pts)",
                    "sections": [
                        {"id": "a", "instruction": "Now add at least 2 individual stocks to your portfolio."},
                        {"id": "b", "instruction": "In 1–2 paragraphs, explain:"},
                        {"id": "c", "instruction": "What stocks you chose and why"},
                        {"id": "d", "instruction": "What changed in your portfolio after adding them (risk, diversification, concentration, etc.)"},
                        {"id": "e", "instruction": "How this felt compared to just holding ETFs"},
                        {"id": "f", "instruction": "You don’t need to be “right”—just show your thinking."},
                    ],
                },
            ],
            "rubricHints": [
                "Accurate calculations for cost basis and portfolio weights",
                "Clear quantitative work with formulas and intermediate values",
                "Thoughtful explanation of stock selection and portfolio changes",
                "Reflection quality: risk, diversification, and decision process",
            ],
        }

    plan = _module_teaching_plan(module_title, "")
    q1_sections = [
        {"id": chr(97 + i), "instruction": instruction}
        for i, instruction in enumerate(plan["assignment_q1"])
    ]
    q2_sections = [
        {"id": chr(97 + i), "instruction": instruction}
        for i, instruction in enumerate(plan["assignment_q2"])
    ]

    return {
        "instructions": (
            f"Complete both 10-point questions for {module_title}. Anchor every claim in simulator evidence (positions, timestamps, weights, returns, and benchmark context). "
            "Show formulas and intermediate values for quantitative work, state assumptions explicitly, and connect conclusions to this module's eText concepts."
        ),
        "questions": [
            {
                "id": "a1",
                "kind": "qualitative",
                "points": 10,
                "prompt": "Question 1 (10 points) - Trade Decision Audit and Risk Control",
                "sections": q1_sections,
            },
            {
                "id": "a2",
                "kind": "quantitative",
                "points": 10,
                "prompt": "Question 2 (10 points) - Portfolio Analytics and Performance Attribution",
                "sections": q2_sections,
            },
        ],
        "rubricHints": [
            "Concept mastery and accurate use of module vocabulary",
            "Specific evidence quality from simulator records",
            "Correctness, transparency, and interpretation of quantitative work",
            "Decision-quality insight: tradeoffs, risk control, and process improvement",
        ],
    }


def _expand_legacy_assignment_content(module_title, assignment_type, content_json):
    assignment_kind = (assignment_type or "").lower()
    if assignment_kind not in ("assignment", "written_assignment"):
        return content_json

    if not isinstance(content_json, dict):
        return content_json

    questions = content_json.get("questions")
    if not isinstance(questions, list) or len(questions) != 2:
        return content_json

    has_sections = all(isinstance(q, dict) and isinstance(q.get("sections"), list) and len(q.get("sections")) > 0 for q in questions)
    if has_sections:
        return content_json

    prompts = [str((q or {}).get("prompt", "")).strip().lower() for q in questions if isinstance(q, dict)]
    looks_like_week1_legacy = (
        len(prompts) == 2
        and "first trades" in prompts[0]
        and "add 2 stocks" in prompts[1]
    )
    if not looks_like_week1_legacy:
        return content_json

    return _assignment_content_for_module(module_title)


def _format_section_block(sections):
    if not isinstance(sections, list):
        return ""
    lines = []
    for section in sections:
        if not isinstance(section, dict):
            continue
        sid = (section.get("id") or "").strip()
        instruction = (section.get("instruction") or "").strip()
        if sid and instruction:
            lines.append(f"{sid}. {instruction}")
    return "\n".join(lines)


def _inline_assignment_sections_for_display(content_json):
    # The student-facing UI renders only question.prompt and ignores the sections array,
    # which hides the lettered sub-instructions authored on the server. Inline them into
    # the prompt text so the existing renderer surfaces them. Idempotent so repeat calls,
    # or content where sections have already been folded in, do not double up.
    if not isinstance(content_json, dict):
        return content_json
    questions = content_json.get("questions")
    if not isinstance(questions, list) or not questions:
        return content_json
    new_questions = []
    changed = False
    for question in questions:
        if not isinstance(question, dict):
            new_questions.append(question)
            continue
        prompt = question.get("prompt")
        sections = question.get("sections")
        block = _format_section_block(sections)
        if not block or not isinstance(prompt, str):
            new_questions.append(question)
            continue
        if prompt.rstrip().endswith(block):
            new_questions.append(question)
            continue
        new_question = dict(question)
        new_question["prompt"] = prompt.rstrip() + "\n\n" + block
        new_questions.append(new_question)
        changed = True
    if not changed:
        return content_json
    new_content = dict(content_json)
    new_content["questions"] = new_questions
    return new_content


def _serialize_module_assignment(assignment):
    raw_content = assignment.content_json
    if (assignment.type or "").lower() in ("assignment", "written_assignment"):
        display_content = _inline_assignment_sections_for_display(raw_content)
    else:
        display_content = raw_content
    return {
        "assignmentId": assignment.id,
        "type": assignment.type,
        "title": assignment.title,
        "points": assignment.points,
        "content": display_content,
        "content_json": display_content,
        "answer_key_json": assignment.answer_key_json,
    }


def _strip_inlined_sections_from_prompt(prompt, sections):
    block = _format_section_block(sections)
    if not block or not isinstance(prompt, str):
        return prompt
    stripped = prompt.rstrip()
    if stripped.endswith(block):
        return stripped[: -len(block)].rstrip()
    return prompt


_GENERIC_QUIZ_BANK = [
    ("Which statement best reflects durable decision quality?", [
        "Judge decisions by process quality and evidence, not just outcome.",
        "Chase the highest recent return with full size.",
        "Avoid benchmarks because they reduce confidence.",
        "Rebuild your rules after each trade outcome.",
    ], "Judge decisions by process quality and evidence, not just outcome."),
    ("Expected return is best described as:", [
        "A probability-based estimate rather than a certainty.",
        "The same as last month's realized return.",
        "An outcome guaranteed by the broker.",
        "Only relevant for short-term traders.",
    ], "A probability-based estimate rather than a certainty."),
    ("Why is documenting an invalidation rule before entry important?", [
        "It prevents post-hoc rationalization and improves risk discipline.",
        "It guarantees every trade will be profitable.",
        "It removes the need for position sizing.",
        "It replaces the need for a thesis.",
    ], "It prevents post-hoc rationalization and improves risk discipline."),
    ("If your portfolio returned 6% and benchmark returned 4%, excess return is:", ["2%", "10%", "-2%", "4%"], "2%"),
    ("Contribution to return is best interpreted as:", [
        "How much each position added/subtracted to total portfolio return.",
        "The number of trades placed during the week.",
        "Only the return of your largest holding.",
        "A measure that ignores position weights.",
    ], "How much each position added/subtracted to total portfolio return."),
    ("A portfolio has top-3 weights of 18%, 16%, and 11%. Top-3 concentration is:", ["45%", "29%", "18%", "11%"], "45%"),
    ("Which response best demonstrates high-quality trade review?", [
        "Compare thesis vs outcome, identify process gaps, and define one concrete upgrade.",
        "Focus only on P&L and skip risk analysis.",
        "Blame market noise for all underperformance.",
        "Double size after any winning trade.",
    ], "Compare thesis vs outcome, identify process gaps, and define one concrete upgrade."),
    ("Which is the best benchmark selection principle?", [
        "Use a benchmark aligned to your portfolio's primary exposure.",
        "Always use cash as the benchmark.",
        "Use the highest returning ETF each week.",
        "Never use benchmarks for concentrated portfolios.",
    ], "Use a benchmark aligned to your portfolio's primary exposure."),
    ("Which is the most disciplined first action after a strong week?", [
        "Re-check thesis evidence and position limits before increasing risk.",
        "Increase every position equally to avoid regret.",
        "Ignore valuation and focus only on momentum.",
        "Wait for social confirmation before deciding.",
    ], "Re-check thesis evidence and position limits before increasing risk."),
    ("Process quality is most visible in which artifact?", [
        "A written thesis, sizing rule, and invalidation point recorded before execution.",
        "A post-hoc explanation of why the trade 'had to work'.",
        "A single price target with no horizon.",
        "Social-media sentiment scores.",
    ], "A written thesis, sizing rule, and invalidation point recorded before execution."),
    ("Position weight is defined as:", [
        "Position market value divided by total portfolio value.",
        "Number of shares held.",
        "Dividend yield of the position.",
        "Beta of the position.",
    ], "Position market value divided by total portfolio value."),
    ("A disciplined investor's horizon determines:", [
        "How much volatility can be tolerated on the way to the objective.",
        "Tax rate on long-term gains.",
        "Exchange trading hours.",
        "Nothing relevant to decisions.",
    ], "How much volatility can be tolerated on the way to the objective."),
    ("Which is the best use of a trade log?", [
        "An objective record of decisions that reveals behavioral and analytical patterns over time.",
        "A collection of wins only.",
        "An empty notebook.",
        "A replacement for the broker statement.",
    ], "An objective record of decisions that reveals behavioral and analytical patterns over time."),
    ("Excess return over a benchmark is most fairly interpreted when:", [
        "Compared against the benchmark that matches the portfolio's exposure and horizon.",
        "Measured against cash only.",
        "Measured against yesterday's close.",
        "Measured against a leveraged ETF.",
    ], "Compared against the benchmark that matches the portfolio's exposure and horizon."),
    ("Which behavior most undermines risk-adjusted return?", [
        "Panic-selling during drawdowns without a pre-committed rule.",
        "Rebalancing to target weights.",
        "Tracking a process journal.",
        "Using broad low-cost ETFs.",
    ], "Panic-selling during drawdowns without a pre-committed rule."),
    ("Why are correlations less reliable in crises?", [
        "Risk-asset correlations often rise toward +1, reducing diversification exactly when it is most needed.",
        "Correlations fall to zero in crises.",
        "Crises have no effect on correlations.",
        "Correlations are computed only for bonds.",
    ], "Risk-asset correlations often rise toward +1, reducing diversification exactly when it is most needed."),
    ("A 'starter position' sizing convention is typically:", [
        "Around 2–3% of the portfolio for lower-conviction ideas that still merit exposure.",
        "40% of the portfolio.",
        "Exactly 10% of the portfolio always.",
        "The entire portfolio.",
    ], "Around 2–3% of the portfolio for lower-conviction ideas that still merit exposure."),
    ("A position size of 5% on a stock with a 20% expected drawdown implies:", [
        "About a 1% portfolio-level impact in that drawdown, all else equal.",
        "A 5% portfolio-level loss.",
        "A 20% portfolio-level loss.",
        "Zero portfolio-level impact.",
    ], "About a 1% portfolio-level impact in that drawdown, all else equal."),
    ("Which pair best captures 'discipline' in this course?", [
        "Pre-committed rules plus honest post-trade review.",
        "Rapid turnover plus high conviction.",
        "Social-media signals plus leverage.",
        "Single-name concentration plus no stop.",
    ], "Pre-committed rules plus honest post-trade review."),
    ("The primary goal of this course is to:", [
        "Build a repeatable investing process whose quality is independent of any single outcome.",
        "Maximize turnover.",
        "Pick the best stock every week.",
        "Predict macro releases.",
    ], "Build a repeatable investing process whose quality is independent of any single outcome."),
]


def _quiz_content_for_module(module_title, question_count=20):
    plan = _module_teaching_plan(module_title, "")
    # Prefer the per-module 20-unique-stem bank when available (weeks 2-10 and final exam).
    # Fall back to the generic bank (also 20 unique stems) so every assignment has distinct questions.
    module_bank = plan.get("quiz_bank") if isinstance(plan, dict) else None
    bank = module_bank if (isinstance(module_bank, list) and len(module_bank) >= question_count) else _GENERIC_QUIZ_BANK

    questions = []
    answer_key = {}
    for idx in range(question_count):
        stem, choices, correct = bank[idx % len(bank)]
        qid = f"q{idx + 1}"
        questions.append({"id": qid, "prompt": stem, "choices": list(choices)})
        answer_key[qid] = correct

    return {
        "instructions": f"{module_title} quiz. Select one best answer for each question based on this week's eText.",
        "questions": questions,
    }, {"questions": answer_key}


def _final_exam_content(topics, question_count=20):
    exam_content, exam_answer_key = _quiz_content_for_module(
        "Final Cumulative Exam",
        question_count=question_count,
    )
    exam_content["instructions"] = (
        "Cumulative final exam. Questions span all modules. Select one best answer per question."
    )
    exam_content["coveredModules"] = [topic[0] for topic in topics]
    return exam_content, exam_answer_key


def generate_curriculum_for_competition(competition_id, total_weeks, start_date, end_date, overwrite=False):
    competition = db.session.get(Competition, competition_id)
    if not competition:
        raise ValueError("Competition not found.")

    curriculum = Curriculum.query.filter_by(competition_id=competition_id).first()
    if curriculum and not overwrite and CurriculumModule.query.filter_by(curriculum_id=curriculum.id).first():
        raise ValueError("Curriculum already generated for this competition.")

    if curriculum and overwrite:
        module_ids = [m.id for m in CurriculumModule.query.filter_by(curriculum_id=curriculum.id).all()]
        if module_ids:
            CurriculumSubmission.query.filter(CurriculumSubmission.assignment_id.in_(
                db.session.query(CurriculumAssignment.id).filter(CurriculumAssignment.module_id.in_(module_ids))
            )).delete(synchronize_session=False)
            CurriculumAssignment.query.filter(CurriculumAssignment.module_id.in_(module_ids)).delete(synchronize_session=False)
        CurriculumModule.query.filter_by(curriculum_id=curriculum.id).delete(synchronize_session=False)
    if not curriculum:
        curriculum = Curriculum(
            competition_id=competition_id,
            enabled=True,
            total_weeks=total_weeks,
            start_date=start_date,
            end_date=end_date,
        )
        db.session.add(curriculum)
        db.session.flush()
    else:
        curriculum.enabled = True
        curriculum.total_weeks = total_weeks
        curriculum.start_date = start_date
        curriculum.end_date = end_date

    topics = _build_curriculum_topics(total_weeks)
    schedule = _build_module_schedule(total_weeks, start_date, end_date)
    previous_module_id = None
    for week in range(1, total_weeks + 1):
        title, description = topics[week - 1]
        unlock_date, due_date = schedule[week - 1]
        module = CurriculumModule(
            curriculum_id=curriculum.id,
            week_number=week,
            title=f"Week {week}: {title}",
            description=description,
            lesson_content=_lesson_content_for_module(title, description, week),
            unlock_date=unlock_date,
            due_date=due_date,
            prerequisite_module_id=previous_module_id,
            passing_threshold=70.0,
        )
        db.session.add(module)
        db.session.flush()
        previous_module_id = module.id

        quiz_content, quiz_answer_key = _quiz_content_for_module(module.title, question_count=20)
        db.session.add(CurriculumAssignment(
            module_id=module.id,
            type="quiz",
            title=f"{module.title} Quiz",
            content_json=quiz_content,
            answer_key_json=quiz_answer_key,
            points=20,
        ))

        db.session.add(CurriculumAssignment(
            module_id=module.id,
            type="assignment",
            title=f"{module.title} Applied Assignment",
            content_json=_assignment_content_for_module(module.title),
            answer_key_json=None,
            points=20,
        ))

        if week == total_weeks:
            exam_content, exam_answer_key = _final_exam_content(topics, question_count=20)
            db.session.add(CurriculumAssignment(
                module_id=module.id,
                type="exam",
                title="Final Cumulative Exam",
                content_json=exam_content,
                answer_key_json=exam_answer_key,
                points=30,
            ))

    db.session.flush()
    return curriculum


def _delete_curriculum_for_competition(competition_id):
    curriculum = Curriculum.query.filter_by(competition_id=competition_id).first()
    if not curriculum:
        return

    module_ids = [m.id for m in CurriculumModule.query.filter_by(curriculum_id=curriculum.id).all()]
    if module_ids:
        assignment_ids = [a.id for a in CurriculumAssignment.query.filter(CurriculumAssignment.module_id.in_(module_ids)).all()]
        if assignment_ids:
            CurriculumSubmission.query.filter(
                CurriculumSubmission.assignment_id.in_(assignment_ids)
            ).delete(synchronize_session=False)
        CurriculumAssignment.query.filter(
            CurriculumAssignment.module_id.in_(module_ids)
        ).delete(synchronize_session=False)
    CurriculumModule.query.filter_by(curriculum_id=curriculum.id).delete(synchronize_session=False)
    db.session.delete(curriculum)


def _is_competition_instructor(user, competition):
    return bool(user and competition and (user.is_admin or competition.created_by == user.id))


def _get_trade_participation_for_module(competition, module, user_id):
    if not competition or not module:
        return False, 0
    # Treat due_date as inclusive for the full calendar day.
    window_start = module.unlock_date
    window_end_exclusive = module.due_date + timedelta(days=1)
    trade_exists = db.session.query(TradeBlotterEntry.id).filter(
        TradeBlotterEntry.user_id == user_id,
        TradeBlotterEntry.account_context == f"competition:{competition.code}",
        TradeBlotterEntry.created_at >= window_start,
        TradeBlotterEntry.created_at < window_end_exclusive,
    ).first() is not None
    return trade_exists, (10 if trade_exists else 0)


def _build_module_grade_breakdown(competition, module, user_id, assignments, submission_by_assignment):
    quiz_assignment = next((a for a in assignments if a.type == "quiz"), None)
    written_assignment = next((a for a in assignments if a.type == "assignment"), None)

    quiz_submission = submission_by_assignment.get(quiz_assignment.id) if quiz_assignment else None
    quiz_eval = _evaluate_assignment_for_gradebook(quiz_assignment, quiz_submission) if quiz_assignment else None
    quiz_points = round(float(quiz_eval["pointsEarned"]), 2) if quiz_eval else 0.0
    quiz_possible = round(float(quiz_eval["pointsPossible"]), 2) if quiz_eval else 0.0
    quiz_percentage = quiz_eval["percentage"] if quiz_eval else None

    assignment_submission = submission_by_assignment.get(written_assignment.id) if written_assignment else None
    assignment_eval = _evaluate_assignment_for_gradebook(written_assignment, assignment_submission) if written_assignment else None
    question_rows = _submission_question_grade_rows(assignment_submission.id) if assignment_submission else []
    question_1_score = assignment_submission.question_1_score if assignment_submission and assignment_submission.question_1_score is not None else None
    question_2_score = assignment_submission.question_2_score if assignment_submission and assignment_submission.question_2_score is not None else None
    assignment_total = round(float(assignment_eval["pointsEarned"]), 2) if assignment_eval else 0.0
    assignment_possible = round(float(assignment_eval["pointsPossible"]), 2) if assignment_eval else 0.0

    trade_completed, trade_points = _get_trade_participation_for_module(competition, module, user_id)
    module_total = round(quiz_points + assignment_total + trade_points, 2)
    module_possible = round(quiz_possible + assignment_possible + 10.0, 2)
    module_percentage = round((module_total / module_possible * 100.0), 2) if module_possible else None
    return {
        "moduleId": module.id,
        "moduleWeek": module.week_number,
        "moduleTitle": module.title,
        "quiz": {
            "assignmentId": quiz_assignment.id if quiz_assignment else None,
            "score": quiz_points,
            "pointsPossible": quiz_possible,
            "percentage": quiz_percentage,
            "gradingStatus": quiz_eval["status"] if quiz_eval else "not_submitted",
            "submittedAt": quiz_submission.submitted_at.isoformat() if quiz_submission else None,
        },
        "writtenAssignment": {
            "assignmentId": written_assignment.id if written_assignment else None,
            "question1Score": question_1_score,
            "question2Score": question_2_score,
            "totalScore": assignment_total,
            "pointsPossible": assignment_possible,
            "percentage": assignment_eval["percentage"] if assignment_eval else None,
            "gradingStatus": assignment_eval["status"] if assignment_eval else "not_submitted",
            "submittedAt": assignment_submission.submitted_at.isoformat() if assignment_submission else None,
            "feedback": (assignment_submission.feedback_json or {}) if assignment_submission else {},
            "questionGrades": [
                {
                    "questionId": q.question_id,
                    "pointsAwarded": q.points_awarded,
                    "pointsPossible": q.points_possible,
                    "feedback": q.feedback,
                } for q in question_rows
            ],
        },
        "tradeParticipation": {
            "tradeCompleted": trade_completed,
            "tradePoints": trade_points,
            "pointsPossible": 10,
            "windowStart": module.unlock_date.isoformat(),
            "windowEnd": module.due_date.isoformat(),
        },
        "moduleTotalPoints": module_total,
        "modulePointsPossible": module_possible,
        "modulePercentage": module_percentage,
    }


def _is_module_in_grade_scope(module, now=None):
    if not module:
        return False
    effective_now = now or datetime.utcnow()
    return bool(module.unlock_date and module.unlock_date <= effective_now)


def _module_prerequisite_passed(curriculum, module, user_id):
    """Return True if the student has graded submissions meeting the module's prerequisite passing threshold.

    Criterion: the prerequisite module's quiz/exam submissions (auto-graded) average at or above
    the prerequisite's passing_threshold. Modules with no explicit prerequisite_module_id
    fall back to 'previous week number'.
    """
    if not module or not curriculum or user_id is None:
        return True
    prereq_id = getattr(module, 'prerequisite_module_id', None)
    prereq = None
    if prereq_id:
        prereq = db.session.get(CurriculumModule, prereq_id)
    else:
        # Implicit prereq: the prior week in the same curriculum.
        if module.week_number and module.week_number > 1:
            prereq = CurriculumModule.query.filter_by(
                curriculum_id=curriculum.id,
                week_number=module.week_number - 1,
            ).first()
    if not prereq:
        return True
    prereq_assignments = CurriculumAssignment.query.filter_by(module_id=prereq.id).all()
    gated_assignments = [a for a in prereq_assignments if a.type in ('quiz', 'exam')]
    if not gated_assignments:
        # No auto-graded item to gate on; treat as passed.
        return True
    threshold = float(getattr(prereq, 'passing_threshold', 70.0) or 70.0)
    for assignment in gated_assignments:
        submission = CurriculumSubmission.query.filter_by(
            assignment_id=assignment.id,
            user_id=user_id,
        ).first()
        if not submission or not submission.auto_graded:
            return False
        if (submission.percentage or 0.0) < threshold:
            return False
    return True


def _module_lock_state(curriculum, module, user_id, now=None, requester_user=None, competition=None):
    """Return a dict describing whether the module is locked for this student.

    Teachers/admins never see locked modules; they get prerequisiteMet=True and locked=False.
    """
    effective_now = now or datetime.utcnow()
    time_unlocked = bool(module and module.unlock_date and module.unlock_date <= effective_now)
    is_instructor = False
    if requester_user and competition:
        is_instructor = _is_competition_instructor(requester_user, competition)
    enforce = bool(curriculum and getattr(curriculum, 'enforce_prerequisites', False))
    prereq_passed = True
    if enforce and not is_instructor:
        prereq_passed = _module_prerequisite_passed(curriculum, module, user_id)
    # Locked if enforce is on AND (not time-unlocked OR prereq not passed), but never for instructors.
    if is_instructor:
        locked = False
    elif enforce:
        locked = (not time_unlocked) or (not prereq_passed)
    else:
        # Legacy behavior: modules with future unlock_date are still readable (back-compat).
        locked = False
    prereq_module_id = getattr(module, 'prerequisite_module_id', None)
    if not prereq_module_id and module and module.week_number and module.week_number > 1:
        implicit = CurriculumModule.query.filter_by(
            curriculum_id=curriculum.id if curriculum else None,
            week_number=module.week_number - 1,
        ).first() if curriculum else None
        prereq_module_id = implicit.id if implicit else None
    return {
        "locked": locked,
        "timeUnlocked": time_unlocked,
        "prerequisiteMet": prereq_passed,
        "prerequisiteModuleId": prereq_module_id,
        "enforcePrerequisites": enforce,
        "passingThreshold": float(getattr(module, 'passing_threshold', 70.0) or 70.0) if module else None,
    }


def _module_grade_row(module, assignments, submission_by_assignment):
    assignment_rows = []
    points_earned = 0.0
    points_possible = 0.0
    has_pending_manual_grade = False
    graded_item_count = 0

    for assignment in assignments:
        submission = submission_by_assignment.get(assignment.id)
        evaluated = _evaluate_assignment_for_gradebook(assignment, submission)
        status = evaluated.get("status")
        earned = float(evaluated.get("pointsEarned", 0.0) or 0.0)
        possible = float(evaluated.get("pointsPossible", 0.0) or 0.0)
        include_in_denominator = status == "graded"
        if status == "pending_grade":
            has_pending_manual_grade = True
        if include_in_denominator:
            graded_item_count += 1
            points_earned += earned
            points_possible += possible

        assignment_rows.append({
            "assignmentId": assignment.id,
            "assignmentType": assignment.type,
            "title": assignment.title,
            "pointsEarned": round(earned, 2),
            "pointsPossible": round(possible, 2),
            "percentage": round(float(evaluated.get("percentage")), 2) if evaluated.get("percentage") is not None else None,
            "gradingStatus": status,
            "isManuallyGradable": bool(evaluated.get("isManuallyGradable")),
            "submittedAt": submission.submitted_at.isoformat() if submission else None,
            "submissionId": submission.id if submission else None,
        })

    module_percentage = round((points_earned / points_possible * 100.0), 2) if points_possible else None
    if graded_item_count > 0:
        module_status = "graded"
    elif has_pending_manual_grade:
        module_status = "pending_grade"
    else:
        module_status = "not_started"
    return {
        "module_id": module.id,
        "moduleId": module.id,
        "week": module.week_number,
        "moduleWeek": module.week_number,
        "title": module.title,
        "points_earned": round(points_earned, 2),
        "pointsEarned": round(points_earned, 2),
        "points_possible": round(points_possible, 2),
        "pointsPossible": round(points_possible, 2),
        "percentage": module_percentage,
        "letter": _grade_letter_from_percentage(module_percentage),
        "letterGrade": _grade_letter_from_percentage(module_percentage),
        "status": module_status,
        "items": assignment_rows,
    }


def _compute_grade_summary(competition_id, user_id):
    curriculum = Curriculum.query.filter_by(competition_id=competition_id, enabled=True).first()
    if not curriculum:
        return None
    competition = db.session.get(Competition, competition_id)
    modules = CurriculumModule.query.filter_by(curriculum_id=curriculum.id).order_by(CurriculumModule.week_number.asc()).all()
    module_ids = [m.id for m in modules]
    assignments = []
    if module_ids:
        assignments = CurriculumAssignment.query.filter(CurriculumAssignment.module_id.in_(module_ids)).all()
    assignment_map = {a.id: a for a in assignments}
    submission_rows = []
    if assignment_map:
        submission_rows = CurriculumSubmission.query.filter(
            CurriculumSubmission.user_id == user_id,
            CurriculumSubmission.assignment_id.in_(assignment_map.keys())
        ).all()
    submission_by_assignment = {s.assignment_id: s for s in submission_rows}
    module_breakdown = []
    all_module_rows = []
    total_possible = 0.0
    total_scored = 0.0
    total_items = 0
    completed_items = 0
    additional_items = []
    for module in modules:
        module_assignments = [a for a in assignments if a.module_id == module.id]
        module_breakdown.append(_build_module_grade_breakdown(competition, module, user_id, module_assignments, submission_by_assignment))
        module_grade = _module_grade_row(module, module_assignments, submission_by_assignment)
        all_module_rows.append(module_grade)
        total_scored += module_grade["pointsEarned"]
        total_possible += module_grade["pointsPossible"]
        total_items += len(module_grade["items"])
        completed_items += len([row for row in module_grade["items"] if row.get("gradingStatus") == "graded"])

        if _is_module_in_grade_scope(module):
            total_items += 1
            trade_completed, _trade_points = _get_trade_participation_for_module(competition, module, user_id)
            if trade_completed:
                completed_items += 1

        for assignment in module_assignments:
            if assignment.type in ("quiz", "assignment"):
                continue
            sub = submission_by_assignment.get(assignment.id)
            additional_items.append({
                "moduleId": module.id,
                "moduleWeek": module.week_number,
                "assignmentId": assignment.id,
                "assignmentType": assignment.type,
                "title": assignment.title,
                "pointsPossible": assignment.points,
                "pointsEarned": round(sub.score, 2) if sub and sub.score is not None else 0.0,
                "percentage": round(sub.percentage, 2) if sub and sub.percentage is not None else None,
                "submittedAt": sub.submitted_at.isoformat() if sub else None,
                "submissionStatus": "graded" if (sub and sub.auto_graded) else ("submitted" if sub else "missing"),
            })
    overall_pct = round((total_scored / total_possible * 100.0), 2) if total_possible else None
    letter = _grade_letter_from_percentage(overall_pct)
    progress_pct = round((completed_items / total_items * 100.0), 2) if total_items else 0.0
    overall_summary = {
        "scope": "all_modules_graded_items",
        "points_earned": round(total_scored, 2),
        "points_possible": round(total_possible, 2),
        "percentage": overall_pct,
        "letter": letter,
        "completed_items": completed_items,
        "total_items": total_items,
        "progress_percentage": progress_pct,
    }
    return {
        "curriculumId": curriculum.id,
        "curriculum_id": curriculum.id,
        "competitionId": competition_id,
        "competition_id": competition_id,
        "userId": user_id,
        "user_id": user_id,
        "totalPointsEarned": round(total_scored, 2),
        "totalPointsPossible": round(total_possible, 2),
        "percentage": overall_pct,
        "letterGrade": letter,
        "completedItems": completed_items,
        "totalItems": total_items,
        "progressPercentage": progress_pct,
        "moduleGrades": module_breakdown,
        "items": all_module_rows,
        "additionalAssessments": additional_items,
        "grade_summary_overall": overall_summary,
        "gradeSummaryOverall": overall_summary,
        "grade_summary_by_module": all_module_rows,
        "gradeSummaryByModule": all_module_rows,
    }


def _grade_letter_from_percentage(overall_pct):
    if overall_pct is None:
        return "N/A"
    if overall_pct >= 90:
        return "A"
    if overall_pct >= 80:
        return "B"
    if overall_pct >= 70:
        return "C"
    if overall_pct >= 60:
        return "D"
    return "F"


def _written_submission_status(submission):
    if not submission:
        return "not_submitted"
    if SubmissionQuestionGrade.query.filter_by(submission_id=submission.id).first():
        return "graded"
    pending_flag = bool((submission.feedback_json or {}).get("pendingManualGrade", False))
    if submission.graded_at is not None:
        return "graded"
    if pending_flag:
        return "pending_grade"
    if submission.assignment_total_score is not None:
        return "graded"
    return "pending_grade"

def _submission_question_grade_rows(submission_id):
    if not submission_id:
        return []
    return SubmissionQuestionGrade.query.filter_by(submission_id=submission_id).all()


def _computed_submission_grade(submission, assignment):
    if not submission:
        return {
            "pointsEarned": 0.0,
            "pointsPossible": float(assignment.points or 0.0),
            "percentage": None,
            "status": "not_submitted",
        }
    question_rows = _submission_question_grade_rows(submission.id)
    if question_rows:
        points_earned = round(sum(float(r.points_awarded or 0.0) for r in question_rows), 2)
        points_possible = round(sum(float(r.points_possible or 0.0) for r in question_rows), 2)
        percentage = round((points_earned / points_possible * 100.0), 2) if points_possible > 0 else None
        return {
            "pointsEarned": points_earned,
            "pointsPossible": points_possible,
            "percentage": percentage,
            "status": "graded",
        }

    if assignment.type in ("assignment", "written_assignment"):
        status = _written_submission_status(submission)
        points_earned = round(float(submission.assignment_total_score if submission.assignment_total_score is not None else 0.0), 2)
        points_possible = float(assignment.points or 0.0)
        percentage = round((points_earned / points_possible * 100.0), 2) if points_possible > 0 and status == "graded" else None
        return {
            "pointsEarned": points_earned,
            "pointsPossible": points_possible,
            "percentage": percentage,
            "status": status,
        }

    points_earned = round(float(submission.score or 0.0), 2)
    points_possible = float(assignment.points or 0.0)
    has_answer_key = bool(_quiz_answer_key_map(assignment))
    assignment_type = (assignment.type or "").lower()
    should_treat_as_graded = submission.auto_graded or (assignment_type in ("quiz", "exam") and not has_answer_key)
    percentage = round(float(submission.percentage), 2) if submission.percentage is not None else (
        round((points_earned / points_possible * 100.0), 2) if points_possible > 0 and should_treat_as_graded else None
    )
    return {
        "pointsEarned": points_earned,
        "pointsPossible": points_possible,
        "percentage": percentage if should_treat_as_graded else None,
        "status": "graded" if should_treat_as_graded else "submitted",
    }


def _evaluate_assignment_for_gradebook(assignment, submission):
    computed = _computed_submission_grade(submission, assignment)
    return {
        "status": computed["status"],
        "pointsEarned": computed["pointsEarned"],
        "pointsPossible": computed["pointsPossible"],
        "percentage": computed["percentage"],
        "isManuallyGradable": assignment.type in ("assignment", "written_assignment"),
    }


def _build_teacher_grade_rows(competition, curriculum, member_ids):
    modules = CurriculumModule.query.filter_by(curriculum_id=curriculum.id).order_by(CurriculumModule.week_number.asc()).all()
    module_ids = [m.id for m in modules]
    assignments = CurriculumAssignment.query.filter(CurriculumAssignment.module_id.in_(module_ids)).all() if module_ids else []
    assignment_ids = [a.id for a in assignments]
    submissions = CurriculumSubmission.query.filter(
        CurriculumSubmission.competition_id == competition.id,
        CurriculumSubmission.user_id.in_(member_ids),
        CurriculumSubmission.assignment_id.in_(assignment_ids),
    ).all() if assignment_ids and member_ids else []

    submissions_by_user = {}
    grader_ids = set()
    for sub in submissions:
        submissions_by_user.setdefault(sub.user_id, {})[sub.assignment_id] = sub
        if sub.graded_by_user_id:
            grader_ids.add(sub.graded_by_user_id)
    graders_by_id = {u.id: u for u in User.query.filter(User.id.in_(grader_ids)).all()} if grader_ids else {}

    assignment_by_module = {}
    for assignment in assignments:
        assignment_by_module.setdefault(assignment.module_id, []).append(assignment)

    trade_count_rows = db.session.query(
        TradeBlotterEntry.user_id,
        func.count(TradeBlotterEntry.id)
    ).filter(
        TradeBlotterEntry.user_id.in_(member_ids),
        TradeBlotterEntry.account_context == f"competition:{competition.code}"
    ).group_by(TradeBlotterEntry.user_id).all() if member_ids else []
    trade_count_by_user = {row[0]: int(row[1]) for row in trade_count_rows}

    rows = {}
    for uid in member_ids:
        total_points_earned = 0.0
        total_points_possible = 0.0
        completed_quizzes = 0
        completed_assignments = 0
        total_curriculum_items = 0
        items = []
        module_rows = []

        for module in modules:
            module_assignments = assignment_by_module.get(module.id, [])
            scoped_submission_by_assignment = submissions_by_user.get(uid, {})
            module_rows.append(_module_grade_row(module, module_assignments, scoped_submission_by_assignment))

            if _is_module_in_grade_scope(module):
                total_curriculum_items += 1

            for assignment in assignment_by_module.get(module.id, []):
                total_curriculum_items += 1
                sub = submissions_by_user.get(uid, {}).get(assignment.id)
                evaluated = _evaluate_assignment_for_gradebook(assignment, sub)
                if evaluated["status"] == "graded":
                    total_points_earned += evaluated["pointsEarned"]
                    total_points_possible += evaluated["pointsPossible"]
                if assignment.type == "quiz" and sub:
                    completed_quizzes += 1
                if assignment.type in ("assignment", "written_assignment") and sub:
                    completed_assignments += 1

                grader = graders_by_id.get(sub.graded_by_user_id) if sub and sub.graded_by_user_id else None
                items.append({
                    "moduleId": module.id,
                    "moduleWeek": module.week_number,
                    "moduleTitle": module.title,
                    "assignmentId": assignment.id,
                    "assignmentType": assignment.type,
                    "title": assignment.title,
                    "pointsEarned": evaluated["pointsEarned"],
                    "pointsPossible": evaluated["pointsPossible"],
                    "percentage": evaluated["percentage"],
                    "gradingStatus": evaluated["status"],
                    "isManuallyGradable": evaluated["isManuallyGradable"],
                    "submittedAt": sub.submitted_at.isoformat() if sub else None,
                    "submissionId": sub.id if sub else None,
                    "submissionContent": sub.answers_json if sub else None,
                    "feedback": (sub.feedback_json or {}) if sub else {},
                    "rubricNotes": sub.rubric_notes if sub else None,
                    "gradedByUserId": sub.graded_by_user_id if sub else None,
                    "gradedByUsername": grader.username if grader else None,
                    "gradedAt": sub.graded_at.isoformat() if sub and sub.graded_at else None,
                })

        trade_completed_items = 0
        for module in modules:
            if not _is_module_in_grade_scope(module):
                continue
            trade_completed, _trade_points = _get_trade_participation_for_module(competition, module, uid)
            if trade_completed:
                trade_completed_items += 1

        percentage = round((total_points_earned / total_points_possible * 100.0), 2) if total_points_possible else None
        progress_numerator = completed_quizzes + completed_assignments + trade_completed_items
        progress_pct = round((progress_numerator / total_curriculum_items) * 100.0, 2) if total_curriculum_items else 0.0
        rows[uid] = {
            "curriculumPercentage": percentage,
            "percentage": percentage,
            "letterGrade": _grade_letter_from_percentage(percentage),
            "totalPointsEarned": round(total_points_earned, 2),
            "totalPointsPossible": round(total_points_possible, 2),
            "completedQuizzes": completed_quizzes,
            "completedAssignments": completed_assignments,
            "totalCurriculumItems": total_curriculum_items,
            "completedCurriculumItems": progress_numerator,
            "progressPercentage": progress_pct,
            "hasTrades": trade_count_by_user.get(uid, 0) > 0,
            "tradeCount": trade_count_by_user.get(uid, 0),
            "items": items,
            "grade_summary_overall": {
                "scope": "all_modules_graded_items",
                "points_earned": round(total_points_earned, 2),
                "points_possible": round(total_points_possible, 2),
                "percentage": percentage,
                "letter": _grade_letter_from_percentage(percentage),
                "completed_items": progress_numerator,
                "total_items": total_curriculum_items,
                "progress_percentage": progress_pct,
            },
            "grade_summary_by_module": module_rows,
        }
    return rows


def _resolve_curriculum_competition_id(raw_competition_id, requester_user_id=None):
    competition = db.session.get(Competition, raw_competition_id)
    competition_member = db.session.get(CompetitionMember, raw_competition_id)
    if (
        requester_user_id is not None
        and competition_member
        and competition_member.user_id == requester_user_id
    ):
        return competition_member.competition_id

    curriculum_for_competition = (
        Curriculum.query.filter_by(competition_id=competition.id).first()
        if competition
        else None
    )
    if competition and curriculum_for_competition:
        return competition.id

    if competition_member:
        return competition_member.competition_id

    competition_team = db.session.get(CompetitionTeam, raw_competition_id)
    if competition_team:
        return competition_team.competition_id

    return raw_competition_id


def _serialize_competition_identity(competition, requesting_user=None):
    curriculum = Curriculum.query.filter_by(competition_id=competition.id).first()
    curriculum_enabled = bool(curriculum and curriculum.enabled)
    is_instructor = _is_competition_instructor(requesting_user, competition)
    payload = {
        "id": competition.id,
        "competition_id": competition.id,
        "competitionId": competition.id,
        "code": competition.code,
        "competition_code": competition.code,
        "competitionCode": competition.code,
        "name": competition.name,
        "competition_name": competition.name,
        "competitionName": competition.name,
        "start_date": competition.start_date.isoformat() if competition.start_date else None,
        "end_date": competition.end_date.isoformat() if competition.end_date else None,
        "featured": competition.featured,
        "is_open": competition.is_open,
        "curriculum_enabled": curriculum_enabled,
        "curriculumEnabled": curriculum_enabled,
        "curriculum_id": None,
        "curriculumId": None,
        "curriculum_weeks": None,
        "curriculumWeeks": None,
        "curriculum_start_date": None,
        "curriculumStartDate": None,
        "curriculum_end_date": None,
        "curriculumEndDate": None,
        "is_instructor_for_competition": is_instructor,
    }
    if curriculum:
        payload.update({
            "curriculum_id": curriculum.id,
            "curriculumId": curriculum.id,
            "curriculum_weeks": curriculum.total_weeks,
            "curriculumWeeks": curriculum.total_weeks,
            "curriculum_start_date": curriculum.start_date.date().isoformat(),
            "curriculumStartDate": curriculum.start_date.date().isoformat(),
            "curriculum_end_date": curriculum.end_date.date().isoformat(),
            "curriculumEndDate": curriculum.end_date.date().isoformat(),
        })
    return payload


def _resolve_submission_competition_id(raw_competition_id):
    if raw_competition_id is None:
        return None
    try:
        parsed = int(raw_competition_id)
    except (TypeError, ValueError):
        return None
    return _resolve_curriculum_competition_id(parsed)


def _extract_quiz_selected_value(answer_row):
    if not isinstance(answer_row, dict):
        return None
    return _first_present(
        answer_row,
        "selectedChoice",
        "selected_choice",
        "selected",
        "answer",
        "value",
    )


def _normalize_submission_answers(assignment, answers):
    assignment_type = (assignment.type or "").lower()
    normalized_answers = []

    if isinstance(answers, dict):
        for raw_question_id, raw_value in answers.items():
            if assignment_type in ("quiz", "exam"):
                selected = raw_value
                if isinstance(raw_value, dict):
                    selected = _extract_quiz_selected_value(raw_value)
                normalized_answers.append({
                    "questionId": str(raw_question_id),
                    "selectedChoice": selected,
                })
            else:
                if isinstance(raw_value, dict) and isinstance(raw_value.get("parts"), list):
                    parts = []
                    for idx, part in enumerate(raw_value.get("parts"), start=1):
                        if isinstance(part, dict):
                            part_id = _first_present(part, "partId", "part_id") or f"part-{idx}"
                            part_response = _first_present(part, "response", "answer", "value")
                        else:
                            part_id = f"part-{idx}"
                            part_response = str(part)
                        parts.append({"partId": str(part_id), "response": part_response})
                    normalized_answers.append({"questionId": str(raw_question_id), "parts": parts})
                else:
                    response_text = _first_present(raw_value, "response", "answer", "value") if isinstance(raw_value, dict) else raw_value
                    normalized_answers.append({"questionId": str(raw_question_id), "response": response_text})
    elif isinstance(answers, list):
        for idx, answer_row in enumerate(answers, start=1):
            if not isinstance(answer_row, dict):
                raise ValueError("Each answer entry must be an object.")
            question_id = _first_present(answer_row, "questionId", "question_id", "id")
            if question_id in (None, ""):
                raise ValueError("Each answer entry must include questionId.")
            if assignment_type in ("quiz", "exam"):
                selected = _extract_quiz_selected_value(answer_row)
                if selected is None:
                    raise ValueError("Quiz answers must include selectedChoice (or alias selected/answer/value).")
                normalized_answers.append({"questionId": str(question_id), "selectedChoice": selected})
            else:
                parts = answer_row.get("parts")
                if parts is not None:
                    if not isinstance(parts, list) or not parts:
                        raise ValueError("Written multipart answers must include a non-empty parts array.")
                    normalized_parts = []
                    for p_idx, part in enumerate(parts, start=1):
                        if not isinstance(part, dict):
                            raise ValueError("Each parts entry must be an object with response.")
                        part_id = _first_present(part, "partId", "part_id") or f"part-{p_idx}"
                        part_response = _first_present(part, "response", "answer", "value")
                        if part_response is None:
                            raise ValueError("Each parts entry must include response.")
                        normalized_parts.append({"partId": str(part_id), "response": part_response})
                    normalized_answers.append({"questionId": str(question_id), "parts": normalized_parts})
                else:
                    response_text = _first_present(answer_row, "response", "answer", "value")
                    if response_text is None:
                        raise ValueError("Written answers must include response or parts[].")
                    normalized_answers.append({"questionId": str(question_id), "response": response_text})
    else:
        raise ValueError("answers must be an object map or an array of answer entries.")

    if not normalized_answers:
        raise ValueError("answers cannot be empty.")
    return {"answers": normalized_answers}




def _build_quiz_snapshot(assignment):
    """Capture the exact question order + answer key the student is about to see.

    Stored on CurriculumSubmission.question_order_json. Ensures grading uses the questions
    the student actually answered, even if assignment.content_json is edited mid-attempt.
    """
    if not assignment:
        return None
    content = assignment.content_json or {}
    questions = content.get("questions") if isinstance(content, dict) else None
    if not isinstance(questions, list):
        return None
    order = [str(q.get("id")) for q in questions if isinstance(q, dict) and q.get("id") is not None]
    key_raw = (assignment.answer_key_json or {}).get("questions") if isinstance(assignment.answer_key_json, dict) else None
    answer_key = {str(k): v for k, v in key_raw.items()} if isinstance(key_raw, dict) else {}
    return {
        "order": order,
        "answerKey": answer_key,
        "assignmentId": assignment.id,
        "capturedAt": datetime.utcnow().isoformat(),
    }


def _snapshot_answer_key(submission, assignment):
    """Return the answer key to grade against: the per-submission snapshot if present,
    else the current assignment answer_key_json. This is the core of Issue #2's fix.
    """
    snapshot = getattr(submission, 'question_order_json', None) if submission else None
    if isinstance(snapshot, dict):
        key = snapshot.get("answerKey")
        if isinstance(key, dict) and key:
            return {str(k): v for k, v in key.items()}
    return _quiz_answer_key_map(assignment)


def _quiz_answer_key_map(assignment):
    if not assignment:
        return {}
    raw_questions = (assignment.answer_key_json or {}).get("questions")
    if not isinstance(raw_questions, dict):
        return {}
    return {str(qid): expected for qid, expected in raw_questions.items()}

def _quiz_answer_map_from_submission(answers_json):
    if isinstance(answers_json, dict) and isinstance(answers_json.get("answers"), list):
        mapped = {}
        for row in answers_json.get("answers") or []:
            if isinstance(row, dict):
                qid = row.get("questionId")
                selected = _first_present(row, "selectedChoice", "selected_choice", "selected", "answer", "value")
                if qid not in (None, "") and selected is not None:
                    mapped[str(qid)] = selected
        return mapped
    if isinstance(answers_json, dict):
        return {str(k): v for k, v in answers_json.items()}
    return {}

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
    # Prefer the quote-level change field when available. This can be more reliable than
    # "08. previous close" around corporate actions and prevents large incorrect day P&L swings.
    quote_change = global_quote.get("09. change")
    if quote_change is not None and str(quote_change).strip() != "":
        try:
            prev_close = current_price - float(quote_change)
        except (TypeError, ValueError):
            prev_close = float(global_quote.get("08. previous close") or 0.0)
    else:
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

    quote_change_value = global_quote.get("09. change")
    quote_change_percent = global_quote.get("10. change percent")
    if quote_change_percent is not None:
        quote_change_percent = quote_change_percent.replace("%", "")

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

    if quote_change_value is not None and quote_change_percent is not None:
        today_change_value = float(quote_change_value)
        today_change_percent = float(quote_change_percent)
    else:
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


def _account_display_name(account_type, competition_name=None, competition_code=None, team_name=None):
    if account_type == "global":
        return "Global Account"
    if account_type == "competition":
        return competition_name or competition_code
    if account_type == "team_competition":
        if team_name and (competition_name or competition_code):
            return f"{team_name} • {competition_name or competition_code}"
        return team_name or competition_name or competition_code
    return "Account"


VALID_ACCOUNT_TYPES = {"global", "competition", "team_competition"}


def _normalize_account_type(raw_account_type):
    if raw_account_type is None:
        return None
    normalized = str(raw_account_type).strip().lower()
    if normalized in {"competition individual", "competition_individual"}:
        return "competition"
    if normalized == "team":
        return "team_competition"
    return normalized


def _parse_snapshot_date(raw_date):
    if raw_date in (None, ""):
        return datetime.now(timezone.utc).date()
    if isinstance(raw_date, date):
        return raw_date
    try:
        return datetime.strptime(str(raw_date), "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("date must be in YYYY-MM-DD format")


def _validate_performance_payload(data):
    username = (data.get("username") or "").strip()
    account_id = str(data.get("account_id") or "").strip()
    account_type = _normalize_account_type(data.get("account_type"))

    if not username:
        return None, "username is required"
    if not account_id:
        return None, "account_id is required"
    if not account_type:
        return None, "account_type is required"
    if account_type not in VALID_ACCOUNT_TYPES:
        return None, "account_type is invalid"

    try:
        snapshot_date = _parse_snapshot_date(data.get("date"))
    except ValueError as exc:
        return None, str(exc)

    try:
        total_value = float(data.get("total_value"))
    except (TypeError, ValueError):
        return None, "total_value must be numeric"

    try:
        cash = float(data.get("cash"))
    except (TypeError, ValueError):
        return None, "cash must be numeric"

    total_pnl_raw = data.get("total_pnl")
    if total_pnl_raw in (None, ""):
        total_pnl = None
    else:
        try:
            total_pnl = float(total_pnl_raw)
        except (TypeError, ValueError):
            return None, "total_pnl must be numeric when provided"

    return {
        "username": username,
        "account_id": account_id,
        "account_type": account_type,
        "date": snapshot_date,
        "total_value": total_value,
        "cash": cash,
        "total_pnl": total_pnl,
    }, None


def _upsert_account_performance_snapshot_record(snapshot):
    statement = None
    if db.engine.dialect.name == 'postgresql':
        statement = text(
            """
            INSERT INTO account_performance_history
                (username, account_id, account_type, date, total_value, cash, total_pnl, updated_at)
            VALUES
                (:username, :account_id, :account_type, :date, :total_value, :cash, :total_pnl, NOW())
            ON CONFLICT (username, account_type, account_id, date)
            DO UPDATE SET
                total_value = EXCLUDED.total_value,
                cash = EXCLUDED.cash,
                total_pnl = EXCLUDED.total_pnl,
                updated_at = NOW()
            """
        )
    elif db.engine.dialect.name == 'sqlite':
        statement = text(
            """
            INSERT INTO account_performance_history
                (username, account_id, account_type, date, total_value, cash, total_pnl, updated_at)
            VALUES
                (:username, :account_id, :account_type, :date, :total_value, :cash, :total_pnl, CURRENT_TIMESTAMP)
            ON CONFLICT(username, account_type, account_id, date)
            DO UPDATE SET
                total_value = excluded.total_value,
                cash = excluded.cash,
                total_pnl = excluded.total_pnl,
                updated_at = CURRENT_TIMESTAMP
            """
        )
    else:
        raise RuntimeError('Unsupported database dialect for upsert')

    db.session.execute(statement, snapshot)


def _calculate_holdings_value_and_unrealized(holdings, price_getter):
    total_holdings_value = 0.0
    unrealized_pnl = 0.0
    for holding in holdings:
        try:
            price = price_getter(holding.symbol)
        except Exception:
            price = 0.0
        total_holdings_value += price * holding.quantity
        unrealized_pnl += (price - holding.buy_price) * holding.quantity
    return total_holdings_value, unrealized_pnl


def _generate_daily_account_snapshots(snapshot_date):
    snapshots = []
    cached_prices = {}

    def price_getter(symbol):
        key = symbol.upper()
        if key not in cached_prices:
            cached_prices[key] = get_current_price(key)
        return cached_prices[key]

    users = User.query.all()
    for user in users:
        holdings = Holding.query.filter_by(user_id=user.id).all()
        holdings_value, unrealized_pnl = _calculate_holdings_value_and_unrealized(holdings, price_getter)
        total_value = user.cash_balance + holdings_value
        snapshots.append({
            "username": user.username,
            "account_id": f"global:{user.id}",
            "account_type": "global",
            "date": snapshot_date,
            "total_value": total_value,
            "cash": user.cash_balance,
            "total_pnl": (user.realized_pnl or 0.0) + unrealized_pnl,
        })

    members = CompetitionMember.query.all()
    for member in members:
        user = db.session.get(User, member.user_id)
        if not user:
            continue
        holdings = CompetitionHolding.query.filter_by(competition_member_id=member.id).all()
        holdings_value, unrealized_pnl = _calculate_holdings_value_and_unrealized(holdings, price_getter)
        total_value = member.cash_balance + holdings_value
        snapshots.append({
            "username": user.username,
            "account_id": str(member.id),
            "account_type": "competition",
            "date": snapshot_date,
            "total_value": total_value,
            "cash": member.cash_balance,
            "total_pnl": (member.realized_pnl or 0.0) + unrealized_pnl,
        })

    team_comp_entries = CompetitionTeam.query.all()
    for ct in team_comp_entries:
        team_members = TeamMember.query.filter_by(team_id=ct.team_id).all()
        if not team_members:
            continue
        holdings = CompetitionTeamHolding.query.filter_by(competition_team_id=ct.id).all()
        holdings_value, unrealized_pnl = _calculate_holdings_value_and_unrealized(holdings, price_getter)
        total_value = ct.cash_balance + holdings_value
        total_pnl = (ct.realized_pnl or 0.0) + unrealized_pnl

        for tm in team_members:
            user = db.session.get(User, tm.user_id)
            if not user:
                continue
            snapshots.append({
                "username": user.username,
                "account_id": str(ct.id),
                "account_type": "team_competition",
                "date": snapshot_date,
                "total_value": total_value,
                "cash": ct.cash_balance,
                "total_pnl": total_pnl,
            })

    return snapshots


def run_daily_account_performance_snapshot_job():
    with app.app_context():
        eastern_tz = pytz.timezone('America/New_York')
        snapshot_date = datetime.now(eastern_tz).date()
        snapshots = _generate_daily_account_snapshots(snapshot_date)
        upsert_count = 0
        try:
            for snapshot in snapshots:
                _upsert_account_performance_snapshot_record(snapshot)
                upsert_count += 1
            db.session.commit()
        except Exception:
            db.session.rollback()
            app.logger.exception('Automatic daily account performance snapshot job failed')
            return
        app.logger.info(
            "Automatic daily account performance snapshot job completed for %s with %s upserts",
            snapshot_date.isoformat(),
            upsert_count
        )

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
                    "account_id": m.id,
                    "competition_member_id": m.id,
                    "competitionMemberId": m.id,
                    "user_id": user.id,
                    "userId": user.id,
                    "competition_id": comp.id,
                    "code": comp.code,
                    "competition_code": comp.code,
                    "name": comp.name,
                    "competition_name": comp.name,
                    "account_type": "competition",
                    "team_name": None,
                    "account_display_name": _account_display_name("competition", competition_name=comp.name, competition_code=comp.code),
                    "cash_balance": m.cash_balance,
                    "portfolio": comp_portfolio,
                    "total_value": total_value,
                    "pnl": total_pnl,
                    "return_pct": return_pct,
                    "realized_pnl": m.realized_pnl or 0.0,
                    "is_instructor_for_competition": _is_competition_instructor(user, comp),
                })

        # --- Team Competitions ---
        team_memberships = TeamMember.query.filter_by(user_id=user.id).all()
        team_competitions = []
        for tm in team_memberships:
            ct_entries = CompetitionTeam.query.filter_by(team_id=tm.team_id).all()
            for ct in ct_entries:
                comp = db.session.get(Competition, ct.competition_id)
                if comp:
                    team = db.session.get(Team, ct.team_id)
                    if not team:
                        # Skip orphaned records so legacy data cannot break account loading.
                        continue
                    team_name = team.name
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
                        "account_id": ct.id,
                        "competition_id": comp.id,
                        "code": comp.code,
                        "competition_code": comp.code,
                        "name": comp.name,
                        "competition_name": comp.name,
                        "account_type": "team_competition",
                        "account_display_name": _account_display_name("team_competition", competition_name=comp.name, competition_code=comp.code, team_name=team_name),
                        "cash_balance": ct.cash_balance,
                        "portfolio": team_portfolio,
                        "total_value": total_value,
                        "pnl": total_pnl,
                        "return_pct": return_pct,
                        'realized_pnl': ct.realized_pnl or 0.0,
                        "team_id": ct.team_id,
                        "team_name": team_name,
                        "is_instructor_for_competition": _is_competition_instructor(user, comp),
                        # Unified payload for rendering team+competition in one UI container.
                        "team_competition": {
                            "team": {"id": ct.team_id, "name": team_name},
                            "competition": {"code": comp.code, "name": comp.name}
                        }
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
            'account_id': m.id,
            'competition_member_id': m.id,
            'competitionMemberId': m.id,
            'user_id': user.id,
            'userId': user.id,
            'competition_id': comp.id,
            'code': comp.code,
            'competition_code': comp.code,
            'name': comp.name,
            'competition_name': comp.name,
            'account_type': 'competition',
            'team_name': None,
            'account_display_name': _account_display_name('competition', competition_name=comp.name, competition_code=comp.code),
            'cash_balance': m.cash_balance,
            'portfolio': comp_portfolio,
            'total_value': comp_total_value,
            'pnl': comp_total_pnl,
            'return_pct': comp_return_pct,
            'realized_pnl': m.realized_pnl or 0.0,
            'start_of_day_value': comp_start_of_day_value,
            'pnl_today': comp_pnl_today,
            'pnl_pct_today': comp_pnl_pct_today
            ,
            'is_instructor_for_competition': _is_competition_instructor(user, comp)
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

            team = db.session.get(Team, ct.team_id)
            if not team:
                # Skip orphaned records so legacy data cannot break account loading.
                continue
            team_name = team.name

            team_competitions.append({
                'account_id': ct.id,
                'competition_id': comp.id,
                'code': comp.code,
                'competition_code': comp.code,
                'name': comp.name,
                'competition_name': comp.name,
                'account_type': 'team_competition',
                'account_display_name': _account_display_name('team_competition', competition_name=comp.name, competition_code=comp.code, team_name=team_name),
                'cash_balance': ct.cash_balance,
                'portfolio': team_portfolio,
                'total_value': team_total_value,
                'team_id': ct.team_id,
                'team_name': team_name,
                'pnl': team_total_pnl,
                'return_pct': team_return_pct,
                'realized_pnl': ct.realized_pnl or 0.0,
                'start_of_day_value': team_start_of_day_value,
                'pnl_today': team_pnl_today,
                'pnl_pct_today': team_pnl_pct_today,
                'is_instructor_for_competition': _is_competition_instructor(user, comp),
                # Unified payload for rendering team+competition in one UI container.
                'team_competition': {
                    'team': {'id': ct.team_id, 'name': team_name},
                    'competition': {'code': comp.code, 'name': comp.name}
                }
            })

    # --- Final Response ---
    global_account = {
        'account_id': f'global:{user.id}',
        'account_type': 'global',
        'competition_code': None,
        'competition_name': None,
        'team_name': None,
        'account_display_name': _account_display_name('global'),
        'cash_balance': user.cash_balance,
        'portfolio': global_portfolio,
        'total_value': global_total_value,
        'pnl': global_total_pnl,
        'realized_pnl': user.realized_pnl,
        'return_pct': global_return_pct,
        'start_of_day_value': global_start_of_day_value,
        'pnl_today': global_pnl_today,
        'pnl_pct_today': global_pnl_pct_today
    }

    all_accounts = [global_account, *competition_accounts, *team_competitions]

    response_data = {
        'username': user.username,
        'is_admin': user.is_admin,
        'global_account': global_account,
        'accounts': all_accounts,
        'competition_accounts': competition_accounts,
        'team_competitions': team_competitions
    }

    return jsonify(response_data)


# --------------------
# Account Performance History Endpoints
# --------------------
@app.route('/account/performance/snapshot', methods=['POST'])
@app.route('/account/performance', methods=['POST'])
def upsert_account_performance_snapshot():
    data = request.get_json() or {}
    validated, error = _validate_performance_payload(data)
    if error:
        return jsonify({'message': error}), 400

    try:
        _upsert_account_performance_snapshot_record(validated)
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        app.logger.exception('Failed to upsert performance snapshot')
        return jsonify({'message': f'Failed to persist performance snapshot: {str(exc)}'}), 500

    return jsonify({
        'message': 'Performance snapshot upserted successfully',
        'snapshot': {
            'username': validated['username'],
            'account_id': validated['account_id'],
            'account_type': validated['account_type'],
            'date': validated['date'].isoformat(),
            'total_value': validated['total_value'],
            'cash': validated['cash'],
            'total_pnl': validated['total_pnl'],
        }
    }), 200


@app.route('/account/performance/history', methods=['GET'])
@app.route('/account/performance', methods=['GET'])
def get_account_performance_history():
    username = (request.args.get('username') or '').strip()
    account_id = str(request.args.get('account_id') or '').strip()
    account_type = _normalize_account_type(request.args.get('account_type'))

    if not username:
        return jsonify({'message': 'username is required'}), 400
    if not account_id:
        return jsonify({'message': 'account_id is required'}), 400
    if not account_type:
        return jsonify({'message': 'account_type is required'}), 400
    if account_type not in VALID_ACCOUNT_TYPES:
        return jsonify({'message': 'account_type is invalid'}), 400

    rows = (
        AccountPerformanceHistory.query
        .filter_by(username=username, account_id=account_id, account_type=account_type)
        .order_by(AccountPerformanceHistory.date.asc())
        .all()
    )

    history = [{
        'date': row.date.isoformat(),
        'total_value': row.total_value,
        'cash': row.cash,
        'total_pnl': row.total_pnl,
        'updated_at': row.updated_at.isoformat() if row.updated_at else None,
    } for row in rows]

    return jsonify({
        'username': username,
        'account_id': account_id,
        'account_type': account_type,
        'history': history
    }), 200


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
    curriculum_enabled = bool(_first_present(data, 'curriculumEnabled', 'curriculum_enabled', default=False))
    curriculum_weeks = _first_present(data, 'curriculumWeeks', 'curriculum_weeks')
    curriculum_start_date_str = _first_present(data, 'curriculumStartDate', 'curriculum_start_date')
    curriculum_end_date_str = _first_present(data, 'curriculumEndDate', 'curriculum_end_date')
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404
    code = secrets.token_hex(4)
    while Competition.query.filter_by(code=code).first():
        code = secrets.token_hex(4)
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d") if start_date_str else None
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else None
    if curriculum_enabled:
        try:
            curriculum_start_date = _parse_iso_date(curriculum_start_date_str, "curriculumStartDate")
            curriculum_end_date = _parse_iso_date(curriculum_end_date_str, "curriculumEndDate")
            curriculum_weeks = _validate_curriculum_window(curriculum_weeks, curriculum_start_date, curriculum_end_date)
        except ValueError as exc:
            return jsonify({'message': str(exc)}), 400
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
    db.session.flush()
    if curriculum_enabled:
        generate_curriculum_for_competition(
            competition_id=comp.id,
            total_weeks=curriculum_weeks,
            start_date=curriculum_start_date,
            end_date=curriculum_end_date,
            overwrite=False,
        )
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


@app.route('/curriculum/competition/<int:competition_id>/generate', methods=['POST'])
def curriculum_generate(competition_id):
    data = request.get_json() or {}
    username = data.get('username')
    user = User.query.filter_by(username=username).first() if username else None
    competition = db.session.get(Competition, competition_id)
    if not competition:
        return jsonify({"message": "Competition not found"}), 404
    if not _is_competition_instructor(user, competition):
        return jsonify({"message": "Instructor access required"}), 403
    try:
        total_weeks = _validate_curriculum_window(
            data.get("curriculumWeeks"),
            _parse_iso_date(data.get("curriculumStartDate"), "curriculumStartDate"),
            _parse_iso_date(data.get("curriculumEndDate"), "curriculumEndDate"),
        )
        curriculum = generate_curriculum_for_competition(
            competition_id=competition_id,
            total_weeks=total_weeks,
            start_date=_parse_iso_date(data.get("curriculumStartDate"), "curriculumStartDate"),
            end_date=_parse_iso_date(data.get("curriculumEndDate"), "curriculumEndDate"),
            overwrite=bool(data.get("overwrite", True)),
        )
        db.session.commit()
        return jsonify({"message": "Curriculum generated", "curriculumId": curriculum.id}), 201
    except ValueError as exc:
        db.session.rollback()
        return jsonify({"message": str(exc)}), 400
    except Exception as exc:
        db.session.rollback()
        return jsonify({"message": f"Failed to generate curriculum: {str(exc)}"}), 500


@app.route('/curriculum/competition/<int:competition_id>', methods=['GET'])
def curriculum_summary(competition_id):
    resolved_competition_id = _resolve_curriculum_competition_id(competition_id)
    curriculum = Curriculum.query.filter_by(competition_id=resolved_competition_id, enabled=True).first()
    if not curriculum:
        return jsonify({"message": "Curriculum not enabled for this competition"}), 404
    module_count = CurriculumModule.query.filter_by(curriculum_id=curriculum.id).count()
    assignment_count = db.session.query(CurriculumAssignment).join(
        CurriculumModule, CurriculumAssignment.module_id == CurriculumModule.id
    ).filter(CurriculumModule.curriculum_id == curriculum.id).count()
    return jsonify({
        "curriculumId": curriculum.id,
        "competitionId": resolved_competition_id,
        "enabled": curriculum.enabled,
        "totalWeeks": curriculum.total_weeks,
        "startDate": curriculum.start_date.date().isoformat(),
        "endDate": curriculum.end_date.date().isoformat(),
        "moduleCount": module_count,
        "assignmentCount": assignment_count,
    })


@app.route('/curriculum/competition/<int:competition_id>/modules', methods=['GET'])
def curriculum_modules(competition_id):
    resolved_competition_id = _resolve_curriculum_competition_id(competition_id)
    curriculum = Curriculum.query.filter_by(competition_id=resolved_competition_id, enabled=True).first()
    if not curriculum:
        return jsonify({"message": "Curriculum not enabled for this competition"}), 404
    # Optional requester context lets us compute per-student lock state and instructor bypass.
    requester_username = request.args.get("username")
    requester_user = User.query.filter_by(username=requester_username).first() if requester_username else None
    competition = db.session.get(Competition, resolved_competition_id)
    modules = CurriculumModule.query.filter_by(curriculum_id=curriculum.id).order_by(CurriculumModule.week_number.asc()).all()
    payload = []
    did_update_legacy_content = False
    now = datetime.utcnow()
    for module in modules:
        assignments = CurriculumAssignment.query.filter_by(module_id=module.id).all()
        for assignment in assignments:
            upgraded_content = _expand_legacy_assignment_content(module.title, assignment.type, assignment.content_json)
            if upgraded_content is not assignment.content_json:
                assignment.content_json = upgraded_content
                did_update_legacy_content = True
        lock_state = _module_lock_state(
            curriculum,
            module,
            requester_user.id if requester_user else None,
            now=now,
            requester_user=requester_user,
            competition=competition,
        )
        payload.append({
            "moduleId": module.id,
            "weekNumber": module.week_number,
            "title": module.title,
            "description": module.description,
            "lessonContent": module.lesson_content,
            "lesson_content": module.lesson_content,
            "lessonContentHtml": _lesson_content_html(module.lesson_content),
            "lesson_content_html": _lesson_content_html(module.lesson_content),
            "unlockDate": module.unlock_date.isoformat(),
            "unlocksAt": module.unlock_date.isoformat(),
            "dueDate": module.due_date.isoformat(),
            "locked": lock_state["locked"],
            "timeUnlocked": lock_state["timeUnlocked"],
            "prerequisiteMet": lock_state["prerequisiteMet"],
            "prerequisiteModuleId": lock_state["prerequisiteModuleId"],
            "enforcePrerequisites": lock_state["enforcePrerequisites"],
            "passingThreshold": lock_state["passingThreshold"],
            "assignments": [_serialize_module_assignment(a) for a in assignments]
        })
    if did_update_legacy_content:
        db.session.commit()
    return jsonify(payload)


@app.route('/curriculum/competition/<int:competition_id>/modules/lesson-content', methods=['PATCH'])
def curriculum_update_lesson_content(competition_id):
    """Instructor-only endpoint to author/update eText (lesson_content) for selected modules
    without regenerating the whole curriculum. See docs/etext_update_backend_prompt.md.
    """
    data = request.get_json() or {}
    username = data.get("username")
    updates = data.get("updates")
    user = User.query.filter_by(username=username).first() if username else None
    if not user:
        return jsonify({"message": "username is required and must refer to an existing user"}), 401

    resolved_competition_id = _resolve_curriculum_competition_id(competition_id, requester_user_id=user.id)
    competition = db.session.get(Competition, resolved_competition_id)
    if not competition:
        return jsonify({"message": "Competition not found"}), 404
    if not _is_competition_instructor(user, competition):
        return jsonify({"message": "Instructor access required"}), 403

    curriculum = Curriculum.query.filter_by(competition_id=resolved_competition_id, enabled=True).first()
    if not curriculum:
        return jsonify({"message": "Curriculum not enabled for this competition"}), 404

    if not isinstance(updates, list) or not updates:
        return jsonify({"message": "updates must be a non-empty array"}), 400

    valid_module_ids = {
        m.id for m in CurriculumModule.query.filter_by(curriculum_id=curriculum.id).all()
    }
    normalized_updates = []
    for idx, item in enumerate(updates):
        if not isinstance(item, dict):
            return jsonify({"message": f"updates[{idx}] must be an object"}), 400
        raw_id = _first_present(item, "moduleId", "module_id")
        raw_content = _first_present(item, "lessonContent", "lesson_content")
        try:
            module_id = int(raw_id)
        except (TypeError, ValueError):
            return jsonify({"message": f"updates[{idx}].moduleId must be an integer"}), 400
        if module_id not in valid_module_ids:
            return jsonify({"message": f"updates[{idx}].moduleId does not belong to this curriculum"}), 400
        if not isinstance(raw_content, str):
            return jsonify({"message": f"updates[{idx}].lessonContent must be a non-empty string"}), 400
        trimmed = raw_content.strip()
        if not trimmed:
            return jsonify({"message": f"updates[{idx}].lessonContent must be a non-empty string"}), 400
        normalized_updates.append((module_id, trimmed))

    updated_ids = []
    for module_id, content in normalized_updates:
        module = db.session.get(CurriculumModule, module_id)
        if not module:
            continue
        module.lesson_content = content
        module.updated_at = datetime.utcnow()
        updated_ids.append(module_id)
    db.session.commit()
    return jsonify({
        "updatedCount": len(updated_ids),
        "updatedModuleIds": updated_ids,
        "competitionId": resolved_competition_id,
        "curriculumId": curriculum.id,
    })


def _validate_written_assignment_content(content, idx):
    """Validate and normalize a written assignment's content_json.

    Mirrors the structure produced by _assignment_content_for_module so existing
    frontend renderers keep working. Returns (normalized_content, error_message).
    If error_message is truthy, the caller should reject the update.
    """
    if not isinstance(content, dict):
        return None, f"updates[{idx}].content must be an object"
    instructions = content.get("instructions")
    if not isinstance(instructions, str) or not instructions.strip():
        return None, f"updates[{idx}].content.instructions must be a non-empty string"
    questions = content.get("questions")
    if not isinstance(questions, list) or not questions:
        return None, f"updates[{idx}].content.questions must be a non-empty array"

    seen_ids = set()
    normalized_questions = []
    for q_idx, question in enumerate(questions):
        if not isinstance(question, dict):
            return None, f"updates[{idx}].content.questions[{q_idx}] must be an object"
        qid = question.get("id")
        if not isinstance(qid, str) or not qid.strip():
            return None, f"updates[{idx}].content.questions[{q_idx}].id must be a non-empty string"
        qid = qid.strip()
        if qid in seen_ids:
            return None, f"updates[{idx}].content.questions has duplicate id '{qid}'"
        seen_ids.add(qid)
        prompt = question.get("prompt")
        if not isinstance(prompt, str) or not prompt.strip():
            return None, f"updates[{idx}].content.questions[{q_idx}].prompt must be a non-empty string"
        sections = question.get("sections")
        if not isinstance(sections, list) or not sections:
            return None, f"updates[{idx}].content.questions[{q_idx}].sections must be a non-empty array"
        normalized_sections = []
        for s_idx, section in enumerate(sections):
            if not isinstance(section, dict):
                return None, f"updates[{idx}].content.questions[{q_idx}].sections[{s_idx}] must be an object"
            sid = section.get("id")
            if not isinstance(sid, str) or not sid.strip():
                return None, f"updates[{idx}].content.questions[{q_idx}].sections[{s_idx}].id must be a non-empty string"
            instruction = section.get("instruction")
            if not isinstance(instruction, str) or not instruction.strip():
                return None, f"updates[{idx}].content.questions[{q_idx}].sections[{s_idx}].instruction must be a non-empty string"
            normalized_sections.append({"id": sid.strip(), "instruction": instruction.strip()})
        canonical_prompt = _strip_inlined_sections_from_prompt(prompt.strip(), normalized_sections)
        if not canonical_prompt:
            return None, f"updates[{idx}].content.questions[{q_idx}].prompt must be a non-empty string"
        normalized_question = {
            "id": qid,
            "prompt": canonical_prompt,
            "sections": normalized_sections,
        }
        if "kind" in question and isinstance(question.get("kind"), str):
            normalized_question["kind"] = question["kind"]
        if "points" in question:
            try:
                normalized_question["points"] = int(question["points"])
            except (TypeError, ValueError):
                return None, f"updates[{idx}].content.questions[{q_idx}].points must be an integer"
        normalized_questions.append(normalized_question)

    rubric_hints = content.get("rubricHints")
    normalized_rubric = None
    if rubric_hints is not None:
        if not isinstance(rubric_hints, list):
            return None, f"updates[{idx}].content.rubricHints must be an array of strings"
        normalized_rubric = []
        for h_idx, hint in enumerate(rubric_hints):
            if not isinstance(hint, str) or not hint.strip():
                return None, f"updates[{idx}].content.rubricHints[{h_idx}] must be a non-empty string"
            normalized_rubric.append(hint.strip())

    normalized = {
        "instructions": instructions.strip(),
        "questions": normalized_questions,
    }
    if normalized_rubric is not None:
        normalized["rubricHints"] = normalized_rubric
    return normalized, None


@app.route('/curriculum/competition/<int:competition_id>/assignments/content', methods=['PATCH'])
def curriculum_update_assignment_content(competition_id):
    """Instructor-only endpoint to author/update written-assignment prompts for selected
    assignments without regenerating the curriculum. Scoped to type='assignment' only;
    quizzes and exams are intentionally excluded so answer keys stay stable.
    """
    data = request.get_json() or {}
    username = data.get("username")
    updates = data.get("updates")
    user = User.query.filter_by(username=username).first() if username else None
    if not user:
        return jsonify({"message": "username is required and must refer to an existing user"}), 401

    resolved_competition_id = _resolve_curriculum_competition_id(competition_id, requester_user_id=user.id)
    competition = db.session.get(Competition, resolved_competition_id)
    if not competition:
        return jsonify({"message": "Competition not found"}), 404
    if not _is_competition_instructor(user, competition):
        return jsonify({"message": "Instructor access required"}), 403

    curriculum = Curriculum.query.filter_by(competition_id=resolved_competition_id, enabled=True).first()
    if not curriculum:
        return jsonify({"message": "Curriculum not enabled for this competition"}), 404

    if not isinstance(updates, list) or not updates:
        return jsonify({"message": "updates must be a non-empty array"}), 400

    curriculum_module_ids = [
        m.id for m in CurriculumModule.query.filter_by(curriculum_id=curriculum.id).all()
    ]
    assignments_in_curriculum = {
        a.id: a
        for a in CurriculumAssignment.query.filter(
            CurriculumAssignment.module_id.in_(curriculum_module_ids)
        ).all()
    } if curriculum_module_ids else {}

    normalized_updates = []
    for idx, item in enumerate(updates):
        if not isinstance(item, dict):
            return jsonify({"message": f"updates[{idx}] must be an object"}), 400
        raw_id = _first_present(item, "assignmentId", "assignment_id")
        try:
            assignment_id = int(raw_id)
        except (TypeError, ValueError):
            return jsonify({"message": f"updates[{idx}].assignmentId must be an integer"}), 400
        assignment = assignments_in_curriculum.get(assignment_id)
        if not assignment:
            return jsonify({"message": f"updates[{idx}].assignmentId does not belong to this curriculum"}), 400
        if assignment.type != "assignment":
            return jsonify({
                "message": f"updates[{idx}].assignmentId refers to a {assignment.type}; only written assignments are editable here",
            }), 400
        raw_content = _first_present(item, "content", "content_json", "contentJson")
        normalized_content, err = _validate_written_assignment_content(raw_content, idx)
        if err:
            return jsonify({"message": err}), 400
        normalized_updates.append((assignment, normalized_content))

    updated_ids = []
    for assignment, content in normalized_updates:
        assignment.content_json = content
        assignment.updated_at = datetime.utcnow()
        updated_ids.append(assignment.id)
    db.session.commit()
    return jsonify({
        "updatedCount": len(updated_ids),
        "updatedAssignmentIds": updated_ids,
        "competitionId": resolved_competition_id,
        "curriculumId": curriculum.id,
    })


@app.route('/curriculum/competition/<int:competition_id>/grades/<int:user_id>', methods=['GET'])
def curriculum_grades(competition_id, user_id):
    requester = request.args.get("username")
    if not requester:
        return jsonify(_error_payload("Authentication required", "auth_required")), 401
    requesting_user = User.query.filter_by(username=requester).first()
    if not requesting_user:
        return jsonify(_error_payload("Invalid credentials", "invalid_credentials")), 401
    resolved_competition_id = _resolve_curriculum_competition_id(
        competition_id,
        requester_user_id=requesting_user.id,
    )
    competition = db.session.get(Competition, resolved_competition_id)
    if not competition:
        return jsonify(_error_payload("Competition not found", "competition_not_found")), 404

    # Frontends may pass either a User.id or a CompetitionMember.id in the path.
    # Prefer the requester's own membership row when ids collide.
    target_member = db.session.get(CompetitionMember, user_id)
    target_user = db.session.get(User, user_id)
    if (
        target_member
        and target_member.competition_id == resolved_competition_id
        and requesting_user.id == target_member.user_id
    ):
        target_user_id = target_member.user_id
        target_user = db.session.get(User, target_user_id)
    elif target_user is not None:
        target_user_id = target_user.id
    elif target_member and target_member.competition_id == resolved_competition_id:
        target_user_id = target_member.user_id
        target_user = db.session.get(User, target_user_id)
    else:
        target_user_id = None

    if target_user_id is None or not target_user:
        return jsonify(_error_payload("User not found", "user_not_found")), 404
    if requesting_user.id != target_user_id and not _is_competition_instructor(requesting_user, competition):
        return jsonify(_error_payload("Forbidden", "forbidden")), 403

    competition_membership = CompetitionMember.query.filter_by(
        competition_id=resolved_competition_id,
        user_id=target_user_id,
    ).first()
    if not competition_membership:
        return jsonify(_error_payload("No grade records found for this user in the specified competition", "grade_records_not_found")), 404

    summary = _compute_grade_summary(resolved_competition_id, target_user_id)
    if summary is None:
        return jsonify(_error_payload("Curriculum not enabled for this competition", "curriculum_not_enabled")), 404
    return jsonify({
        **summary,
        "gradeSummary": summary,
    })


@app.route('/curriculum/competition/<int:competition_id>/grades', methods=['GET'])
def curriculum_competition_grades(competition_id):
    requester = request.args.get("username")
    requesting_user = User.query.filter_by(username=requester).first() if requester else None
    competition = db.session.get(Competition, competition_id)
    if not competition:
        return jsonify({"message": "Competition not found"}), 404
    if not _is_competition_instructor(requesting_user, competition):
        return jsonify({"message": "Instructor access required"}), 403

    curriculum = Curriculum.query.filter_by(competition_id=competition_id, enabled=True).first()
    if not curriculum:
        return jsonify({"message": "Curriculum not enabled for this competition"}), 404

    member_ids = [m.user_id for m in CompetitionMember.query.filter_by(competition_id=competition_id).all()]
    student_grades = []
    for uid in member_ids:
        user_obj = db.session.get(User, uid)
        summary = _compute_grade_summary(competition_id, uid)
        if not summary:
            continue
        summary["username"] = user_obj.username if user_obj else f"user-{uid}"
        student_grades.append(summary)

    return jsonify({
        "competitionId": competition_id,
        "curriculumId": curriculum.id,
        "studentCount": len(student_grades),
        "grades": student_grades,
    })


@app.route('/curriculum/assignments/<int:assignment_id>/submissions', methods=['GET', 'POST'])
def curriculum_submit_assignment(assignment_id):
    if request.method == 'GET':
        requester = request.args.get("username")
        user = User.query.filter_by(username=requester).first() if requester else None
        assignment = db.session.get(CurriculumAssignment, assignment_id)
        if not assignment:
            return jsonify({"message": "Assignment not found"}), 404
        module = db.session.get(CurriculumModule, assignment.module_id)
        curriculum = db.session.get(Curriculum, module.curriculum_id) if module else None
        competition = db.session.get(Competition, curriculum.competition_id) if curriculum else None
        if not curriculum or not competition:
            return jsonify({"message": "Curriculum not found"}), 404
        if not _is_competition_instructor(user, competition):
            return jsonify({"message": "Instructor access required"}), 403

        submissions = CurriculumSubmission.query.filter_by(assignment_id=assignment_id).order_by(
            CurriculumSubmission.submitted_at.desc()
        ).all()
        payload = []
        for sub in submissions:
            student = db.session.get(User, sub.user_id)
            payload.append({
                "submissionId": sub.id,
                "assignmentId": assignment_id,
                "userId": sub.user_id,
                "username": student.username if student else f"user-{sub.user_id}",
                "answers": sub.answers_json,
                "score": sub.score,
                "percentage": sub.percentage,
                "autoGraded": sub.auto_graded,
                "submittedAt": sub.submitted_at.isoformat(),
                "feedback": sub.feedback_json,
                "question1Score": sub.question_1_score,
                "question2Score": sub.question_2_score,
                "assignmentTotalScore": sub.assignment_total_score,
                "gradingStatus": _written_submission_status(sub) if assignment.type == "assignment" else ("graded" if sub.auto_graded else "submitted"),
                "isManuallyGradable": assignment.type == "assignment",
                "gradedByUserId": sub.graded_by_user_id,
                "gradedAt": sub.graded_at.isoformat() if sub.graded_at else None,
                "rubricNotes": sub.rubric_notes,
            })
        return jsonify({
            "assignmentId": assignment_id,
            "assignmentType": assignment.type,
            "totalSubmissions": len(payload),
            "submissions": payload,
        })

    data = request.get_json() or {}
    username = data.get("username")
    answers = data.get("answers", {})
    user = User.query.filter_by(username=username).first() if username else None
    if not user:
        return jsonify({"message": "User not found"}), 404
    assignment = db.session.get(CurriculumAssignment, assignment_id)
    if not assignment:
        return jsonify({"message": "Assignment not found"}), 404
    module = db.session.get(CurriculumModule, assignment.module_id)
    curriculum = db.session.get(Curriculum, module.curriculum_id) if module else None
    if not module or not curriculum or not curriculum.enabled:
        return jsonify({"message": "Curriculum not enabled for this assignment"}), 404
    request_competition_id = _first_present(data, "competition_id", "competitionId")
    if request_competition_id is None:
        return jsonify({"message": "competition_id is required"}), 422
    resolved_request_competition_id = _resolve_submission_competition_id(request_competition_id)
    if resolved_request_competition_id is None:
        return jsonify({"message": "competition_id must be a valid integer"}), 422
    if resolved_request_competition_id != curriculum.competition_id:
        return jsonify({"message": "assignment does not belong to provided competition_id"}), 422

    # optional membership guard for students
    membership = CompetitionMember.query.filter_by(competition_id=curriculum.competition_id, user_id=user.id).first()
    comp = db.session.get(Competition, curriculum.competition_id)
    if not membership and not _is_competition_instructor(user, comp):
        return jsonify({"message": "User is not a member of this competition"}), 403

    # Module lock enforcement: non-instructors cannot submit to a locked module when
    # enforce_prerequisites is on for this curriculum. Returns 423 Locked.
    lock_state = _module_lock_state(
        curriculum,
        module,
        user.id,
        requester_user=user,
        competition=comp,
    )
    if lock_state["locked"]:
        reason = "Module is locked: complete the prerequisite module first." if not lock_state["prerequisiteMet"] else "Module is not yet unlocked."
        return jsonify({
            "message": reason,
            "locked": True,
            "prerequisiteMet": lock_state["prerequisiteMet"],
            "prerequisiteModuleId": lock_state["prerequisiteModuleId"],
            "unlocksAt": module.unlock_date.isoformat(),
        }), 423

    try:
        normalized_answers = _normalize_submission_answers(assignment, answers)
    except ValueError as exc:
        return jsonify({"message": str(exc)}), 422

    # Load or initialize the submission row first so we can honor any stored question-order snapshot
    # when grading. Snapshot is captured on the first quiz/exam submission and reused afterwards.
    submission = CurriculumSubmission.query.filter_by(assignment_id=assignment_id, user_id=user.id).first()
    if not submission:
        submission = CurriculumSubmission(
            assignment_id=assignment_id,
            user_id=user.id,
            competition_id=curriculum.competition_id,
            answers_json=normalized_answers,
        )
        if assignment.type in ("quiz", "exam"):
            submission.question_order_json = _build_quiz_snapshot(assignment)
        db.session.add(submission)
    elif assignment.type in ("quiz", "exam") and not getattr(submission, 'question_order_json', None):
        # Back-fill snapshot for pre-existing submissions so subsequent writes stay stable.
        submission.question_order_json = _build_quiz_snapshot(assignment)

    score = 0.0
    auto_graded = False
    feedback = {"lateSubmission": datetime.utcnow() > module.due_date}
    if assignment.type in ("quiz", "exam"):
        # Grade against the per-submission snapshot (falls back to current answer key if none).
        answer_key = _snapshot_answer_key(submission, assignment)
        if answer_key:
            total_questions = len(answer_key)
            correct = 0
            mapped_answers = _quiz_answer_map_from_submission(normalized_answers)
            for qid, expected in answer_key.items():
                if mapped_answers.get(str(qid)) == expected:
                    correct += 1
            score = assignment.points * (correct / total_questions)
            auto_graded = True
            feedback["correct"] = correct
            feedback["totalQuestions"] = total_questions
            feedback["gradedAgainstSnapshot"] = bool(getattr(submission, 'question_order_json', None))
        else:
            feedback["gradingMode"] = "no_answer_key"
    else:
        # Written assignments are instructor graded.
        score = 0.0
        auto_graded = False
        feedback["gradingMode"] = "manual_instructor_required"
        feedback["pendingManualGrade"] = True
        feedback["question1PointsPossible"] = 10
        feedback["question2PointsPossible"] = 10

    percentage = (score / assignment.points * 100.0) if assignment.points else 0.0
    submission.answers_json = normalized_answers
    submission.competition_id = curriculum.competition_id
    submission.score = round(score, 2)
    submission.percentage = round(percentage, 2)
    submission.submitted_at = datetime.utcnow()
    submission.auto_graded = auto_graded
    if assignment.type == "assignment":
        submission.question_1_score = None
        submission.question_2_score = None
        submission.assignment_total_score = 0.0
    else:
        submission.question_1_score = None
        submission.question_2_score = None
        submission.assignment_total_score = None
    submission.feedback_json = feedback
    db.session.commit()
    updated_summary = _compute_grade_summary(curriculum.competition_id, user.id)

    return jsonify({
        "assignmentId": assignment_id,
        "competitionId": curriculum.competition_id,
        "competition_id": curriculum.competition_id,
        "userId": user.id,
        "user_id": user.id,
        "answers": submission.answers_json,
        "score": submission.score,
        "pointsEarned": submission.score,
        "pointsPossible": assignment.points,
        "percentage": submission.percentage,
        "status": "graded" if submission.auto_graded else ("submitted" if assignment.type in ("quiz", "exam") else "pending_grade"),
        "autoGraded": submission.auto_graded,
        "submittedAt": submission.submitted_at.isoformat(),
        "feedback": submission.feedback_json,
        "question1Score": submission.question_1_score,
        "question2Score": submission.question_2_score,
        "assignmentTotalScore": submission.assignment_total_score,
        "submissionId": submission.id,
        "isManuallyGradable": assignment.type == "assignment",
        "gradeSummary": updated_summary,
    })


@app.route('/curriculum/assignments/<int:assignment_id>/submissions/<int:user_id>', methods=['GET'])
def curriculum_get_submission(assignment_id, user_id):
    requester = request.args.get("username")
    requesting_user = User.query.filter_by(username=requester).first() if requester else None
    if not requesting_user:
        return jsonify({"message": "Requesting user not found"}), 404
    assignment = db.session.get(CurriculumAssignment, assignment_id)
    if not assignment:
        return jsonify({"message": "Assignment not found"}), 404
    module = db.session.get(CurriculumModule, assignment.module_id)
    curriculum = db.session.get(Curriculum, module.curriculum_id) if module else None
    competition = db.session.get(Competition, curriculum.competition_id) if curriculum else None
    if not curriculum or not competition:
        return jsonify({"message": "Curriculum not found"}), 404
    if requesting_user.id != user_id and not _is_competition_instructor(requesting_user, competition):
        return jsonify({"message": "Forbidden"}), 403
    submission = CurriculumSubmission.query.filter_by(assignment_id=assignment_id, user_id=user_id).first()
    if not submission:
        return jsonify({"message": "Submission not found"}), 404
    question_grades = _submission_question_grade_rows(submission.id)
    return jsonify({
        "assignmentId": assignment_id,
        "userId": user_id,
        "answers": submission.answers_json,
        "submissionContent": submission.answers_json,
        "score": submission.score,
        "percentage": submission.percentage,
        "submittedAt": submission.submitted_at.isoformat(),
        "autoGraded": submission.auto_graded,
        "feedback": submission.feedback_json,
        "question1Score": submission.question_1_score,
        "question2Score": submission.question_2_score,
        "assignmentTotalScore": submission.assignment_total_score,
        "gradingStatus": _written_submission_status(submission) if assignment.type == "assignment" else ("graded" if submission.auto_graded else "submitted"),
        "isManuallyGradable": assignment.type == "assignment",
        "submissionId": submission.id,
        "gradedByUserId": submission.graded_by_user_id,
        "gradedAt": submission.graded_at.isoformat() if submission.graded_at else None,
        "rubricNotes": submission.rubric_notes,
        "questionGrades": [
            {
                "questionId": row.question_id,
                "pointsAwarded": row.points_awarded,
                "pointsPossible": row.points_possible,
                "feedback": row.feedback,
                "gradedBy": row.graded_by,
                "gradedAt": row.graded_at.isoformat() if row.graded_at else None,
            } for row in question_grades
        ],
    })


@app.route('/curriculum/submissions/<int:submission_id>/grade', methods=['POST'])
def curriculum_grade_submission(submission_id):
    data = request.get_json() or {}
    requester = data.get("username")
    user = User.query.filter_by(username=requester).first() if requester else None
    submission = db.session.get(CurriculumSubmission, submission_id)
    if not submission:
        return jsonify({"message": "Submission not found"}), 404
    assignment = db.session.get(CurriculumAssignment, submission.assignment_id)
    module = db.session.get(CurriculumModule, assignment.module_id) if assignment else None
    curriculum = db.session.get(Curriculum, module.curriculum_id) if module else None
    competition = db.session.get(Competition, curriculum.competition_id) if curriculum else None
    if not assignment or not module or not curriculum or not competition:
        return jsonify({"message": "Curriculum context not found"}), 404
    if not _is_competition_instructor(user, competition):
        return jsonify({"message": "Instructor access required"}), 403

    feedback = submission.feedback_json or {}
    feedback["gradedBy"] = requester
    feedback["gradedAt"] = datetime.utcnow().isoformat()
    if "feedback" in data:
        feedback["instructorComment"] = data.get("feedback")
    if "comments" in data:
        feedback["comments"] = data.get("comments")
    if "rubric_notes" in data:
        feedback["rubricNotes"] = data.get("rubric_notes")

    if assignment.type == "assignment":
        score_input = data.get("score")
        q1_input = data.get("question_1_score", data.get("question1Score"))
        q2_input = data.get("question_2_score", data.get("question2Score"))
        if score_input is None and (q1_input is None and q2_input is None):
            return jsonify({"message": "score is required"}), 422
        try:
            if score_input is not None:
                total_score = float(score_input)
                q1_score = None
                q2_score = None
            else:
                q1_score = float(q1_input) if q1_input is not None else None
                q2_score = float(q2_input) if q2_input is not None else None
                total_score = float((q1_score or 0.0) + (q2_score or 0.0))
        except (TypeError, ValueError):
            return jsonify({"message": "score must be numeric"}), 422
        if total_score < 0:
            return jsonify({"message": "score must be greater than or equal to 0"}), 422
        if assignment.points is not None and total_score > assignment.points:
            return jsonify({"message": "score must be less than or equal to points possible"}), 422
        if q1_score is not None or q2_score is not None:
            rows = []
            if q1_score is not None:
                rows.append({"questionId": "q1", "pointsAwarded": q1_score, "pointsPossible": float(assignment.points or 0.0) / 2.0})
            if q2_score is not None:
                rows.append({"questionId": "q2", "pointsAwarded": q2_score, "pointsPossible": float(assignment.points or 0.0) / 2.0})
            try:
                computed = _upsert_submission_question_grades(
                    submission=submission,
                    assignment=assignment,
                    teacher_user_id=user.id,
                    grades=rows,
                    final_feedback=data.get("feedback"),
                    rubric_notes=data.get("rubric_notes"),
                )
            except ValueError as exc:
                return jsonify({"message": str(exc)}), 422
            if "percentage" in data and data.get("percentage") is not None:
                try:
                    submission.percentage = round(float(data.get("percentage")), 2)
                except (TypeError, ValueError):
                    return jsonify({"message": "percentage must be numeric"}), 422
            total_score = computed["pointsEarned"]
        else:
            submission.question_1_score = None
            submission.question_2_score = None
            submission.assignment_total_score = round(total_score, 2)
            submission.score = round(total_score, 2)
            if "percentage" in data and data.get("percentage") is not None:
                try:
                    submission.percentage = round(float(data.get("percentage")), 2)
                except (TypeError, ValueError):
                    return jsonify({"message": "percentage must be numeric"}), 422
            else:
                submission.percentage = round((total_score / assignment.points * 100.0) if assignment.points else 0.0, 2)
        feedback["pendingManualGrade"] = False
    else:
        return jsonify({"message": "Only written assignments can be manually graded at this endpoint"}), 422

    submission.auto_graded = False
    submission.graded_by_user_id = user.id
    submission.graded_at = datetime.utcnow()
    submission.rubric_notes = data.get("rubric_notes")
    submission.feedback_json = feedback
    db.session.commit()
    updated_summary = _compute_grade_summary(curriculum.competition_id, submission.user_id)

    return jsonify({
        "submissionId": submission.id,
        "assignmentId": assignment.id,
        "userId": submission.user_id,
        "score": submission.score,
        "pointsEarned": submission.score,
        "pointsPossible": assignment.points,
        "percentage": submission.percentage,
        "status": "graded",
        "gradingStatus": "graded",
        "autoGraded": submission.auto_graded,
        "feedback": submission.feedback_json,
        "question1Score": submission.question_1_score,
        "question2Score": submission.question_2_score,
        "assignmentTotalScore": submission.assignment_total_score,
        "gradedByUserId": submission.graded_by_user_id,
        "gradedAt": submission.graded_at.isoformat() if submission.graded_at else None,
        "rubricNotes": submission.rubric_notes,
        "isManuallyGradable": True,
        "gradeSummary": updated_summary,
    })


def _upsert_submission_question_grades(submission, assignment, teacher_user_id, grades, final_feedback=None, rubric_notes=None):
    if not isinstance(grades, list) or not grades:
        raise ValueError("grades must be a non-empty list")

    total_earned = 0.0
    total_possible = 0.0
    question_grade_rows = []
    now = datetime.utcnow()

    for row in grades:
        question_id = row.get("questionId")
        if not question_id:
            raise ValueError("questionId is required for every grade row")
        try:
            points_awarded = float(row.get("pointsAwarded"))
            points_possible = float(row.get("pointsPossible"))
        except (TypeError, ValueError):
            raise ValueError("pointsAwarded and pointsPossible must be numeric")
        if points_awarded < 0 or points_possible < 0:
            raise ValueError("pointsAwarded and pointsPossible must be non-negative")
        if points_awarded > points_possible:
            raise ValueError("pointsAwarded must be <= pointsPossible")

        existing = SubmissionQuestionGrade.query.filter_by(
            submission_id=submission.id,
            question_id=str(question_id),
        ).first()
        if not existing:
            existing = SubmissionQuestionGrade(submission_id=submission.id, question_id=str(question_id))
            db.session.add(existing)
        existing.points_awarded = round(points_awarded, 2)
        existing.points_possible = round(points_possible, 2)
        existing.feedback = row.get("feedback")
        existing.graded_by = teacher_user_id
        existing.graded_at = now
        question_grade_rows.append(existing)
        total_earned += existing.points_awarded
        total_possible += existing.points_possible

    total_earned = round(total_earned, 2)
    total_possible = round(total_possible, 2)
    percentage = round((total_earned / total_possible * 100.0), 2) if total_possible else None

    submission.score = total_earned
    submission.assignment_total_score = total_earned
    submission.percentage = percentage if percentage is not None else 0.0
    submission.auto_graded = False
    submission.graded_by_user_id = teacher_user_id
    submission.graded_at = now
    submission.rubric_notes = rubric_notes
    feedback = submission.feedback_json or {}
    feedback["pendingManualGrade"] = False
    if final_feedback is not None:
        feedback["instructorComment"] = final_feedback
    feedback["gradedAt"] = now.isoformat()
    submission.feedback_json = feedback

    # Backward-compatible fields for two-question assignments.
    if len(question_grade_rows) >= 2:
        ordered = sorted(question_grade_rows, key=lambda r: r.question_id)
        submission.question_1_score = ordered[0].points_awarded
        submission.question_2_score = ordered[1].points_awarded
    elif len(question_grade_rows) == 1:
        submission.question_1_score = question_grade_rows[0].points_awarded
        submission.question_2_score = None

    db.session.flush()
    return {
        "pointsEarned": total_earned,
        "pointsPossible": total_possible,
        "percentage": percentage,
        "status": "graded",
    }


@app.route('/teacher/submissions/<int:submission_id>/question-grades', methods=['POST'])
def teacher_submission_question_grades(submission_id):
    data = request.get_json() or {}
    requester = data.get("username")
    user = User.query.filter_by(username=requester).first() if requester else None
    submission = db.session.get(CurriculumSubmission, submission_id)
    if not submission:
        return jsonify({"message": "Submission not found"}), 404
    assignment = db.session.get(CurriculumAssignment, submission.assignment_id)
    module = db.session.get(CurriculumModule, assignment.module_id) if assignment else None
    curriculum = db.session.get(Curriculum, module.curriculum_id) if module else None
    competition = db.session.get(Competition, curriculum.competition_id) if curriculum else None
    if not assignment or not module or not curriculum or not competition:
        return jsonify({"message": "Curriculum context not found"}), 404
    if not _is_competition_instructor(user, competition):
        return jsonify({"message": "Instructor access required"}), 403
    if assignment.type not in ("assignment", "written_assignment"):
        return jsonify({"message": "Question-level grading is only supported for written assignments"}), 422

    try:
        computed = _upsert_submission_question_grades(
            submission=submission,
            assignment=assignment,
            teacher_user_id=user.id,
            grades=data.get("grades"),
            final_feedback=data.get("finalFeedback"),
            rubric_notes=data.get("rubricNotes"),
        )
        db.session.commit()
    except ValueError as exc:
        db.session.rollback()
        return jsonify({"message": str(exc)}), 422

    updated_summary = _compute_grade_summary(curriculum.competition_id, submission.user_id)
    question_rows = _submission_question_grade_rows(submission.id)
    return jsonify({
        "submissionId": submission.id,
        "assignmentId": assignment.id,
        "userId": submission.user_id,
        "status": computed["status"],
        "gradingStatus": computed["status"],
        "pointsEarned": computed["pointsEarned"],
        "pointsPossible": computed["pointsPossible"],
        "percentage": computed["percentage"],
        "finalFeedback": (submission.feedback_json or {}).get("instructorComment"),
        "questionGrades": [
            {
                "questionId": row.question_id,
                "pointsAwarded": row.points_awarded,
                "pointsPossible": row.points_possible,
                "feedback": row.feedback,
                "gradedBy": row.graded_by,
                "gradedAt": row.graded_at.isoformat() if row.graded_at else None,
            } for row in question_rows
        ],
        "gradeSummary": updated_summary,
    }), 200


@app.route('/curriculum/competition/<int:competition_id>/written-submissions', methods=['GET'])
@app.route('/curriculum/competition/<int:competition_id>/instructor/submissions', methods=['GET'])
@app.route('/curriculum/competition/<int:competition_id>/teacher/submissions', methods=['GET'])
@app.route('/curriculum/competition/<int:competition_id>/submissions', methods=['GET'])
def curriculum_competition_written_submissions(competition_id):
    requester = request.args.get("username")
    user = User.query.filter_by(username=requester).first() if requester else None
    resolved_competition_id = _resolve_curriculum_competition_id(
        competition_id,
        requester_user_id=user.id if user else None,
    )
    competition = db.session.get(Competition, resolved_competition_id)
    if not competition:
        return jsonify({"message": "Competition not found"}), 404
    if not _is_competition_instructor(user, competition):
        return jsonify({"message": "Instructor access required"}), 403

    curriculum = Curriculum.query.filter_by(competition_id=resolved_competition_id, enabled=True).first()
    if not curriculum:
        return jsonify({"message": "Curriculum not enabled for this competition"}), 404

    module_ids = [m.id for m in CurriculumModule.query.filter_by(curriculum_id=curriculum.id).all()]
    assignment_rows = CurriculumAssignment.query.filter(
        CurriculumAssignment.module_id.in_(module_ids),
        CurriculumAssignment.type == "assignment",
    ).all() if module_ids else []
    assignment_ids = [a.id for a in assignment_rows]
    submissions = CurriculumSubmission.query.filter(
        CurriculumSubmission.assignment_id.in_(assignment_ids)
    ).order_by(CurriculumSubmission.submitted_at.desc()).all() if assignment_ids else []
    assignment_map = {a.id: a for a in assignment_rows}

    payload = []
    for sub in submissions:
        student = db.session.get(User, sub.user_id)
        assignment = assignment_map.get(sub.assignment_id)
        payload.append({
            "submissionId": sub.id,
            "assignmentId": sub.assignment_id,
            "assignmentTitle": assignment.title if assignment else None,
            "moduleId": assignment.module_id if assignment else None,
            "userId": sub.user_id,
            "username": student.username if student else f"user-{sub.user_id}",
            "answers": sub.answers_json,
            "question1Score": sub.question_1_score,
            "question2Score": sub.question_2_score,
            "assignmentTotalScore": sub.assignment_total_score if sub.assignment_total_score is not None else sub.score,
            "feedback": sub.feedback_json,
            "submittedAt": sub.submitted_at.isoformat(),
            "gradingStatus": _written_submission_status(sub),
            "isManuallyGradable": True,
        })

    return jsonify({
        "competitionId": resolved_competition_id,
        "curriculumId": curriculum.id,
        "totalSubmissions": len(payload),
        "submissions": payload,
    })


@app.route('/curriculum/competition/<int:competition_id>/instructor-overview', methods=['GET'])
@app.route('/curriculum/competition/<int:competition_id>/teacher-overview', methods=['GET'])
@app.route('/curriculum/competition/<int:competition_id>/instructor/overview', methods=['GET'])
@app.route('/curriculum/competition/<int:competition_id>/teacher/overview', methods=['GET'])
def curriculum_instructor_overview(competition_id):
    requester = request.args.get("username")
    user = User.query.filter_by(username=requester).first() if requester else None
    resolved_competition_id = _resolve_curriculum_competition_id(
        competition_id,
        requester_user_id=user.id if user else None,
    )
    competition = db.session.get(Competition, resolved_competition_id)
    if not competition:
        return jsonify({"message": "Competition not found"}), 404
    if not _is_competition_instructor(user, competition):
        return jsonify({"message": "Instructor access required"}), 403
    curriculum = Curriculum.query.filter_by(competition_id=resolved_competition_id, enabled=True).first()
    if not curriculum:
        return jsonify({"message": "Curriculum not enabled for this competition"}), 404
    modules = CurriculumModule.query.filter_by(curriculum_id=curriculum.id).order_by(CurriculumModule.week_number.asc()).all()
    module_ids = [m.id for m in modules]
    assignments = CurriculumAssignment.query.filter(CurriculumAssignment.module_id.in_(module_ids)).all() if module_ids else []
    assignment_ids = [a.id for a in assignments]
    member_ids = [m.user_id for m in CompetitionMember.query.filter_by(competition_id=resolved_competition_id).all()]
    submissions = CurriculumSubmission.query.filter(
        CurriculumSubmission.assignment_id.in_(assignment_ids),
        CurriculumSubmission.user_id.in_(member_ids)
    ).all() if assignment_ids and member_ids else []

    by_user = {}
    for uid in member_ids:
        by_user[uid] = {"earned": 0.0, "submitted": 0, "moduleGrades": []}
        user_summary = _compute_grade_summary(resolved_competition_id, uid)
        if user_summary:
            by_user[uid]["earned"] = user_summary.get("totalPointsEarned", 0.0)
            by_user[uid]["possible"] = user_summary.get("totalPointsPossible", 0.0)
            percentage = user_summary.get("percentage")
            by_user[uid]["percentage"] = percentage if percentage is not None else 0.0
            by_user[uid]["moduleGrades"] = user_summary.get("moduleGrades", [])
        else:
            by_user[uid]["possible"] = 0.0
            by_user[uid]["percentage"] = 0.0
    for sub in submissions:
        by_user[sub.user_id]["submitted"] += 1

    student_rows = []
    for uid in member_ids:
        user_obj = db.session.get(User, uid)
        earned = by_user[uid]["earned"]
        possible = by_user[uid]["possible"]
        submitted = by_user[uid]["submitted"]
        pct = by_user[uid]["percentage"]
        completion = (submitted / len(assignments) * 100.0) if assignments else 0.0
        student_rows.append({
            "userId": uid,
            "username": user_obj.username if user_obj else f"user-{uid}",
            "totalPointsEarned": round(earned, 2),
            "totalPointsPossible": round(possible, 2),
            "percentage": round(pct, 2),
            "completionRate": round(completion, 2),
            "moduleGrades": by_user[uid]["moduleGrades"],
        })

    class_avg = round(sum(row["percentage"] for row in student_rows) / len(student_rows), 2) if student_rows else 0.0
    class_completion = round(sum(row["completionRate"] for row in student_rows) / len(student_rows), 2) if student_rows else 0.0
    assignment_rows = []
    written_submissions_rows = []
    for assignment in assignments:
        assignment_subs = [s for s in submissions if s.assignment_id == assignment.id]
        avg_score = round(sum(s.score for s in assignment_subs) / len(assignment_subs), 2) if assignment_subs else 0.0
        assignment_rows.append({
            "assignmentId": assignment.id,
            "title": assignment.title,
            "type": assignment.type,
            "pointsPossible": assignment.points,
            "submissionCount": len(assignment_subs),
            "averageScore": avg_score,
        })
        if assignment.type == "assignment":
            for sub in assignment_subs:
                student = db.session.get(User, sub.user_id)
                written_submissions_rows.append({
                    "submissionId": sub.id,
                    "assignmentId": assignment.id,
                    "assignmentTitle": assignment.title,
                    "moduleId": assignment.module_id,
                    "userId": sub.user_id,
                    "username": student.username if student else f"user-{sub.user_id}",
                    "answers": sub.answers_json,
                    "question1Score": sub.question_1_score,
                    "question2Score": sub.question_2_score,
                    "assignmentTotalScore": sub.assignment_total_score if sub.assignment_total_score is not None else sub.score,
                    "feedback": sub.feedback_json,
                    "submittedAt": sub.submitted_at.isoformat(),
                })

    recent_submissions = sorted(submissions, key=lambda s: s.submitted_at, reverse=True)[:25]
    recent_rows = []
    for sub in recent_submissions:
        assignment = next((a for a in assignments if a.id == sub.assignment_id), None)
        student = db.session.get(User, sub.user_id)
        recent_rows.append({
            "submissionId": sub.id,
            "assignmentId": sub.assignment_id,
            "assignmentTitle": assignment.title if assignment else None,
            "userId": sub.user_id,
            "username": student.username if student else f"user-{sub.user_id}",
            "score": sub.score,
            "percentage": sub.percentage,
            "submittedAt": sub.submitted_at.isoformat(),
            "autoGraded": sub.auto_graded,
        })

    return jsonify({
        "competitionId": resolved_competition_id,
        "curriculumId": curriculum.id,
        "students": student_rows,
        "assignments": assignment_rows,
        "writtenAssignmentSubmissions": written_submissions_rows,
        "recentSubmissions": recent_rows,
        "classAveragePercentage": class_avg,
        "classCompletionRate": class_completion,
        "totalAssignments": len(assignments),
        "totalStudents": len(member_ids),
    })


@app.route('/curriculum/submissions', methods=['GET'])
def curriculum_submissions_by_query():
    competition_id = request.args.get("competition_id", type=int)
    if competition_id is None:
        return jsonify({"message": "competition_id is required"}), 400
    return curriculum_competition_written_submissions(competition_id)


@app.route('/curriculum/competition/<int:competition_id>/teacher/roster', methods=['GET'])
@app.route('/curriculum/competition/<int:competition_id>/instructor/roster', methods=['GET'])
def curriculum_teacher_roster(competition_id):
    requester = request.args.get("username")
    if not requester:
        return jsonify({"message": "Authentication required"}), 401
    requesting_user = User.query.filter_by(username=requester).first()
    if not requesting_user:
        return jsonify({"message": "Invalid credentials"}), 401

    resolved_competition_id = _resolve_curriculum_competition_id(
        competition_id,
        requester_user_id=requesting_user.id,
    )
    competition = db.session.get(Competition, resolved_competition_id)
    if not competition:
        return jsonify({"message": "Competition not found"}), 404
    if not _is_competition_instructor(requesting_user, competition):
        return jsonify({"message": "Instructor access required"}), 403

    curriculum = Curriculum.query.filter_by(competition_id=resolved_competition_id, enabled=True).first()
    if not curriculum:
        return jsonify({"message": "Curriculum not enabled for this competition"}), 404

    members = CompetitionMember.query.filter_by(competition_id=resolved_competition_id).all()
    member_ids = [m.user_id for m in members]
    users = User.query.filter(User.id.in_(member_ids)).all() if member_ids else []
    users_by_id = {u.id: u for u in users}
    grade_rows = _build_teacher_grade_rows(competition, curriculum, member_ids)

    roster = []
    for uid in member_ids:
        user = users_by_id.get(uid)
        grade = grade_rows.get(uid, {})
        latest_quiz = None
        latest_written = None
        for item in grade.get("items", []):
            if item.get("assignmentType") in ("quiz", "exam") and (latest_quiz is None or (item.get("submittedAt") or "") > (latest_quiz.get("submittedAt") or "")):
                latest_quiz = item
            if item.get("assignmentType") in ("assignment", "written_assignment") and (latest_written is None or (item.get("submittedAt") or "") > (latest_written.get("submittedAt") or "")):
                latest_written = item
        roster.append({
            "userId": uid,
            "displayName": user.username if user else f"user-{uid}",
            "email": user.email if user else None,
            "curriculumPercentage": grade.get("curriculumPercentage"),
            "percentage": grade.get("curriculumPercentage"),
            "letterGrade": grade.get("letterGrade", "N/A"),
            "totalPointsEarned": grade.get("totalPointsEarned", 0.0),
            "totalPointsPossible": grade.get("totalPointsPossible", 0.0),
            "completedQuizzes": grade.get("completedQuizzes", 0),
            "completedAssignments": grade.get("completedAssignments", 0),
            "completedCurriculumItems": grade.get("completedCurriculumItems", 0),
            "progressPercentage": grade.get("progressPercentage", 0.0),
            "totalCurriculumItems": grade.get("totalCurriculumItems", 0),
            "hasTrades": grade.get("hasTrades", False),
            "tradeCount": grade.get("tradeCount", 0),
            "latestQuizSubmission": latest_quiz,
            "latestWrittenSubmission": latest_written,
            "grade_summary_overall": grade.get("grade_summary_overall", {}),
            "gradeSummaryOverall": grade.get("grade_summary_overall", {}),
            "grade_summary_by_module": grade.get("grade_summary_by_module", []),
            "gradeSummaryByModule": grade.get("grade_summary_by_module", []),
        })

    return jsonify({
        "competitionId": resolved_competition_id,
        "competition_id": resolved_competition_id,
        "is_instructor_for_competition": _is_competition_instructor(requesting_user, competition),
        "curriculumId": curriculum.id,
        "roster": roster,
    }), 200


@app.route('/curriculum/competition/<int:competition_id>/teacher/students/<int:student_id>', methods=['GET'])
@app.route('/curriculum/competition/<int:competition_id>/instructor/students/<int:student_id>', methods=['GET'])
def curriculum_teacher_student_detail(competition_id, student_id):
    requester = request.args.get("username")
    if not requester:
        return jsonify({"message": "Authentication required"}), 401
    requesting_user = User.query.filter_by(username=requester).first()
    if not requesting_user:
        return jsonify({"message": "Invalid credentials"}), 401

    resolved_competition_id = _resolve_curriculum_competition_id(
        competition_id,
        requester_user_id=requesting_user.id,
    )
    competition = db.session.get(Competition, resolved_competition_id)
    if not competition:
        return jsonify({"message": "Competition not found"}), 404
    if not _is_competition_instructor(requesting_user, competition):
        return jsonify({"message": "Instructor access required"}), 403

    curriculum = Curriculum.query.filter_by(competition_id=resolved_competition_id, enabled=True).first()
    if not curriculum:
        return jsonify({"message": "Curriculum not enabled for this competition"}), 404

    membership = CompetitionMember.query.filter_by(competition_id=resolved_competition_id, user_id=student_id).first()
    if not membership:
        return jsonify({"message": "Student not found in competition"}), 404
    student = db.session.get(User, student_id)
    if not student:
        return jsonify({"message": "User not found"}), 404

    grade_rows = _build_teacher_grade_rows(competition, curriculum, [student_id])
    grade = grade_rows.get(student_id) or {}
    items = grade.get("items", [])

    return jsonify({
        "competitionId": resolved_competition_id,
        "competition_id": resolved_competition_id,
        "is_instructor_for_competition": _is_competition_instructor(requesting_user, competition),
        "curriculumId": curriculum.id,
        "student": {
            "userId": student.id,
            "displayName": student.username,
            "email": student.email,
        },
        "gradeSummary": {
            "curriculumPercentage": grade.get("curriculumPercentage"),
            "percentage": grade.get("curriculumPercentage"),
            "letterGrade": grade.get("letterGrade", "N/A"),
            "totalPointsEarned": grade.get("totalPointsEarned", 0.0),
            "totalPointsPossible": grade.get("totalPointsPossible", 0.0),
            "completedQuizzes": grade.get("completedQuizzes", 0),
            "completedAssignments": grade.get("completedAssignments", 0),
            "completedCurriculumItems": grade.get("completedCurriculumItems", 0),
            "progressPercentage": grade.get("progressPercentage", 0.0),
            "totalCurriculumItems": grade.get("totalCurriculumItems", 0),
            "hasTrades": grade.get("hasTrades", False),
            "tradeCount": grade.get("tradeCount", 0),
            "grade_summary_overall": grade.get("grade_summary_overall", {}),
            "gradeSummaryOverall": grade.get("grade_summary_overall", {}),
            "grade_summary_by_module": grade.get("grade_summary_by_module", []),
            "gradeSummaryByModule": grade.get("grade_summary_by_module", []),
        },
        "grade_summary_overall": grade.get("grade_summary_overall", {}),
        "gradeSummaryOverall": grade.get("grade_summary_overall", {}),
        "grade_summary_by_module": grade.get("grade_summary_by_module", []),
        "gradeSummaryByModule": grade.get("grade_summary_by_module", []),
        "items": items,
    }), 200


@app.route('/curriculum/competition/<int:competition_id>/teacher/students/<int:student_id>/trades', methods=['GET'])
def curriculum_teacher_student_trades(competition_id, student_id):
    requester = request.args.get("username")
    if not requester:
        return jsonify({"message": "Authentication required"}), 401
    requesting_user = User.query.filter_by(username=requester).first()
    if not requesting_user:
        return jsonify({"message": "Invalid credentials"}), 401

    resolved_competition_id = _resolve_curriculum_competition_id(
        competition_id,
        requester_user_id=requesting_user.id,
    )
    competition = db.session.get(Competition, resolved_competition_id)
    if not competition:
        return jsonify({"message": "Competition not found"}), 404
    if not _is_competition_instructor(requesting_user, competition):
        return jsonify({"message": "Instructor access required"}), 403

    membership = CompetitionMember.query.filter_by(competition_id=resolved_competition_id, user_id=student_id).first()
    if not membership:
        return jsonify({"message": "Student not found in competition"}), 404

    entries = TradeBlotterEntry.query.filter(
        TradeBlotterEntry.user_id == student_id,
        TradeBlotterEntry.account_context == f"competition:{competition.code}",
    ).order_by(TradeBlotterEntry.created_at.desc()).all()

    rows = []
    for entry in entries:
        serialized = _serialize_trade_blotter_entry(entry)
        rows.append({
            "tradeId": serialized["id"],
            "timestamp": serialized["executed_at"],
            "symbol": serialized["symbol"],
            "side": serialized["side"],
            "quantity": serialized["quantity"],
            "price": serialized["price"],
            "orderType": serialized["order_type"],
            "status": "filled",
            "accountName": serialized["account_display_name"],
            "accountContext": serialized["account_context"],
        })

    return jsonify({
        "competitionId": resolved_competition_id,
        "studentId": student_id,
        "trades": rows,
    }), 200


@app.route('/competition/buy', methods=['POST'])
def competition_buy():
    data = request.get_json()
    username = data.get('username')
    competition_code = data.get('competition_code')
    competition_id = data.get('competition_id')
    symbol = data.get('symbol').upper()
    quantity = int(data.get('quantity'))

    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    comp = None
    if competition_id is not None:
        try:
            comp = db.session.get(Competition, int(competition_id))
        except (TypeError, ValueError):
            return jsonify({'message': 'Invalid competition id'}), 400
    if comp is None and competition_code:
        comp = Competition.query.filter_by(code=str(competition_code).strip()).first()
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
        account_context=f'competition:{comp.code}',
    )
    db.session.commit()
    grade_summary = _compute_grade_summary(comp.id, user.id)
    return jsonify({
        'message': 'Competition buy successful',
        'competition_cash': member.cash_balance,
        'gradeSummary': grade_summary,
    })



@app.route('/competition/sell', methods=['POST'])
def competition_sell():
    
    data = request.get_json()
    username = data.get('username')
    competition_code = data.get('competition_code')
    competition_id = data.get('competition_id')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))

    # 1. Find user
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    # 2. Find competition
    comp = None
    if competition_id is not None:
        try:
            comp = db.session.get(Competition, int(competition_id))
        except (TypeError, ValueError):
            return jsonify({'message': 'Invalid competition id'}), 400
    if comp is None and competition_code:
        comp = Competition.query.filter_by(code=str(competition_code).strip()).first()
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
        account_context=f'competition:{comp.code}',
    )
    db.session.commit()
    grade_summary = _compute_grade_summary(comp.id, user.id)

    return jsonify({
        'message': 'Competition sell successful',
        'competition_cash': member.cash_balance,
        'gradeSummary': grade_summary,
    })


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
@app.route('/competition/join_team', methods=['POST'])
def competition_team_join():
    data = request.get_json() or {}
    username = data.get('username')
    # Accept both `team_code` and `team_id` for compatibility with older/newer clients.
    team_code = data.get('team_code') or data.get('team_id')
    competition_code = data.get('competition_code') or data.get('code')
    competition_id = data.get('competition_id')

    if not username or not team_code or (not competition_code and not competition_id):
        return jsonify({'message': 'username, team_code/team_id, and competition_code/code/competition_id are required'}), 400

    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    try:
        team_code = int(team_code)
    except (TypeError, ValueError):
        return jsonify({'message': 'Invalid team code'}), 400

    team = Team.query.filter_by(id=team_code).first()
    if not team:
        return jsonify({'message': 'Team not found'}), 404
    if not TeamMember.query.filter_by(team_id=team.id, user_id=user.id).first():
        return jsonify({'message': 'User is not a member of this team'}), 403

    comp = None
    if competition_id is not None:
        try:
            comp = db.session.get(Competition, int(competition_id))
        except (TypeError, ValueError):
            return jsonify({'message': 'Invalid competition id'}), 400
    if comp is None and competition_code:
        comp = Competition.query.filter_by(code=str(competition_code).strip()).first()
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
    competition_id = data.get('competition_id')
    team_id = data.get('team_id')
    symbol = data.get('symbol').upper()
    quantity = int(data.get('quantity'))

    # 1. Find the user
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    # 2. Find the competition
    comp = None
    if competition_id is not None:
        try:
            comp = db.session.get(Competition, int(competition_id))
        except (TypeError, ValueError):
            return jsonify({'message': 'Invalid competition id'}), 400
    if comp is None and competition_code:
        comp = Competition.query.filter_by(code=str(competition_code).strip()).first()
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
    competition_id = data.get('competition_id')
    team_id = data.get('team_id')
    symbol = data.get('symbol')
    quantity = int(data.get('quantity'))

    # 1. Find the user
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'message': 'User not found'}), 404

    # 2. Find the competition
    comp = None
    if competition_id is not None:
        try:
            comp = db.session.get(Competition, int(competition_id))
        except (TypeError, ValueError):
            return jsonify({'message': 'Invalid competition id'}), 400
    if comp is None and competition_code:
        comp = Competition.query.filter_by(code=str(competition_code).strip()).first()
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
    data = [_serialize_competition_identity(c, requesting_user=admin_user) for c in competitions]
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
        _delete_curriculum_for_competition(comp.id)

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
    competition_id = data.get('competition_id')
    is_open = data.get('is_open')
    
    admin_user = User.query.filter_by(username=admin_username).first()
    if not admin_user or not admin_user.is_admin:
        return jsonify({'message': 'Not authorized'}), 403

    comp = None
    if competition_id is not None:
        try:
            comp = db.session.get(Competition, int(competition_id))
        except (TypeError, ValueError):
            return jsonify({'message': 'Invalid competition id'}), 400
    if comp is None and competition_code:
        comp = Competition.query.filter_by(code=str(competition_code).strip()).first()
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
    competition_id = data.get('competition_id')
    
    # ✅ Validate admin
    admin_user = User.query.filter_by(username=admin_username).first()
    if not admin_user or not admin_user.is_admin:
        return jsonify({'message': 'Not authorized'}), 403

    # ✅ Find target user
    target_user = User.query.filter_by(username=target_username).first()
    if not target_user:
        return jsonify({'message': 'Target user not found'}), 404

    # ✅ Find competition
    comp = None
    if competition_id is not None:
        try:
            comp = db.session.get(Competition, int(competition_id))
        except (TypeError, ValueError):
            return jsonify({'message': 'Invalid competition id'}), 400
    if comp is None and competition_code:
        comp = Competition.query.filter_by(code=str(competition_code).strip()).first()
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
    requester = request.args.get("username")
    requesting_user = User.query.filter_by(username=requester).first() if requester else None
    competitions = Competition.query.all()
    competitions_data = [_serialize_competition_identity(comp, requesting_user=requesting_user) for comp in competitions]
    return jsonify(competitions_data)


@app.route('/competition/by_code/<string:competition_code>', methods=['GET'])
def get_competition_by_code(competition_code):
    requester = request.args.get("username")
    requesting_user = User.query.filter_by(username=requester).first() if requester else None
    competition = Competition.query.filter_by(code=str(competition_code).strip()).first()
    if not competition:
        return jsonify({'message': 'Competition not found'}), 404
    return jsonify(_serialize_competition_identity(competition, requesting_user=requesting_user)), 200

# --------------------
# Featured Competitions Endpoint (updated)
# --------------------
@app.route('/featured_competitions', methods=['GET'])
def get_featured_competitions():
    requester = request.args.get("username")
    requesting_user = User.query.filter_by(username=requester).first() if requester else None
    try:
        featured = Competition.query.filter_by(featured=True).all()
        result = []
        for comp in featured:
            result.append(_serialize_competition_identity(comp, requesting_user=requesting_user))
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
    account_labels = _resolve_account_labels_for_user(entry.user_id, account_context)
    return {
        "id": entry.id,
        "symbol": entry.symbol,
        "side": entry.side,
        "quantity": entry.quantity,
        "price": entry.price,
        "order_type": entry.order_type,
        "account_context": account_context,
        "account": account_context,
        "account_id": account_labels["account_id"],
        "account_type": account_labels["account_type"],
        "account_display_name": account_labels["account_display_name"],
        "competition_code": account_labels["competition_code"],
        "competition_name": account_labels["competition_name"],
        "team_name": account_labels["team_name"],
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


def _resolve_account_labels_for_user(user_id, account_context):
    normalized = (account_context or "global").strip()
    default_payload = {
        "account_id": normalized,
        "account_type": "global",
        "account_display_name": "Global Account",
        "competition_code": None,
        "competition_name": None,
        "team_name": None,
    }

    if normalized == "global":
        return default_payload

    if normalized.startswith("competition:"):
        competition_code = normalized.split(":", 1)[1].strip()
        if not competition_code:
            return default_payload
        row = (
            db.session.query(CompetitionMember.id, Competition.code, Competition.name)
            .join(Competition, Competition.id == CompetitionMember.competition_id)
            .filter(CompetitionMember.user_id == user_id, Competition.code == competition_code)
            .first()
        )
        if not row:
            return {
                **default_payload,
                "account_id": normalized,
                "account_type": "competition",
                "competition_code": competition_code,
                "account_display_name": competition_code,
            }
        account_id, code, name = row
        return {
            "account_id": account_id,
            "account_type": "competition",
            "account_display_name": name or code,
            "competition_code": code,
            "competition_name": name,
            "team_name": None,
        }

    if normalized.startswith("competition_team:"):
        parts = normalized.split(":")
        if len(parts) < 3:
            return default_payload
        competition_code = parts[1].strip()
        try:
            team_id = int(parts[2])
        except ValueError:
            team_id = None

        row = (
            db.session.query(CompetitionTeam.id, Competition.code, Competition.name, Team.name)
            .join(Competition, Competition.id == CompetitionTeam.competition_id)
            .join(Team, Team.id == CompetitionTeam.team_id)
            .join(TeamMember, TeamMember.team_id == Team.id)
            .filter(TeamMember.user_id == user_id)
        )
        if competition_code:
            row = row.filter(Competition.code == competition_code)
        if team_id is not None:
            row = row.filter(Team.id == team_id)
        row = row.first()

        if not row:
            return {
                **default_payload,
                "account_id": normalized,
                "account_type": "team_competition",
                "competition_code": competition_code or None,
                "account_display_name": normalized,
            }

        account_id, code, comp_name, team_name = row
        display_name = f"{team_name} • {comp_name or code}"
        return {
            "account_id": account_id,
            "account_type": "team_competition",
            "account_display_name": display_name,
            "competition_code": code,
            "competition_name": comp_name,
            "team_name": team_name,
        }

    return default_payload


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
# Daily snapshot after market close using latest available end-of-day prices.
# 5:15 PM America/New_York gives a short buffer after the 4:00 PM close.
scheduler.add_job(
    func=run_daily_account_performance_snapshot_job,
    trigger="cron",
    hour=17,
    minute=15,
    timezone="America/New_York"
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
