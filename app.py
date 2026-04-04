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
    """Best-effort additive schema sync for deployments without migrations."""
    try:
        insp = inspect(db.engine)
        table_names = insp.get_table_names()

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
        if 'curriculum_module' in table_names:
            existing_cols = {c['name'] for c in insp.get_columns('curriculum_module')}
            if 'lesson_content' not in existing_cols:
                db.session.execute(text('ALTER TABLE curriculum_module ADD COLUMN lesson_content TEXT'))
        if 'curriculum_submission' in table_names:
            existing_cols = {c['name'] for c in insp.get_columns('curriculum_submission')}
            submission_needed = {
                'question_1_score': 'DOUBLE PRECISION',
                'question_2_score': 'DOUBLE PRECISION',
                'assignment_total_score': 'DOUBLE PRECISION',
                'graded_by_user_id': 'INTEGER',
                'graded_at': 'TIMESTAMP',
                'rubric_notes': 'TEXT',
            }
            for col_name, col_type in submission_needed.items():
                if col_name in existing_cols:
                    continue
                db.session.execute(text(f'ALTER TABLE curriculum_submission ADD COLUMN {col_name} {col_type}'))
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
        })
    elif "risk and return" in lower_title:
        plan.update({
            "hook": "Return is attractive, but risk determines whether your process is durable.",
            "core_terms": [
                ("Volatility", "The variability of returns around average."),
                ("Drawdown", "Peak-to-trough decline that tests both capital and psychology."),
                ("Risk-adjusted return", "Return evaluated relative to the risk required to earn it."),
            ],
            "likely_confusion": "Strong trailing returns are often mistaken for low future risk after extended rallies.",
            "scenario": "Two portfolios both return 10%, but one suffered a -28% drawdown and one only -8%. Decide which process is stronger and why.",
        })
    elif "diversification" in lower_title:
        plan.update({
            "hook": "Diversification is about different risk behaviors, not just more ticker symbols.",
            "core_terms": [
                ("Correlation", "How assets move relative to one another."),
                ("Idiosyncratic risk", "Single-company risk that can be diversified away."),
                ("Concentration", "Overdependence on one position, sector, or factor."),
            ],
            "likely_confusion": "Owning many names in one theme is still concentrated risk.",
            "scenario": "Your portfolio holds eight names, but most risk is concentrated in one growth factor exposure.",
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
        })
    elif "fundamental analysis" in lower_title:
        plan.update({
            "hook": "Fundamental analysis asks: what is this business worth, and where is the market likely mispricing it?",
            "core_terms": [
                ("Revenue quality", "Sustainable growth with sound unit economics beats one-time spikes."),
                ("Margin structure", "Operating leverage can amplify both upside and downside."),
                ("Valuation multiple", "Price paid relative to earnings, cash flow, or sales."),
            ],
            "likely_confusion": "Great businesses can still be poor investments when valuation assumptions are stretched.",
            "scenario": "Two firms grow similarly, but one converts cash better with lower leverage. Determine investability and required margin of safety.",
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
        })

    return plan


def _lesson_content_for_module(module_title, module_description, week_number):
    plan = _module_teaching_plan(module_title, module_description)
    terms_block = "\n".join([f"- **{term}:** {meaning}" for term, meaning in plan["core_terms"]])
    walkthrough_block = "\n".join([f"{idx}. {step}" for idx, step in enumerate(plan["walkthrough"], start=1)])
    quiz_focus_block = "\n".join([f"- {item}" for item in plan["quiz_focus"]])

    return (
        f"## Week {week_number} eText: {module_title}\n\n"
        f"{module_description} {plan['hook']}\n\n"
        "You are not being graded on bold predictions; you are being graded on decision quality you can repeat. "
        "In this course, strong investing means a clear thesis, measurable evidence, and disciplined risk controls.\n\n"
        "### Core concepts you need to own\n"
        f"{terms_block}\n\n"
        "### Why this matters in your simulator account\n"
        f"{plan['scenario']}\n\n"
        "### Step-by-step decision workflow\n"
        f"{walkthrough_block}\n\n"
        "### Frequent mistake to avoid\n"
        f"{plan['likely_confusion']} Write your thesis, size, and invalidation *before* execution so your process does not get rewritten by emotion.\n\n"
        "### Quant toolkit (use in quiz + assignment)\n"
        "- **Holding period return:** (Ending Value - Beginning Value + Cash Flows) / Beginning Value\n"
        "- **Portfolio weight:** Position Market Value / Total Portfolio Value\n"
        "- **Contribution to return:** Position Weight × Position Return\n"
        "- **Excess return vs benchmark:** Portfolio Return - Benchmark Return\n"
        "- **Concentration check (Top-3 weight):** Sum of top 3 position weights\n"
        "Every number needs interpretation: What does it imply for risk, and what action follows?\n\n"
        "### Quiz alignment map (20 points)\n"
        "Expect decision-based multiple-choice questions on:\n"
        f"{quiz_focus_block}\n\n"
        "### Assignment alignment map (2 questions, 10 points each)\n"
        "- **A1:** Deep qualitative trade critique using thesis, evidence, risk, and execution quality.\n"
        "- **A2:** Quantitative portfolio analysis with return math, allocation breakdown, and contribution interpretation.\n\n"
        "### What an excellent submission looks like\n"
        "It is specific, evidence-driven, and self-critical. It includes calculations, states assumptions, explains tradeoffs, and closes with a clear process upgrade for your next trade.\n\n"
        "### Exit ticket\n"
        f"{plan['application_prompt']}"
    )


def _assignment_content_for_module(module_title):
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


def _quiz_content_for_module(module_title, question_count=20):
    plan = _module_teaching_plan(module_title, "")
    term1, term2, _term3 = [term for term, _ in plan["core_terms"]]
    stem_scenario = plan["scenario"]

    bank = [
        (f"Which statement best reflects this module's core mindset?", [
            "Judge decisions by process quality and evidence, not just outcome.",
            "Chase the highest recent return with full size.",
            "Avoid benchmarks because they reduce confidence.",
            "Rebuild your rules after each trade outcome.",
        ], "Judge decisions by process quality and evidence, not just outcome."),
        (f"In this module, {term1.lower()} is best defined as:", [
            f"{term1} is a probability-based estimate rather than a certainty.",
            f"{term1} is the same as last month's realized return.",
            f"{term1} means ignoring downside to maximize upside.",
            f"{term1} is only relevant for short-term traders.",
        ], f"{term1} is a probability-based estimate rather than a certainty."),
        (f"Why is documenting an invalidation rule before entry important?", [
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
        (f"What is the strongest reason to monitor {term2.lower()} proactively?", [
            f"Because {term2.lower()} determines whether your strategy can survive adverse paths.",
            f"Because {term2.lower()} only matters after the semester ends.",
            f"Because {term2.lower()} can be ignored when conviction is high.",
            f"Because {term2.lower()} is identical across all assets.",
        ], f"Because {term2.lower()} determines whether your strategy can survive adverse paths."),
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
        (f"{stem_scenario} What is the most disciplined first action?", [
            "Re-check thesis evidence and position limits before increasing risk.",
            "Increase every position equally to avoid regret.",
            "Ignore valuation and focus only on momentum.",
            "Wait for social confirmation before deciding.",
        ], "Re-check thesis evidence and position limits before increasing risk."),
    ]

    questions = []
    answer_key = {}
    for idx in range(question_count):
        stem, choices, correct = bank[idx % len(bank)]
        qid = f"q{idx + 1}"
        questions.append({"id": qid, "prompt": stem, "choices": choices})
        answer_key[qid] = correct

    return {
        "instructions": f"{module_title} quiz. Select one best answer for each question based on this week's eText and assignment expectations.",
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
        )
        db.session.add(module)
        db.session.flush()

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
            overwrite=bool(data.get("overwrite", False)),
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
    modules = CurriculumModule.query.filter_by(curriculum_id=curriculum.id).order_by(CurriculumModule.week_number.asc()).all()
    payload = []
    for module in modules:
        assignments = CurriculumAssignment.query.filter_by(module_id=module.id).all()
        payload.append({
            "moduleId": module.id,
            "weekNumber": module.week_number,
            "title": module.title,
            "description": module.description,
            "lessonContent": module.lesson_content,
            "lesson_content": module.lesson_content,
            "unlockDate": module.unlock_date.isoformat(),
            "dueDate": module.due_date.isoformat(),
            "assignments": [{
                "assignmentId": a.id,
                "type": a.type,
                "title": a.title,
                "points": a.points,
                "content": a.content_json,
                "content_json": a.content_json,
                "answer_key_json": a.answer_key_json,
            } for a in assignments]
        })
    return jsonify(payload)


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
    try:
        normalized_answers = _normalize_submission_answers(assignment, answers)
    except ValueError as exc:
        return jsonify({"message": str(exc)}), 422

    score = 0.0
    auto_graded = False
    feedback = {"lateSubmission": datetime.utcnow() > module.due_date}
    if assignment.type in ("quiz", "exam"):
        answer_key = _quiz_answer_key_map(assignment)
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
    submission = CurriculumSubmission.query.filter_by(assignment_id=assignment_id, user_id=user.id).first()
    if not submission:
        submission = CurriculumSubmission(
            assignment_id=assignment_id,
            user_id=user.id,
            competition_id=curriculum.competition_id,
            answers_json=normalized_answers,
        )
        db.session.add(submission)
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
