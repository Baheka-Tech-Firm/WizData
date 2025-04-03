import os
import logging
from flask import Flask, render_template, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Database setup
class Base(DeclarativeBase):
    pass

db = SQLAlchemy(model_class=Base)

# Create the Flask application
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev_secret_key")

# Configure database
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_recycle": 300,
    "pool_pre_ping": True,
}

# Initialize database with app
db.init_app(app)

# Routes
@app.route('/')
def index():
    """Render the dashboard page"""
    return render_template('dashboard.html')

@app.route('/jobs')
def jobs():
    """Render the jobs page"""
    # This would be implemented in a future version
    return render_template('dashboard.html')  # Placeholder

@app.route('/sources')
def sources():
    """Render the data sources page"""
    # This would be implemented in a future version
    return render_template('dashboard.html')  # Placeholder

@app.route('/api/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "version": "1.0.0",
        "service": "WizData Web Dashboard"
    })

# Create database tables
with app.app_context():
    logger.info("Creating database tables if they don't exist")
    db.create_all()
    logger.info("Database setup complete")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)