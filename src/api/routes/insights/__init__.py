"""
Insights API Routes

This module provides routes for the AI-powered insights wizard feature.
"""

from flask import Blueprint

insights_bp = Blueprint("insights", __name__, url_prefix="/api/insights")

# Import all routes
from src.api.routes.insights.insights_routes import *

# Register routes with the blueprint
insights_bp.add_url_rule("/session/start", view_func=start_session, methods=["POST"])
insights_bp.add_url_rule("/data/analyze", view_func=analyze_data, methods=["POST"])
insights_bp.add_url_rule("/guide", view_func=get_analysis_guide, methods=["GET"])
insights_bp.add_url_rule("/question/answer", view_func=answer_question, methods=["POST"])
insights_bp.add_url_rule("/narrative", view_func=generate_narrative, methods=["POST"])
insights_bp.add_url_rule("/comparison/suggest", view_func=suggest_comparison, methods=["GET"])
insights_bp.add_url_rule("/history", view_func=get_history, methods=["GET"])