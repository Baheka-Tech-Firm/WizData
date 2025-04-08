"""
AI-Powered Insights API Routes

This module implements the routes for the AI-powered insights wizard feature,
providing step-by-step guidance for data exploration and analysis.
"""

import json
from datetime import datetime
from typing import Dict, List, Optional, Union

from flask import jsonify, request

from src.ai.insights_wizard import wizard

def start_session():
    """
    Start a new insights wizard session
    
    POST data:
    - user_name (str, optional): User name for personalized experience
    - focus_area (str, optional): Initial focus area ('ESG', 'Market Analysis')
    
    Returns:
        JSON response with session details
    """
    try:
        data = request.get_json()
        
        # Extract parameters
        user_name = data.get('user_name') if data else None
        focus_area = data.get('focus_area') if data else None
        
        # Start session
        result = wizard.start_session(user_name, focus_area)
        
        return jsonify(result)
    
    except Exception as e:
        return jsonify({
            "error": "Session Creation Failed",
            "message": str(e)
        }), 500


def analyze_data():
    """
    Get AI-powered insights on specific data
    
    POST data:
    - data (dict/list): The data to analyze
    - question (str): The specific question or analysis request
    
    Returns:
        JSON response with insights
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                "error": "Invalid Request",
                "message": "Missing request data"
            }), 400
            
        # Extract parameters
        input_data = data.get('data')
        question = data.get('question')
        
        if not input_data:
            return jsonify({
                "error": "Invalid Request",
                "message": "Missing 'data' parameter"
            }), 400
            
        if not question:
            return jsonify({
                "error": "Invalid Request",
                "message": "Missing 'question' parameter"
            }), 400
            
        # Analyze data
        result = wizard.analyze_data(input_data, question)
        
        return jsonify(result)
    
    except Exception as e:
        return jsonify({
            "error": "Analysis Failed",
            "message": str(e)
        }), 500


def get_analysis_guide():
    """
    Get a step-by-step guide for a specific type of analysis
    
    Query parameters:
    - analysis_type (str): The type of analysis (e.g., 'ESG comparison')
    - complexity (str, optional): The complexity level ('beginner', 'intermediate', 'advanced')
    
    Returns:
        JSON response with step-by-step guide
    """
    try:
        # Extract parameters
        analysis_type = request.args.get('analysis_type')
        complexity = request.args.get('complexity', 'intermediate')
        
        if not analysis_type:
            return jsonify({
                "error": "Invalid Request",
                "message": "Missing 'analysis_type' parameter"
            }), 400
            
        # Get guide
        result = wizard.get_analysis_guide(analysis_type, complexity)
        
        return jsonify(result)
    
    except Exception as e:
        return jsonify({
            "error": "Guide Generation Failed",
            "message": str(e)
        }), 500


def answer_question():
    """
    Answer a specific question about data or analysis methods
    
    POST data:
    - question (str): The question to answer
    - context (dict, optional): Optional context information
    
    Returns:
        JSON response with answer and guidance
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                "error": "Invalid Request",
                "message": "Missing request data"
            }), 400
            
        # Extract parameters
        question = data.get('question')
        context = data.get('context')
        
        if not question:
            return jsonify({
                "error": "Invalid Request",
                "message": "Missing 'question' parameter"
            }), 400
            
        # Answer question
        result = wizard.answer_question(question, context)
        
        return jsonify(result)
    
    except Exception as e:
        return jsonify({
            "error": "Question Answering Failed",
            "message": str(e)
        }), 500


def generate_narrative():
    """
    Generate a narrative explanation of data trends or patterns
    
    POST data:
    - data (dict/list): The data to explain
    - narrative_type (str, optional): Type of narrative ('trends_summary', 'comparison', 'forecast')
    
    Returns:
        JSON response with narrative explanation
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                "error": "Invalid Request",
                "message": "Missing request data"
            }), 400
            
        # Extract parameters
        input_data = data.get('data')
        narrative_type = data.get('narrative_type', 'trends_summary')
        
        if not input_data:
            return jsonify({
                "error": "Invalid Request",
                "message": "Missing 'data' parameter"
            }), 400
            
        # Generate narrative
        result = wizard.generate_narrative(input_data, narrative_type)
        
        return jsonify(result)
    
    except Exception as e:
        return jsonify({
            "error": "Narrative Generation Failed",
            "message": str(e)
        }), 500


def suggest_comparison():
    """
    Suggest comparative analysis between different regions or metrics
    
    Query parameters:
    - data_type (str): Type of data for analysis ('esg', 'market', 'financial')
    - regions (list, optional): Comma-separated list of regions to focus on
    - metrics (list, optional): Comma-separated list of metrics to compare
    
    Returns:
        JSON response with suggested comparative analysis
    """
    try:
        # Extract parameters
        data_type = request.args.get('data_type', 'esg')
        regions = request.args.get('regions')
        metrics = request.args.get('metrics')
        
        # Suggest comparison
        result = wizard.suggest_comparison(data_type, regions, metrics)
        
        return jsonify(result)
    
    except Exception as e:
        return jsonify({
            "error": "Comparison Suggestion Failed",
            "message": str(e)
        }), 500


def get_history():
    """
    Get the history of the current session
    
    Returns:
        JSON response with session history
    """
    try:
        # Extract parameters
        session_id = request.args.get('session_id')
        
        if not session_id:
            return jsonify({
                "error": "Invalid Request",
                "message": "Missing 'session_id' parameter"
            }), 400
            
        # Get history
        result = wizard.get_history(session_id)
        
        return jsonify(result)
    
    except Exception as e:
        return jsonify({
            "error": "History Retrieval Failed",
            "message": str(e)
        }), 500