"""
AI-Powered Insights Wizard

This module provides AI-powered data analysis and insights generation
for financial and ESG data. It uses the OpenAI API to process and analyze
data, generate narratives, and provide step-by-step guidance for data exploration.
"""

import json
import os
import random
from datetime import datetime

from openai import OpenAI

# Initialize OpenAI client
# the newest OpenAI model is "gpt-4o" which was released May 13, 2024.
# do not change this unless explicitly requested by the user
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
MODEL = "gpt-4o"  # Using the latest model

class InsightsWizard:
    """
    InsightsWizard class for AI-powered data analysis and insights generation.
    """
    def __init__(self):
        self.sessions = {}
        
    def start_session(self, user_name=None, focus_area=None):
        """
        Start a new insights wizard session.
        
        Args:
            user_name (str, optional): User name for personalized experience
            focus_area (str, optional): Initial focus area ('ESG', 'Market Analysis')
            
        Returns:
            dict: Session details
        """
        session_id = f"session_{datetime.now().strftime('%Y%m%d%H%M%S')}_{random.randint(1000, 9999)}"
        
        # Default focus areas and guidance
        focus_areas = {
            "ESG": {
                "welcome": "Welcome to the ESG Data Analysis Wizard",
                "guidance": "I'll help you explore environmental, social, and governance data for various regions.",
                "suggested_next_steps": [
                    "What ESG metrics are available?",
                    "How do I compare ESG scores between regions?",
                    "What are the top performing regions in environmental metrics?",
                    "How have social metrics evolved over time?",
                    "What governance factors impact overall ESG scores the most?"
                ]
            },
            "Market Analysis": {
                "welcome": "Welcome to the Market Data Analysis Wizard",
                "guidance": "I'll help you analyze market data, identify trends, and compare market performance.",
                "suggested_next_steps": [
                    "What markets are available for analysis?",
                    "How do I analyze stock price trends?",
                    "What are the best performing stocks in a particular market?",
                    "How do I compare dividend yields across different markets?",
                    "What correlation exists between earnings reports and stock prices?"
                ]
            },
            "Comparative": {
                "welcome": "Welcome to the Comparative Analysis Wizard",
                "guidance": "I'll help you compare data across different dimensions, regions, and time periods.",
                "suggested_next_steps": [
                    "How do I compare ESG metrics across different regions?",
                    "What's the correlation between environmental factors and market performance?",
                    "How do infrastructure investments impact ESG scores?",
                    "What regions show similar patterns in social metrics?",
                    "How do governance factors compare between developed and emerging markets?"
                ]
            },
            "General": {
                "welcome": "Welcome to the WizData Insights Wizard",
                "guidance": "I'll help you explore our data and provide insights for your analysis needs.",
                "suggested_next_steps": [
                    "What type of data is available?",
                    "How do I get started with data analysis?",
                    "Can you recommend interesting insights to explore?",
                    "What are the key metrics available in the platform?",
                    "How can I export data for further analysis?"
                ]
            }
        }
        
        # Use default general area if no focus area specified or invalid
        selected_focus = "General"
        if focus_area and focus_area in focus_areas:
            selected_focus = focus_area
            
        # Create session
        self.sessions[session_id] = {
            "id": session_id,
            "user_name": user_name,
            "focus_area": selected_focus,
            "created_at": datetime.now().isoformat(),
            "history": []
        }
        
        # Personalize welcome if user name provided
        welcome = focus_areas[selected_focus]["welcome"]
        if user_name:
            welcome = f"{welcome}, {user_name}!"
            
        return {
            "session_id": session_id,
            "welcome": welcome,
            "guidance": focus_areas[selected_focus]["guidance"],
            "suggested_next_steps": focus_areas[selected_focus]["suggested_next_steps"]
        }
        
    def analyze_data(self, data, question):
        """
        Analyze data using AI to provide insights.
        
        Args:
            data (dict/list): The data to analyze
            question (str): The specific question or analysis request
            
        Returns:
            dict: Analysis results with insights
        """
        try:
            # Convert data to JSON string for the prompt
            data_str = json.dumps(data, indent=2)
            
            # Create prompt for OpenAI
            prompt = f"""
            You are a financial and ESG data analysis expert.
            Please analyze the following data and answer this question: "{question}"
            
            DATA:
            {data_str}
            
            Analyze the data and provide:
            1. Key insights (provide 3-5 bullet points)
            2. A detailed explanation
            3. Suggested visualizations that would help understand the data better
            4. Next analytical steps or questions to explore
            
            Format your response as JSON with the following structure:
            {{
                "key_insights": ["insight 1", "insight 2", ...],
                "explanation": "detailed explanation here",
                "suggested_visualizations": ["visualization 1", "visualization 2", ...],
                "next_analysis_steps": ["next step 1", "next step 2", ...]
            }}
            """
            
            # Call OpenAI API
            response = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            
            # Parse response
            result = json.loads(response.choices[0].message.content)
            return result
        
        except Exception as e:
            return {
                "error": "Analysis Failed",
                "message": f"Failed to analyze data: {str(e)}"
            }
            
    def get_analysis_guide(self, analysis_type, complexity="intermediate"):
        """
        Get a step-by-step guide for a specific type of analysis.
        
        Args:
            analysis_type (str): The type of analysis (e.g., 'ESG comparison')
            complexity (str, optional): The complexity level ('beginner', 'intermediate', 'advanced')
            
        Returns:
            dict: Step-by-step guide
        """
        try:
            # Create prompt for OpenAI
            prompt = f"""
            You are a financial and ESG data analysis expert.
            Create a detailed step-by-step guide for: "{analysis_type}"
            The guide should be at {complexity} level.
            
            Format your response as JSON with the following structure:
            {{
                "analysis_title": "Title of the analysis",
                "complexity": "beginner|intermediate|advanced",
                "overview": "Brief overview of what this analysis achieves",
                "prerequisites": ["prerequisite 1", "prerequisite 2", ...],
                "steps": [
                    {{
                        "step_number": 1,
                        "title": "Step title",
                        "description": "Detailed step description",
                        "code_example": "Optional code example (if applicable)",
                        "tip": "Optional tip or best practice"
                    }},
                    ...
                ],
                "expected_outcome": "What the user should achieve by following this guide",
                "next_level_analysis": "Suggestions for more advanced analysis after this"
            }}
            
            Create a comprehensive guide with 4-7 steps. Include code examples where appropriate
            using Python and the WizData API. Ensure each step is detailed enough to be actionable.
            """
            
            # Call OpenAI API
            response = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            
            # Parse response
            result = json.loads(response.choices[0].message.content)
            return result
        
        except Exception as e:
            return {
                "error": "Guide Generation Failed",
                "message": f"Failed to generate analysis guide: {str(e)}"
            }
            
    def answer_question(self, question, context=None):
        """
        Answer a specific question about data or analysis methods.
        
        Args:
            question (str): The question to answer
            context (dict, optional): Optional context information
            
        Returns:
            dict: Answer with guidance
        """
        try:
            # Prepare context information if provided
            context_str = ""
            if context:
                context_str = f"\nContext: {json.dumps(context, indent=2)}"
                
            # Create prompt for OpenAI
            prompt = f"""
            You are a financial and ESG data analysis expert working with WizData.
            Please answer this question: "{question}"{context_str}
            
            Your answer should include:
            1. A clear direct answer to the question
            2. Related concepts or information that might be helpful
            3. Relevant data sources or API endpoints in WizData that could help
            4. Suggested follow-up questions
            
            Format your response as JSON with the following structure:
            {{
                "answer": "detailed answer here",
                "related_concepts": ["concept 1", "concept 2", ...],
                "relevant_data_sources": ["source 1", "source 2", ...],
                "suggested_next_questions": ["question 1", "question 2", ...]
            }}
            
            Base your answer on your knowledge of financial and ESG data analysis. 
            Be specific about WizData features and available data sources (African markets, global markets, ESG data, etc.).
            """
            
            # Call OpenAI API
            response = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            
            # Parse response
            result = json.loads(response.choices[0].message.content)
            return result
        
        except Exception as e:
            return {
                "error": "Question Answering Failed",
                "message": f"Failed to answer question: {str(e)}"
            }
            
    def generate_narrative(self, data, narrative_type="trends_summary"):
        """
        Generate a narrative explanation of data trends or patterns.
        
        Args:
            data (dict/list): The data to explain
            narrative_type (str, optional): Type of narrative ('trends_summary', 'comparison', 'forecast')
            
        Returns:
            dict: Narrative explanation
        """
        try:
            # Convert data to JSON string for the prompt
            data_str = json.dumps(data, indent=2)
            
            # Define narrative types
            narrative_prompts = {
                "trends_summary": "Create a narrative summary that explains the key trends and patterns in the data.",
                "comparison": "Create a comparative narrative that highlights the similarities and differences between various elements in the data.",
                "forecast": "Create a forward-looking narrative that projects potential future developments based on the data."
            }
            
            # Get appropriate narrative prompt or use default
            narrative_prompt = narrative_prompts.get(
                narrative_type, 
                narrative_prompts["trends_summary"]
            )
            
            # Create prompt for OpenAI
            prompt = f"""
            You are a financial and ESG data analysis expert.
            {narrative_prompt}
            
            DATA:
            {data_str}
            
            Format your response as JSON with the following structure:
            {{
                "title": "Narrative title",
                "summary": "Executive summary (1-2 sentences)",
                "detailed_narrative": "Detailed narrative explanation (3-5 paragraphs)",
                "key_data_points": ["key data point 1", "key data point 2", ...],
                "limitations": "Limitations of this analysis"
            }}
            
            Ensure your narrative is clear, insightful, and backed by the data provided.
            Avoid speculation unless specifically generating a forecast narrative.
            """
            
            # Call OpenAI API
            response = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            
            # Parse response
            result = json.loads(response.choices[0].message.content)
            return result
        
        except Exception as e:
            return {
                "error": "Narrative Generation Failed",
                "message": f"Failed to generate narrative: {str(e)}"
            }
            
    def suggest_comparison(self, data_type, regions=None, metrics=None):
        """
        Suggest comparative analysis between different regions or metrics.
        
        Args:
            data_type (str): Type of data for analysis ('esg', 'market', 'financial')
            regions (list, optional): List of regions to focus on
            metrics (list, optional): List of metrics to compare
            
        Returns:
            dict: Suggested comparative analysis
        """
        try:
            # Prepare regions and metrics information if provided
            regions_str = ""
            if regions:
                if isinstance(regions, str):
                    regions = regions.split(',')
                regions_str = f"\nRegions to focus on: {', '.join(regions)}"
                
            metrics_str = ""
            if metrics:
                if isinstance(metrics, str):
                    metrics = metrics.split(',')
                metrics_str = f"\nMetrics to focus on: {', '.join(metrics)}"
                
            # Create prompt for OpenAI
            prompt = f"""
            You are a financial and ESG data analysis expert.
            Suggest a comparative analysis approach for {data_type} data.{regions_str}{metrics_str}
            
            Format your response as JSON with the following structure:
            {{
                "analysis_title": "Title of the comparative analysis",
                "objective": "What this analysis aims to achieve",
                "methodology": "Brief explanation of the methodology",
                "data_requirements": ["requirement 1", "requirement 2", ...],
                "suggested_comparisons": [
                    {{
                        "comparison_name": "Name of the specific comparison",
                        "elements_to_compare": ["element 1", "element 2", ...],
                        "metrics": ["metric 1", "metric 2", ...],
                        "visualization": "Suggested visualization type",
                        "expected_insights": "What insights this comparison might reveal"
                    }},
                    ...
                ],
                "interpretation_guidance": "How to interpret the results",
                "potential_api_endpoints": ["endpoint 1", "endpoint 2", ...]
            }}
            
            Provide 2-4 specific comparative analyses that would yield valuable insights.
            Be specific about the WizData platform data sources and features.
            """
            
            # Call OpenAI API
            response = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            
            # Parse response
            result = json.loads(response.choices[0].message.content)
            return result
        
        except Exception as e:
            return {
                "error": "Comparison Suggestion Failed",
                "message": f"Failed to suggest comparison: {str(e)}"
            }
            
    def get_history(self, session_id):
        """
        Get the history of a specific session.
        
        Args:
            session_id (str): The session ID
            
        Returns:
            dict: Session history
        """
        if session_id not in self.sessions:
            return {
                "error": "Session Not Found",
                "message": "The specified session does not exist."
            }
            
        return {
            "session_id": session_id,
            "history": self.sessions[session_id]["history"]
        }


# Create a singleton instance of the Insights Wizard
wizard = InsightsWizard()