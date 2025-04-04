#!/usr/bin/env python
"""
API Key Request Utility for WizData

This utility helps check for necessary API keys and provides instructions for obtaining them.
Run this script to check which API keys are set and to get instructions for any missing ones.
"""
import os
import sys
import logging
from typing import Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# API Key information
API_KEY_INFO = {
    # Financial Data APIs
    "ALPHA_VANTAGE_API_KEY": {
        "provider": "Alpha Vantage",
        "description": "Provides real-time and historical financial data across various asset classes.",
        "signup_url": "https://www.alphavantage.co/support/#api-key",
        "documentation_url": "https://www.alphavantage.co/documentation/",
        "free_tier": "Yes, 5 API requests per minute, 500 per day",
        "required": True
    },
    "SP_GLOBAL_API_KEY": {
        "provider": "S&P Global Market Intelligence",
        "description": "Premium financial data, indices, company information, and market analysis.",
        "signup_url": "https://www.spglobal.com/marketintelligence/en/solutions/api-data-delivery",
        "documentation_url": "https://developer.marketplace.spglobal.com/",
        "free_tier": "No, enterprise pricing required",
        "required": False
    },
    "BLOOMBERG_API_KEY": {
        "provider": "Bloomberg Data Services",
        "description": "Comprehensive financial data including real-time market data, news, research, and analytics.",
        "signup_url": "https://www.bloomberg.com/professional/product/data-distribution/",
        "documentation_url": "https://www.bloomberg.com/professional/support/api-library/",
        "free_tier": "No, enterprise pricing required",
        "required": False
    },
    "REFINITIV_API_KEY": {
        "provider": "Refinitiv / LSEG Data & Analytics",
        "description": "Comprehensive financial data including real-time market data, fundamentals, news, and analytics.",
        "signup_url": "https://www.lseg.com/en/data-analytics/developer-portal",
        "documentation_url": "https://developers.lseg.com/",
        "free_tier": "No, enterprise pricing required",
        "required": False
    },
    "REFINITIV_API_SECRET": {
        "provider": "Refinitiv / LSEG Data & Analytics",
        "description": "Secret key for Refinitiv API authentication.",
        "signup_url": "https://www.lseg.com/en/data-analytics/developer-portal",
        "documentation_url": "https://developers.lseg.com/",
        "free_tier": "No, enterprise pricing required",
        "required": False
    },
    
    # ESG and World Data APIs
    "WORLD_BANK_API_KEY": {
        "provider": "World Bank Data",
        "description": "Provides global economic, social, and environmental data.",
        "signup_url": "https://datahelpdesk.worldbank.org/knowledgebase/articles/889392-api-documentation",
        "documentation_url": "https://datahelpdesk.worldbank.org/knowledgebase/articles/889392-api-documentation",
        "free_tier": "Yes, public access with some rate limits",
        "required": False
    },
    
    # AI/ML APIs
    "OPENAI_API_KEY": {
        "provider": "OpenAI",
        "description": "Provides access to AI models for text analysis, insights, and sentiment analysis.",
        "signup_url": "https://platform.openai.com/signup",
        "documentation_url": "https://platform.openai.com/docs/",
        "free_tier": "No, pay-as-you-go pricing",
        "required": False
    }
}

def check_api_keys() -> Tuple[List[str], List[str]]:
    """
    Check which API keys are set in the environment
    
    Returns:
        Tuple containing lists of: (available keys, missing keys)
    """
    available_keys = []
    missing_keys = []
    
    for key_name in API_KEY_INFO.keys():
        if os.environ.get(key_name):
            available_keys.append(key_name)
        else:
            missing_keys.append(key_name)
    
    return available_keys, missing_keys

def display_key_status(available_keys: List[str], missing_keys: List[str]) -> None:
    """
    Display the status of API keys
    
    Args:
        available_keys: List of available API keys
        missing_keys: List of missing API keys
    """
    logger.info("=== WizData API Key Status ===")
    
    if available_keys:
        logger.info("\nAvailable API Keys:")
        for key in available_keys:
            logger.info(f"✓ {key} ({API_KEY_INFO[key]['provider']})")
    
    if missing_keys:
        logger.info("\nMissing API Keys:")
        for key in missing_keys:
            required_str = "[REQUIRED]" if API_KEY_INFO[key]["required"] else "[OPTIONAL]"
            logger.info(f"✗ {key} ({API_KEY_INFO[key]['provider']}) {required_str}")

def display_key_instructions(key_name: str) -> None:
    """
    Display instructions for obtaining a specific API key
    
    Args:
        key_name: Name of the API key
    """
    if key_name not in API_KEY_INFO:
        logger.error(f"Unknown API key: {key_name}")
        return
    
    key_info = API_KEY_INFO[key_name]
    
    logger.info(f"\n=== Instructions for {key_name} ===")
    logger.info(f"Provider: {key_info['provider']}")
    logger.info(f"Description: {key_info['description']}")
    logger.info(f"Sign-up URL: {key_info['signup_url']}")
    logger.info(f"Documentation: {key_info['documentation_url']}")
    logger.info(f"Free Tier Available: {key_info['free_tier']}")
    logger.info(f"Required: {'Yes' if key_info['required'] else 'No'}")
    
    logger.info("\nSteps to obtain this API key:")
    
    if key_name == "ALPHA_VANTAGE_API_KEY":
        logger.info("1. Go to Alpha Vantage's website: https://www.alphavantage.co/support/#api-key")
        logger.info("2. Fill out the form with your name and email address")
        logger.info("3. Click 'Get Free API Key'")
        logger.info("4. Check your email for the API key")
        logger.info("5. Set the environment variable: export ALPHA_VANTAGE_API_KEY=your_key_here")
    
    elif key_name == "SP_GLOBAL_API_KEY":
        logger.info("1. Contact S&P Global directly for enterprise access")
        logger.info("2. Complete their client onboarding process")
        logger.info("3. After approval, you'll receive API credentials")
        logger.info("4. Set the environment variable: export SP_GLOBAL_API_KEY=your_key_here")
    
    elif key_name == "BLOOMBERG_API_KEY":
        logger.info("1. Contact Bloomberg's sales team for access")
        logger.info("2. Sign up for a Bloomberg Terminal subscription")
        logger.info("3. Request API access and credentials")
        logger.info("4. Complete their onboarding process")
        logger.info("5. Set the environment variable: export BLOOMBERG_API_KEY=your_key_here")
    
    elif key_name in ["REFINITIV_API_KEY", "REFINITIV_API_SECRET"]:
        logger.info("1. Contact LSEG Data & Analytics (formerly Refinitiv) for enterprise access")
        logger.info("2. Complete their client onboarding process")
        logger.info("3. After approval, you'll receive API credentials")
        logger.info("4. Set the environment variables:")
        logger.info("   export REFINITIV_API_KEY=your_key_here")
        logger.info("   export REFINITIV_API_SECRET=your_secret_here")
    
    elif key_name == "WORLD_BANK_API_KEY":
        logger.info("1. World Bank Data API has public access with some rate limits")
        logger.info("2. For higher rate limits, contact the World Bank Data Help Desk")
        logger.info("3. Set the environment variable: export WORLD_BANK_API_KEY=your_key_here")
    
    elif key_name == "OPENAI_API_KEY":
        logger.info("1. Go to OpenAI's website: https://platform.openai.com/signup")
        logger.info("2. Create an account or sign in")
        logger.info("3. Navigate to the API keys section: https://platform.openai.com/api-keys")
        logger.info("4. Click 'Create new secret key'")
        logger.info("5. Name your key and copy it (you won't be able to see it again)")
        logger.info("6. Set the environment variable: export OPENAI_API_KEY=your_key_here")

def main() -> None:
    """Main function to check API keys and display instructions"""
    available_keys, missing_keys = check_api_keys()
    display_key_status(available_keys, missing_keys)
    
    if missing_keys:
        logger.info("\nSome API keys are missing. For instructions on how to obtain them, run:")
        logger.info("python request_api_keys.py <KEY_NAME>")
        
        required_missing = [k for k in missing_keys if API_KEY_INFO[k]["required"]]
        if required_missing:
            logger.warning("\nWARNING: The following required API keys are missing:")
            for key in required_missing:
                logger.warning(f"  - {key} ({API_KEY_INFO[key]['provider']})")
            logger.warning("The application may not function correctly without these keys.")
    
    # If a specific key was requested, show instructions for it
    if len(sys.argv) > 1:
        key_name = sys.argv[1].upper()
        display_key_instructions(key_name)

if __name__ == "__main__":
    main()