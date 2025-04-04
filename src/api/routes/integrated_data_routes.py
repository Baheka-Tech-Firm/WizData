"""
Integrated Data API Routes

These routes provide access to integrated financial and ESG data from
multiple sources through a unified API.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from flask import Blueprint, request, jsonify, Response, send_file
import pandas as pd
from io import StringIO

from src.integration.data_manager import DataManager

# Setup logging
logger = logging.getLogger(__name__)

# Create Blueprint
integrated_data_bp = Blueprint('integrated_data', __name__)

# Initialize data manager
data_manager = DataManager()

@integrated_data_bp.route('/api/integrated/stock/<symbol>', methods=['GET'])
def get_stock_data(symbol: str):
    """
    Get integrated stock data including financial and ESG metrics
    
    URL Parameters:
    - symbol: Stock symbol
    
    Query Parameters:
    - country_code: Country code for ESG data (optional)
    - format: Output format (json or csv, default: json)
    """
    try:
        country_code = request.args.get('country_code')
        output_format = request.args.get('format', 'json').lower()
        
        # Get stock data
        stock_data = data_manager.get_stock_data(symbol)
        
        # If country code is provided, add ESG impact analysis
        if country_code:
            result = data_manager.get_market_esg_impact(symbol, country_code)
        else:
            result = {
                "symbol": symbol,
                "company_name": stock_data["company_info"].get("Name", symbol),
                "timestamp": datetime.now().isoformat(),
                "stock_data": {
                    "latest_price": stock_data["time_series"][0]["close"] if stock_data["time_series"] else None,
                    "change_30d": data_manager._calculate_price_change(stock_data["time_series"], 30) if stock_data["time_series"] else None,
                    "change_90d": data_manager._calculate_price_change(stock_data["time_series"], 90) if stock_data["time_series"] else None,
                    "change_1y": data_manager._calculate_price_change(stock_data["time_series"], 365) if stock_data["time_series"] else None
                },
                "time_series": stock_data["time_series"],
                "company_info": stock_data["company_info"]
            }
        
        # Format output
        if output_format == 'csv':
            if not country_code:
                # Create DataFrame from time series
                df = pd.DataFrame(stock_data["time_series"])
                csv_data = df.to_csv(index=False)
                
                # Create response with CSV data
                output = StringIO()
                output.write(csv_data)
                output.seek(0)
                
                return Response(
                    output.getvalue(),
                    mimetype="text/csv",
                    headers={"Content-disposition": f"attachment; filename={symbol}_stock_data.csv"}
                )
            else:
                # For combined data, create a more complex CSV
                # First create a DataFrame with stock data
                stock_df = pd.DataFrame(stock_data["time_series"])
                
                # Then add ESG scores
                esg_scores = result["esg_scores"]
                esg_df = pd.DataFrame([esg_scores])
                
                # Combine into one CSV with sections
                output = StringIO()
                output.write(f"# Stock Data for {symbol}\n")
                stock_df.to_csv(output, index=False)
                output.write(f"\n\n# ESG Scores for {result['country_name']}\n")
                esg_df.to_csv(output, index=False)
                output.seek(0)
                
                return Response(
                    output.getvalue(),
                    mimetype="text/csv",
                    headers={"Content-disposition": f"attachment; filename={symbol}_integrated_data.csv"}
                )
        else:
            return jsonify(result)
    except Exception as e:
        logger.error(f"Error getting integrated stock data: {str(e)}")
        return jsonify({"error": str(e)}), 500

@integrated_data_bp.route('/api/integrated/sectors', methods=['GET'])
def get_sector_performance():
    """
    Get sector performance data
    
    Query Parameters:
    - format: Output format (json or csv, default: json)
    """
    try:
        output_format = request.args.get('format', 'json').lower()
        
        # Get sector data
        sector_data = data_manager.get_sector_performance()
        
        # Format output
        if output_format == 'csv':
            # Convert sector data to DataFrame
            # This requires some data restructuring
            sectors_dict = {}
            for key, value in sector_data["data"].items():
                if key == "Meta Data":
                    continue
                    
                # Extract time period and create a dictionary for each sector
                time_period = key.split(". ")[1]
                if time_period not in sectors_dict:
                    sectors_dict[time_period] = {}
                
                for sector, performance in value.items():
                    # Clean sector name
                    sector_name = sector.replace("GICS ", "").strip()
                    sectors_dict[time_period][sector_name] = float(performance.replace("%", ""))
            
            # Create DataFrame
            df = pd.DataFrame(sectors_dict)
            csv_data = df.to_csv(index=True)
            
            # Create response with CSV data
            output = StringIO()
            output.write(csv_data)
            output.seek(0)
            
            return Response(
                output.getvalue(),
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=sector_performance.csv"}
            )
        else:
            return jsonify(sector_data)
    except Exception as e:
        logger.error(f"Error getting sector performance: {str(e)}")
        return jsonify({"error": str(e)}), 500

@integrated_data_bp.route('/api/integrated/forex', methods=['GET'])
def get_forex_data():
    """
    Get forex exchange rate data
    
    Query Parameters:
    - from_currency: From currency code (required)
    - to_currency: To currency code (required)
    - format: Output format (json or csv, default: json)
    """
    try:
        from_currency = request.args.get('from_currency')
        to_currency = request.args.get('to_currency')
        output_format = request.args.get('format', 'json').lower()
        
        if not from_currency or not to_currency:
            return jsonify({"error": "from_currency and to_currency parameters are required"}), 400
        
        # Get forex data
        forex_data = data_manager.get_forex_data(from_currency, to_currency)
        
        # Format output
        if output_format == 'csv':
            # Create a simple CSV with the exchange rate
            output = StringIO()
            output.write(f"from_currency,to_currency,exchange_rate,last_refreshed\n")
            
            # Extract data from the response
            exchange_rate_data = forex_data["data"].get("Realtime Currency Exchange Rate", {})
            exchange_rate = exchange_rate_data.get("5. Exchange Rate", "")
            last_refreshed = exchange_rate_data.get("6. Last Refreshed", "")
            
            output.write(f"{from_currency},{to_currency},{exchange_rate},{last_refreshed}\n")
            output.seek(0)
            
            return Response(
                output.getvalue(),
                mimetype="text/csv",
                headers={"Content-disposition": f"attachment; filename={from_currency}_{to_currency}_forex.csv"}
            )
        else:
            return jsonify(forex_data)
    except Exception as e:
        logger.error(f"Error getting forex data: {str(e)}")
        return jsonify({"error": str(e)}), 500

@integrated_data_bp.route('/api/integrated/crypto/<symbol>', methods=['GET'])
def get_crypto_data(symbol: str):
    """
    Get cryptocurrency data
    
    URL Parameters:
    - symbol: Cryptocurrency symbol
    
    Query Parameters:
    - format: Output format (json or csv, default: json)
    """
    try:
        output_format = request.args.get('format', 'json').lower()
        
        # Get crypto data
        crypto_data = data_manager.get_crypto_data(symbol)
        
        # Format output
        if output_format == 'csv':
            # Structure for CSV conversion depends on the actual data structure
            # This is a simplified example
            output = StringIO()
            output.write(f"# Cryptocurrency Data for {symbol}\n")
            output.write("date,open,high,low,close,volume\n")
            
            # Extract time series data
            time_series_key = next((k for k in crypto_data["data"].keys() if "Time Series" in k), None)
            if time_series_key:
                time_series = crypto_data["data"][time_series_key]
                for date, values in time_series.items():
                    open_price = values.get("1a. open (USD)", "")
                    high = values.get("2a. high (USD)", "")
                    low = values.get("3a. low (USD)", "")
                    close = values.get("4a. close (USD)", "")
                    volume = values.get("5. volume", "")
                    output.write(f"{date},{open_price},{high},{low},{close},{volume}\n")
            
            output.seek(0)
            
            return Response(
                output.getvalue(),
                mimetype="text/csv",
                headers={"Content-disposition": f"attachment; filename={symbol}_crypto.csv"}
            )
        else:
            return jsonify(crypto_data)
    except Exception as e:
        logger.error(f"Error getting crypto data: {str(e)}")
        return jsonify({"error": str(e)}), 500

@integrated_data_bp.route('/api/integrated/esg/countries', methods=['GET'])
def get_countries_list():
    """
    Get list of countries with ESG data available
    """
    try:
        countries = data_manager.get_countries_list()
        return jsonify(countries)
    except Exception as e:
        logger.error(f"Error getting countries list: {str(e)}")
        return jsonify({"error": str(e)}), 500

@integrated_data_bp.route('/api/integrated/esg/country/<country_code>', methods=['GET'])
def get_country_esg_data(country_code: str):
    """
    Get ESG data for a country
    
    URL Parameters:
    - country_code: Country code (ISO 3166-1 alpha-2 or alpha-3)
    
    Query Parameters:
    - format: Output format (json or csv, default: json)
    """
    try:
        output_format = request.args.get('format', 'json').lower()
        
        # Get country ESG data
        esg_data = data_manager.get_country_esg_data(country_code)
        
        # Format output
        if output_format == 'csv':
            # Create CSV with ESG scores
            output = StringIO()
            output.write(f"# ESG Scores for {esg_data['country_info'].get('name', country_code)}\n")
            output.write("dimension,score\n")
            
            for dimension, score in esg_data["esg_scores"].items():
                output.write(f"{dimension},{score}\n")
                
            output.write(f"\n\n# Raw ESG Indicators\n")
            
            # Add environmental indicators
            output.write(f"\n## Environmental Indicators\n")
            output.write("indicator,value,date\n")
            for indicator in esg_data["esg_data"]["environmental"]:
                indicator_name = indicator["indicator"]["value"]
                value = indicator["value"]
                date = indicator["date"]
                output.write(f"{indicator_name},{value},{date}\n")
                
            # Add social indicators
            output.write(f"\n## Social Indicators\n")
            output.write("indicator,value,date\n")
            for indicator in esg_data["esg_data"]["social"]:
                indicator_name = indicator["indicator"]["value"]
                value = indicator["value"]
                date = indicator["date"]
                output.write(f"{indicator_name},{value},{date}\n")
                
            # Add governance indicators
            output.write(f"\n## Governance Indicators\n")
            output.write("indicator,value,date\n")
            for indicator in esg_data["esg_data"]["governance"]:
                indicator_name = indicator["indicator"]["value"]
                value = indicator["value"]
                date = indicator["date"]
                output.write(f"{indicator_name},{value},{date}\n")
                
            # Add infrastructure indicators
            output.write(f"\n## Infrastructure Indicators\n")
            output.write("indicator,value,date\n")
            for indicator in esg_data["esg_data"]["infrastructure"]:
                indicator_name = indicator["indicator"]["value"]
                value = indicator["value"]
                date = indicator["date"]
                output.write(f"{indicator_name},{value},{date}\n")
                
            output.seek(0)
            
            return Response(
                output.getvalue(),
                mimetype="text/csv",
                headers={"Content-disposition": f"attachment; filename={country_code}_esg_data.csv"}
            )
        else:
            return jsonify(esg_data)
    except Exception as e:
        logger.error(f"Error getting country ESG data: {str(e)}")
        return jsonify({"error": str(e)}), 500

@integrated_data_bp.route('/api/integrated/esg/region/<region_code>/scores', methods=['GET'])
def get_region_esg_scores(region_code: str):
    """
    Get ESG scores for a region
    
    URL Parameters:
    - region_code: Region code (country or custom region)
    
    Query Parameters:
    - year: Year for the data (optional)
    - format: Output format (json or csv, default: json)
    """
    try:
        year = request.args.get('year')
        if year:
            year = int(year)
        output_format = request.args.get('format', 'json').lower()
        
        # Get region ESG scores
        esg_scores = data_manager.get_esg_scores_for_region(region_code, year)
        
        # Format output
        if output_format == 'csv':
            # Create CSV with ESG scores
            output = StringIO()
            output.write(f"# ESG Scores for {esg_scores['region_name']} ({esg_scores['year']})\n")
            output.write("dimension,score\n")
            
            for dimension, score in esg_scores["scores"].items():
                output.write(f"{dimension},{score}\n")
                
            output.seek(0)
            
            return Response(
                output.getvalue(),
                mimetype="text/csv",
                headers={"Content-disposition": f"attachment; filename={region_code}_esg_scores_{esg_scores['year']}.csv"}
            )
        else:
            return jsonify(esg_scores)
    except Exception as e:
        logger.error(f"Error getting region ESG scores: {str(e)}")
        return jsonify({"error": str(e)}), 500

@integrated_data_bp.route('/api/integrated/esg/compare', methods=['GET'])
def compare_esg_scores():
    """
    Compare ESG scores for multiple regions
    
    Query Parameters:
    - regions: Comma-separated list of region codes (required)
    - year: Year for the data (optional)
    - dimension: ESG dimension to compare (optional)
    - format: Output format (json or csv, default: json)
    """
    try:
        regions_param = request.args.get('regions')
        if not regions_param:
            return jsonify({"error": "regions parameter is required"}), 400
            
        region_codes = regions_param.split(',')
        
        year = request.args.get('year')
        if year:
            year = int(year)
            
        dimension = request.args.get('dimension')
        output_format = request.args.get('format', 'json').lower()
        
        # Get comparative ESG scores
        comparison = data_manager.compare_esg_scores(region_codes, year, dimension)
        
        # Format output
        if output_format == 'csv':
            # Create CSV with comparative ESG scores
            output = StringIO()
            output.write(f"# Comparative ESG Scores ({comparison['year']})\n")
            
            if dimension:
                # Single dimension comparison
                output.write(f"region_code,region_name,{dimension}_score\n")
                for region_code, data in comparison["regions"].items():
                    output.write(f"{region_code},{data['region_name']},{data['score']}\n")
            else:
                # All dimensions comparison
                output.write("region_code,region_name,environmental,social,governance,infrastructure,overall\n")
                for region_code, data in comparison["regions"].items():
                    scores = data["scores"]
                    output.write(f"{region_code},{data['region_name']},{scores['environmental']},{scores['social']},{scores['governance']},{scores['infrastructure']},{scores['overall']}\n")
                    
            output.seek(0)
            
            file_suffix = f"_{dimension}" if dimension else ""
            return Response(
                output.getvalue(),
                mimetype="text/csv",
                headers={"Content-disposition": f"attachment; filename=esg_comparison{file_suffix}_{comparison['year']}.csv"}
            )
        else:
            return jsonify(comparison)
    except Exception as e:
        logger.error(f"Error comparing ESG scores: {str(e)}")
        return jsonify({"error": str(e)}), 500