"""
Integrated Data API Routes

These routes provide access to integrated financial and ESG data from
multiple sources through a unified API.
"""
import os
import json
from datetime import datetime
from typing import Dict, List, Any, Optional

from flask import Blueprint, request, jsonify, send_file, Response
import pandas as pd

from src.integration.data_manager import DataManager

# Create Blueprint
integrated_data_bp = Blueprint('integrated', __name__, url_prefix='/api/integrated')

# Initialize data manager
data_manager = DataManager()


@integrated_data_bp.route('/stock/<symbol>', methods=['GET'])
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
        export_format = request.args.get('format', 'json').lower()
        
        # Get stock data
        stock_data = data_manager.get_stock_data(symbol)
        
        # If country code is provided, get ESG data as well
        if country_code:
            try:
                # Get combined market and ESG data
                result = data_manager.get_market_esg_impact(symbol, country_code)
            except Exception as e:
                # Fallback to just stock data if ESG data fails
                result = {
                    "symbol": symbol,
                    "company_name": stock_data["company_info"].get("Name", symbol),
                    "stock_data": stock_data,
                    "esg_error": str(e)
                }
        else:
            # Just return stock data
            result = {
                "symbol": symbol,
                "company_name": stock_data["company_info"].get("Name", symbol),
                "stock_data": stock_data
            }
        
        # Export format handling
        if export_format == 'csv':
            # Convert to DataFrame for CSV export
            df = pd.json_normalize(result)
            
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"stock_{symbol}_{timestamp}.csv"
            output_dir = os.path.join("data", "exports")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, filename)
            
            # Export to CSV
            df.to_csv(output_path, index=False)
            
            # Return the file for download
            return send_file(
                output_path,
                as_attachment=True,
                download_name=filename,
                mimetype='text/csv'
            )
        else:
            return jsonify(result)
    except Exception as e:
        return jsonify({
            "error": "Failed to get stock data",
            "message": str(e)
        }), 500


@integrated_data_bp.route('/sector', methods=['GET'])
def get_sector_performance():
    """
    Get sector performance data
    
    Query Parameters:
    - format: Output format (json or csv, default: json)
    """
    try:
        export_format = request.args.get('format', 'json').lower()
        
        # Get sector data
        sector_data = data_manager.get_sector_performance()
        
        # Export format handling
        if export_format == 'csv':
            # Convert to DataFrame for CSV export
            df = pd.DataFrame(sector_data["data"])
            
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"sector_performance_{timestamp}.csv"
            output_dir = os.path.join("data", "exports")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, filename)
            
            # Export to CSV
            df.to_csv(output_path, index=False)
            
            # Return the file for download
            return send_file(
                output_path,
                as_attachment=True,
                download_name=filename,
                mimetype='text/csv'
            )
        else:
            return jsonify(sector_data)
    except Exception as e:
        return jsonify({
            "error": "Failed to get sector performance data",
            "message": str(e)
        }), 500


@integrated_data_bp.route('/forex', methods=['GET'])
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
        export_format = request.args.get('format', 'json').lower()
        
        if not from_currency or not to_currency:
            return jsonify({
                "error": "Missing required parameters",
                "message": "Both from_currency and to_currency are required"
            }), 400
        
        # Get forex data
        forex_data = data_manager.get_forex_data(from_currency, to_currency)
        
        # Export format handling
        if export_format == 'csv':
            # Convert to DataFrame for CSV export
            df = pd.DataFrame([forex_data["data"]])
            
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"forex_{from_currency}_{to_currency}_{timestamp}.csv"
            output_dir = os.path.join("data", "exports")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, filename)
            
            # Export to CSV
            df.to_csv(output_path, index=False)
            
            # Return the file for download
            return send_file(
                output_path,
                as_attachment=True,
                download_name=filename,
                mimetype='text/csv'
            )
        else:
            return jsonify(forex_data)
    except Exception as e:
        return jsonify({
            "error": "Failed to get forex data",
            "message": str(e)
        }), 500


@integrated_data_bp.route('/crypto/<symbol>', methods=['GET'])
def get_crypto_data(symbol: str):
    """
    Get cryptocurrency data
    
    URL Parameters:
    - symbol: Cryptocurrency symbol
    
    Query Parameters:
    - format: Output format (json or csv, default: json)
    """
    try:
        export_format = request.args.get('format', 'json').lower()
        
        # Get crypto data
        crypto_data = data_manager.get_crypto_data(symbol)
        
        # Export format handling
        if export_format == 'csv':
            # Convert to DataFrame for CSV export
            df = pd.DataFrame([crypto_data["data"]])
            
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"crypto_{symbol}_{timestamp}.csv"
            output_dir = os.path.join("data", "exports")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, filename)
            
            # Export to CSV
            df.to_csv(output_path, index=False)
            
            # Return the file for download
            return send_file(
                output_path,
                as_attachment=True,
                download_name=filename,
                mimetype='text/csv'
            )
        else:
            return jsonify(crypto_data)
    except Exception as e:
        return jsonify({
            "error": "Failed to get cryptocurrency data",
            "message": str(e)
        }), 500


@integrated_data_bp.route('/countries', methods=['GET'])
def get_countries_list():
    """
    Get list of countries with ESG data available
    """
    try:
        countries = data_manager.get_countries_list()
        return jsonify({
            "count": len(countries),
            "countries": countries
        })
    except Exception as e:
        return jsonify({
            "error": "Failed to get countries list",
            "message": str(e)
        }), 500


@integrated_data_bp.route('/esg/country/<country_code>', methods=['GET'])
def get_country_esg_data(country_code: str):
    """
    Get ESG data for a country
    
    URL Parameters:
    - country_code: Country code (ISO 3166-1 alpha-2 or alpha-3)
    
    Query Parameters:
    - format: Output format (json or csv, default: json)
    """
    try:
        export_format = request.args.get('format', 'json').lower()
        
        # Get ESG data
        esg_data = data_manager.get_country_esg_data(country_code)
        
        # Export format handling
        if export_format == 'csv':
            # Convert to DataFrame for CSV export
            df = pd.DataFrame({
                'dimension': list(esg_data["esg_scores"].keys()),
                'score': list(esg_data["esg_scores"].values())
            })
            
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"esg_{country_code}_{timestamp}.csv"
            output_dir = os.path.join("data", "exports")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, filename)
            
            # Export to CSV
            df.to_csv(output_path, index=False)
            
            # Return the file for download
            return send_file(
                output_path,
                as_attachment=True,
                download_name=filename,
                mimetype='text/csv'
            )
        else:
            return jsonify(esg_data)
    except Exception as e:
        return jsonify({
            "error": "Failed to get ESG data",
            "message": str(e)
        }), 500


@integrated_data_bp.route('/esg/region/<region_code>/scores', methods=['GET'])
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
        export_format = request.args.get('format', 'json').lower()
        
        # Get ESG scores
        esg_scores = data_manager.get_esg_scores_for_region(region_code, year)
        
        # Export format handling
        if export_format == 'csv':
            # Convert to DataFrame for CSV export
            df = pd.DataFrame({
                'dimension': list(esg_scores["scores"].keys()),
                'score': list(esg_scores["scores"].values())
            })
            
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            year_part = year if year else "latest"
            filename = f"esg_scores_{region_code}_{year_part}_{timestamp}.csv"
            output_dir = os.path.join("data", "exports")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, filename)
            
            # Export to CSV
            df.to_csv(output_path, index=False)
            
            # Return the file for download
            return send_file(
                output_path,
                as_attachment=True,
                download_name=filename,
                mimetype='text/csv'
            )
        else:
            return jsonify(esg_scores)
    except Exception as e:
        return jsonify({
            "error": "Failed to get ESG scores",
            "message": str(e)
        }), 500


@integrated_data_bp.route('/esg/compare', methods=['GET'])
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
            return jsonify({
                "error": "Missing required parameter",
                "message": "regions parameter is required"
            }), 400
            
        region_codes = [r.strip() for r in regions_param.split(',')]
        
        year = request.args.get('year')
        if year:
            year = int(year)
            
        dimension = request.args.get('dimension')
        export_format = request.args.get('format', 'json').lower()
        
        # Get comparison data
        comparison = data_manager.compare_esg_scores(region_codes, year, dimension)
        
        # Export format handling
        if export_format == 'csv':
            rows = []
            
            if dimension:
                # Single dimension comparison
                for region_code, data in comparison["regions"].items():
                    rows.append({
                        "region_code": region_code,
                        "region_name": data["region_name"],
                        "dimension": dimension,
                        "score": data["score"]
                    })
            else:
                # All dimensions comparison
                for region_code, data in comparison["regions"].items():
                    for dim, score in data["scores"].items():
                        rows.append({
                            "region_code": region_code,
                            "region_name": data["region_name"],
                            "dimension": dim,
                            "score": score
                        })
            
            # Convert to DataFrame
            df = pd.DataFrame(rows)
            
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            year_part = year if year else "latest"
            dim_part = dimension if dimension else "all"
            filename = f"esg_comparison_{dim_part}_{year_part}_{timestamp}.csv"
            output_dir = os.path.join("data", "exports")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, filename)
            
            # Export to CSV
            df.to_csv(output_path, index=False)
            
            # Return the file for download
            return send_file(
                output_path,
                as_attachment=True,
                download_name=filename,
                mimetype='text/csv'
            )
        else:
            return jsonify(comparison)
    except Exception as e:
        return jsonify({
            "error": "Failed to compare ESG scores",
            "message": str(e)
        }), 500


# New routes for expanded data sources

@integrated_data_bp.route('/sp500/constituents', methods=['GET'])
def get_sp500_constituents():
    """
    Get the current constituents of the S&P 500 index
    
    Query Parameters:
    - format: Output format (json or csv, default: json)
    """
    try:
        export_format = request.args.get('format', 'json').lower()
        
        # Get S&P 500 constituents
        constituents = data_manager.get_sp500_constituents()
        
        # Export format handling
        if export_format == 'csv':
            # Convert to DataFrame for CSV export
            df = pd.DataFrame(constituents)
            
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"sp500_constituents_{timestamp}.csv"
            output_dir = os.path.join("data", "exports")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, filename)
            
            # Export to CSV
            df.to_csv(output_path, index=False)
            
            # Return the file for download
            return send_file(
                output_path,
                as_attachment=True,
                download_name=filename,
                mimetype='text/csv'
            )
        else:
            return jsonify({
                "count": len(constituents),
                "constituents": constituents
            })
    except Exception as e:
        return jsonify({
            "error": "Failed to get S&P 500 constituents",
            "message": str(e)
        }), 500


@integrated_data_bp.route('/index/<index_id>/performance', methods=['GET'])
def get_index_performance(index_id: str):
    """
    Get performance data for a specific index
    
    URL Parameters:
    - index_id: Index identifier (e.g., SP500, DJIA)
    
    Query Parameters:
    - period: Time period (1d, 5d, 1m, 3m, 6m, 1y, 5y, default: 1y)
    - format: Output format (json or csv, default: json)
    """
    try:
        period = request.args.get('period', '1y')
        export_format = request.args.get('format', 'json').lower()
        
        # Get index performance
        performance = data_manager.get_index_performance(index_id, period)
        
        # Export format handling
        if export_format == 'csv':
            # Convert to DataFrame for CSV export
            df = pd.DataFrame(performance["data"])
            
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"index_{index_id}_{period}_{timestamp}.csv"
            output_dir = os.path.join("data", "exports")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, filename)
            
            # Export to CSV
            df.to_csv(output_path, index=False)
            
            # Return the file for download
            return send_file(
                output_path,
                as_attachment=True,
                download_name=filename,
                mimetype='text/csv'
            )
        else:
            return jsonify(performance)
    except Exception as e:
        return jsonify({
            "error": f"Failed to get index performance for {index_id}",
            "message": str(e)
        }), 500


@integrated_data_bp.route('/indices', methods=['GET'])
def get_market_indices():
    """
    Get market indices data
    
    Query Parameters:
    - region: Region filter (e.g., US, EU, ASIA, optional)
    - format: Output format (json or csv, default: json)
    """
    try:
        region = request.args.get('region')
        export_format = request.args.get('format', 'json').lower()
        
        # Get market indices
        indices = data_manager.get_market_indices(region)
        
        # Export format handling
        if export_format == 'csv':
            # Convert to DataFrame for CSV export
            df = pd.DataFrame(indices["data"])
            
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            region_part = region if region else "global"
            filename = f"indices_{region_part}_{timestamp}.csv"
            output_dir = os.path.join("data", "exports")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, filename)
            
            # Export to CSV
            df.to_csv(output_path, index=False)
            
            # Return the file for download
            return send_file(
                output_path,
                as_attachment=True,
                download_name=filename,
                mimetype='text/csv'
            )
        else:
            return jsonify(indices)
    except Exception as e:
        return jsonify({
            "error": "Failed to get market indices",
            "message": str(e)
        }), 500


@integrated_data_bp.route('/fixed-income', methods=['GET'])
def get_fixed_income_data():
    """
    Get fixed income market data
    
    Query Parameters:
    - issuer_type: Type of issuer (sovereign, corporate, default: sovereign)
    - currency: Currency code (default: USD)
    - format: Output format (json or csv, default: json)
    """
    try:
        issuer_type = request.args.get('issuer_type', 'sovereign')
        currency = request.args.get('currency', 'USD')
        export_format = request.args.get('format', 'json').lower()
        
        # Get fixed income data
        fixed_income_data = data_manager.get_fixed_income_data(issuer_type, currency)
        
        # Export format handling
        if export_format == 'csv':
            # Convert to DataFrame for CSV export
            df = pd.DataFrame(fixed_income_data["data"])
            
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"fixed_income_{issuer_type}_{currency}_{timestamp}.csv"
            output_dir = os.path.join("data", "exports")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, filename)
            
            # Export to CSV
            df.to_csv(output_path, index=False)
            
            # Return the file for download
            return send_file(
                output_path,
                as_attachment=True,
                download_name=filename,
                mimetype='text/csv'
            )
        else:
            return jsonify(fixed_income_data)
    except Exception as e:
        return jsonify({
            "error": "Failed to get fixed income data",
            "message": str(e)
        }), 500


@integrated_data_bp.route('/fundamentals/<identifier>', methods=['GET'])
def get_company_fundamentals(identifier: str):
    """
    Get company fundamental data
    
    URL Parameters:
    - identifier: Company identifier (ticker or RIC)
    
    Query Parameters:
    - source: Data source provider (refinitiv, bloomberg, sp_global, alpha_vantage, default: refinitiv)
    - format: Output format (json or csv, default: json)
    """
    try:
        source = request.args.get('source', 'refinitiv')
        export_format = request.args.get('format', 'json').lower()
        
        # Get fundamentals data
        fundamentals = data_manager.get_company_fundamentals(identifier, source)
        
        # Export format handling
        if export_format == 'csv':
            # Convert to DataFrame for CSV export
            # This might need customization based on the actual structure of the data
            if isinstance(fundamentals["data"], dict):
                df = pd.DataFrame([fundamentals["data"]])
            else:
                df = pd.DataFrame(fundamentals["data"])
            
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"fundamentals_{source}_{identifier}_{timestamp}.csv"
            output_dir = os.path.join("data", "exports")
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, filename)
            
            # Export to CSV
            df.to_csv(output_path, index=False)
            
            # Return the file for download
            return send_file(
                output_path,
                as_attachment=True,
                download_name=filename,
                mimetype='text/csv'
            )
        else:
            return jsonify(fundamentals)
    except Exception as e:
        return jsonify({
            "error": "Failed to get company fundamentals",
            "message": str(e)
        }), 500