"""
Quality API Routes

This module provides endpoints for data quality monitoring and reporting,
allowing users to check data quality for various data types, markets, and symbols.
"""

import logging
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from flask import Blueprint, jsonify, request, current_app, send_file
import pandas as pd
from src.processing.data_quality import DataQualityMonitor
from src.processing.standards import StandardsCompliance

logger = logging.getLogger(__name__)

quality_bp = Blueprint('quality', __name__, url_prefix='/api/quality')

# Initialize data quality components
data_quality_monitor = DataQualityMonitor()
standards_compliance = StandardsCompliance()

@quality_bp.route('/')
def quality_index():
    """Quality API index endpoint"""
    return jsonify({
        "status": "success",
        "version": "1.0.0",
        "endpoints": [
            "/api/quality/check",
            "/api/quality/reports",
            "/api/quality/sources",
            "/api/quality/compliance"
        ]
    })

@quality_bp.route('/check')
def check_data_quality():
    """
    Check data quality for a specific data type, market, and symbol
    
    Query parameters:
    - data_type: The data type ("price", "dividend", "earnings", etc.)
    - market: The market code (e.g., "jse", "ngx", "nasdaq", etc.)
    - symbol: Symbol to check (optional)
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format ("json", "csv") - if provided, triggers a file download
    
    Returns:
        JSON response with data quality report or a file download
    """
    try:
        # Get query parameters
        data_type = request.args.get('data_type', 'price')
        market = request.args.get('market')
        symbol = request.args.get('symbol')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Input validation
        if not market:
            return jsonify({
                "status": "error",
                "message": "Market parameter is required"
            }), 400
        
        # Import appropriate fetcher based on data type
        df = None
        
        if data_type == 'price':
            if market in ['jse', 'ngx', 'egx', 'nse', 'cse']:  # African markets
                from src.ingestion.africa_markets_fetcher import AfricanMarketsFetcher
                fetcher = AfricanMarketsFetcher()
                df = fetcher.get_data(market=market, symbol=symbol, 
                                     start_date=start_date, end_date=end_date)
            elif market in ['asx', 'lse', 'nasdaq', 'nyse', 'tyo']:  # Global markets
                from src.ingestion.global_markets_fetcher import GlobalMarketsFetcher
                fetcher = GlobalMarketsFetcher()
                df = fetcher.get_data(market=market, symbol=symbol, 
                                     start_date=start_date, end_date=end_date)
            elif market == 'crypto':
                from src.ingestion.crypto_fetcher import CryptoFetcher
                fetcher = CryptoFetcher()
                df = fetcher.get_data(symbol=symbol, start_date=start_date, end_date=end_date)
            elif market == 'forex':
                from src.ingestion.forex_fetcher import ForexFetcher
                fetcher = ForexFetcher()
                df = fetcher.get_data(symbol=symbol, start_date=start_date, end_date=end_date)
                
        elif data_type == 'dividend':
            from src.ingestion.dividend_fetcher import DividendFetcher
            fetcher = DividendFetcher()
            df = fetcher.get_data(market=market, symbol=symbol, 
                                 start_date=start_date, end_date=end_date)
                
        elif data_type == 'earnings':
            from src.ingestion.earnings_fetcher import EarningsFetcher
            fetcher = EarningsFetcher()
            df = fetcher.get_data(market=market, symbol=symbol, 
                                 start_date=start_date, end_date=end_date)
        
        # Check if we got any data
        if df is None or df.empty:
            return jsonify({
                "status": "error",
                "message": f"No {data_type} data available for {market} {symbol if symbol else ''}"
            }), 404
        
        # Run data quality check
        df_quality, quality_report = data_quality_monitor.check_data_quality(df, data_type=data_type)
        
        # Handle export if requested
        if export_format:
            # Create export directory if it doesn't exist
            export_dir = Path(current_app.config.get("DATA_EXPORT_DIR", "data/exports"))
            export_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_base = f"{market}_{data_type}_quality_{timestamp}"
            
            if export_format.lower() == 'json':
                filepath = export_dir / f"{filename_base}.json"
                with open(filepath, 'w') as f:
                    json.dump(quality_report, f, indent=2)
                return send_file(filepath, as_attachment=True, download_name=filepath.name, mimetype='application/json')
                
            elif export_format.lower() == 'csv':
                filepath = export_dir / f"{filename_base}.csv"
                df_quality.to_csv(filepath, index=False)
                return send_file(filepath, as_attachment=True, download_name=filepath.name, mimetype='text/csv')
        
        # Return JSON response with quality report
        return jsonify({
            "status": "success",
            "quality_report": quality_report,
            "anomaly_count": int(df_quality['has_anomaly'].sum()),
            "total_records": len(df_quality)
        })
        
    except Exception as e:
        logger.exception(f"Error in check_data_quality: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@quality_bp.route('/reports')
def get_quality_reports():
    """
    Get a list of recent data quality reports
    
    Query parameters:
    - data_type: Filter by data type (optional)
    - days: Number of days to look back (default: 7)
    
    Returns:
        JSON response with list of reports
    """
    try:
        # Get query parameters
        data_type = request.args.get('data_type')
        days = int(request.args.get('days', 7))
        
        # Get report directory
        log_dir = Path("logs/data_quality")
        if not log_dir.exists():
            return jsonify({
                "status": "error",
                "message": "No quality reports found"
            }), 404
        
        # Get all report files
        reports = []
        for file in log_dir.glob("*.json"):
            # Skip source reliability reports
            if "source_reliability" in file.name:
                continue
                
            # Filter by data type if specified
            if data_type and not file.name.startswith(f"{data_type}_quality"):
                continue
                
            # Get file stats
            stats = file.stat()
            modified_time = datetime.fromtimestamp(stats.st_mtime)
            
            # Skip files older than the requested number of days
            if modified_time < datetime.now() - timedelta(days=days):
                continue
                
            # Add report info to list
            reports.append({
                "filename": file.name,
                "timestamp": modified_time.isoformat(),
                "size": stats.st_size,
                "path": str(file)
            })
        
        # Sort reports by timestamp (newest first)
        reports.sort(key=lambda x: x['timestamp'], reverse=True)
        
        return jsonify({
            "status": "success",
            "count": len(reports),
            "reports": reports
        })
        
    except Exception as e:
        logger.exception(f"Error in get_quality_reports: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
        
@quality_bp.route('/reports/download')
def download_quality_report():
    """
    Download a specific quality report
    
    Query parameters:
    - filename: Name of the report file to download
    
    Returns:
        File download
    """
    try:
        # Get filename parameter
        filename = request.args.get('filename')
        if not filename:
            return jsonify({
                "status": "error",
                "message": "Filename parameter is required"
            }), 400
            
        # Get file path
        log_dir = Path("logs/data_quality")
        file_path = log_dir / filename
        
        # Check if file exists
        if not file_path.exists():
            return jsonify({
                "status": "error",
                "message": f"Report file not found: {filename}"
            }), 404
            
        # Determine MIME type
        mime_type = 'application/json' if filename.endswith('.json') else 'text/csv'
        
        # Return file for download
        return send_file(
            file_path,
            as_attachment=True,
            download_name=filename,
            mimetype=mime_type
        )
        
    except Exception as e:
        logger.exception(f"Error downloading quality report: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@quality_bp.route('/sources')
def get_source_reliability():
    """
    Get source reliability report
    
    Returns:
        JSON response with source reliability report
    """
    try:
        return jsonify({
            "status": "success",
            "reliability_report": data_quality_monitor.get_source_reliability_report()
        })
        
    except Exception as e:
        logger.exception(f"Error in get_source_reliability: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@quality_bp.route('/compliance')
def check_standards_compliance():
    """
    Check standards compliance for a specific data type, market, and symbol
    
    Query parameters:
    - data_type: The data type ("price", "dividend", "earnings", etc.)
    - market: The market code (e.g., "jse", "ngx", "nasdaq", etc.)
    - symbol: Symbol to check (optional)
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format ("json", "csv") - if provided, triggers a file download
    
    Returns:
        JSON response with compliance report or a file download
    """
    try:
        # Get query parameters
        data_type = request.args.get('data_type', 'price')
        market = request.args.get('market')
        symbol = request.args.get('symbol')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Input validation
        if not market:
            return jsonify({
                "status": "error",
                "message": "Market parameter is required"
            }), 400
        
        # Import appropriate fetcher based on data type
        df = None
        
        if data_type == 'price':
            if market in ['jse', 'ngx', 'egx', 'nse', 'cse']:  # African markets
                from src.ingestion.africa_markets_fetcher import AfricanMarketsFetcher
                fetcher = AfricanMarketsFetcher()
                df = fetcher.get_data(market=market, symbol=symbol, 
                                     start_date=start_date, end_date=end_date)
            elif market in ['asx', 'lse', 'nasdaq', 'nyse', 'tyo']:  # Global markets
                from src.ingestion.global_markets_fetcher import GlobalMarketsFetcher
                fetcher = GlobalMarketsFetcher()
                df = fetcher.get_data(market=market, symbol=symbol, 
                                     start_date=start_date, end_date=end_date)
            elif market == 'crypto':
                from src.ingestion.crypto_fetcher import CryptoFetcher
                fetcher = CryptoFetcher()
                df = fetcher.get_data(symbol=symbol, start_date=start_date, end_date=end_date)
            elif market == 'forex':
                from src.ingestion.forex_fetcher import ForexFetcher
                fetcher = ForexFetcher()
                df = fetcher.get_data(symbol=symbol, start_date=start_date, end_date=end_date)
                
        elif data_type == 'dividend':
            from src.ingestion.dividend_fetcher import DividendFetcher
            fetcher = DividendFetcher()
            df = fetcher.get_data(market=market, symbol=symbol, 
                                 start_date=start_date, end_date=end_date)
                
        elif data_type == 'earnings':
            from src.ingestion.earnings_fetcher import EarningsFetcher
            fetcher = EarningsFetcher()
            df = fetcher.get_data(market=market, symbol=symbol, 
                                 start_date=start_date, end_date=end_date)
        
        # Check if we got any data
        if df is None or df.empty:
            return jsonify({
                "status": "error",
                "message": f"No {data_type} data available for {market} {symbol if symbol else ''}"
            }), 404
        
        # Run standards compliance check
        df_std, compliance_report = standards_compliance.standardize_data(df, data_type=data_type)
        
        # Handle export if requested
        if export_format:
            # Create export directory if it doesn't exist
            export_dir = Path(current_app.config.get("DATA_EXPORT_DIR", "data/exports"))
            export_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_base = f"{market}_{data_type}_compliance_{timestamp}"
            
            if export_format.lower() == 'json':
                filepath = export_dir / f"{filename_base}.json"
                with open(filepath, 'w') as f:
                    json.dump(compliance_report, f, indent=2)
                return send_file(filepath, as_attachment=True, download_name=filepath.name, mimetype='application/json')
                
            elif export_format.lower() == 'csv':
                filepath = export_dir / f"{filename_base}.csv"
                df_std.to_csv(filepath, index=False)
                return send_file(filepath, as_attachment=True, download_name=filepath.name, mimetype='text/csv')
        
        # Return JSON response with compliance report
        return jsonify({
            "status": "success",
            "compliance_report": compliance_report,
            "total_records": len(df_std)
        })
        
    except Exception as e:
        logger.exception(f"Error in check_standards_compliance: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500