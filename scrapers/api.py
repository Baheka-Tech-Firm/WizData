"""
API endpoints for scraper management and monitoring
"""

from flask import Blueprint, jsonify, request, current_app
from middleware.rate_limiter import rate_limit
from middleware.cache_manager import cached
from middleware.monitoring import monitor_function
import asyncio
import logging
from datetime import datetime, timezone

try:
    from scrapers.orchestrator import ScraperOrchestrator
    ORCHESTRATOR_AVAILABLE = True
except ImportError:
    ORCHESTRATOR_AVAILABLE = False

logger = logging.getLogger(__name__)

scrapers_bp = Blueprint('scrapers', __name__, url_prefix='/api/scrapers')

# Global orchestrator instance
orchestrator = None

def get_orchestrator():
    """Get or create orchestrator instance"""
    global orchestrator
    
    if not ORCHESTRATOR_AVAILABLE:
        return None
    
    if orchestrator is None:
        config = {
            'max_concurrent_jobs': 3,
            'scheduler_interval': 30
        }
        orchestrator = ScraperOrchestrator(config)
        
        # Add default jobs
        _add_default_jobs()
    
    return orchestrator

def _add_default_jobs():
    """Add default scraping jobs"""
    global orchestrator
    
    if not orchestrator:
        return
    
    # JSE scraper job
    orchestrator.add_job('jse_stocks', {
        'scraper_class': 'jse',
        'config': {
            'proxy_config': {},
            'queue_config': {'enabled': True, 'queue_type': 'memory'},
            'request_delay': 2.0
        },
        'schedule': {
            'interval': 300,  # 5 minutes
            'enabled': True
        },
        'enabled': True
    })
    
    # CoinGecko scraper job
    orchestrator.add_job('crypto_prices', {
        'scraper_class': 'coingecko',
        'config': {
            'proxy_config': {},
            'queue_config': {'enabled': True, 'queue_type': 'memory'},
            'request_delay': 1.2
        },
        'schedule': {
            'interval': 180,  # 3 minutes
            'enabled': True
        },
        'enabled': True
    })
    
    # Financial News scraper job
    orchestrator.add_job('financial_news', {
        'scraper_class': 'financial_news',
        'config': {
            'proxy_config': {},
            'queue_config': {'enabled': True, 'queue_type': 'memory'},
            'request_delay': 2.0
        },
        'schedule': {
            'interval': 900,  # 15 minutes
            'enabled': True
        },
        'enabled': True
    })
    
    # Forex rates scraper job
    orchestrator.add_job('forex_rates', {
        'scraper_class': 'forex',
        'config': {
            'proxy_config': {},
            'queue_config': {'enabled': True, 'queue_type': 'memory'},
            'request_delay': 1.5
        },
        'schedule': {
            'interval': 300,  # 5 minutes
            'enabled': True
        },
        'enabled': True
    })
    
    # Economic data scraper job
    orchestrator.add_job('economic_data', {
        'scraper_class': 'economic',
        'config': {
            'proxy_config': {},
            'queue_config': {'enabled': True, 'queue_type': 'memory'},
            'request_delay': 2.0
        },
        'schedule': {
            'interval': 1800,  # 30 minutes
            'enabled': True
        },
        'enabled': True
    })

@scrapers_bp.route('/status', methods=['GET'])
@rate_limit(requests_per_minute=30)
@cached(ttl=30, data_type='api_responses')
@monitor_function("scrapers_status")
def get_scrapers_status():
    """Get overall scraper system status"""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({
            'status': 'unavailable',
            'message': 'Scraper orchestrator not available',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 503
    
    orch = get_orchestrator()
    if not orch:
        return jsonify({
            'status': 'error',
            'message': 'Failed to initialize orchestrator',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 500
    
    try:
        stats = orch.get_stats()
        
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'orchestrator': stats['orchestrator'],
            'jobs_summary': {
                'total': len(stats['jobs']),
                'enabled': sum(1 for job in stats['jobs'].values() if job['enabled']),
                'available_scrapers': stats['available_scrapers']
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting scraper status: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 500

@scrapers_bp.route('/jobs', methods=['GET'])
@rate_limit(requests_per_minute=30)
@cached(ttl=60, data_type='static_data')
@monitor_function("get_scraper_jobs")
def get_scraper_jobs():
    """Get all scraper jobs and their configurations"""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({
            'message': 'Scraper orchestrator not available',
            'jobs': []
        }), 503
    
    orch = get_orchestrator()
    if not orch:
        return jsonify({
            'message': 'Failed to initialize orchestrator',
            'jobs': []
        }), 500
    
    try:
        stats = orch.get_stats()
        
        return jsonify({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'jobs': stats['jobs'],
            'available_scrapers': stats['available_scrapers']
        })
        
    except Exception as e:
        logger.error(f"Error getting scraper jobs: {str(e)}")
        return jsonify({
            'message': str(e),
            'jobs': []
        }), 500

@scrapers_bp.route('/jobs/<job_name>', methods=['GET'])
@rate_limit(requests_per_minute=60)
@monitor_function("get_scraper_job")
def get_scraper_job(job_name):
    """Get specific scraper job details"""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({
            'message': 'Scraper orchestrator not available'
        }), 503
    
    orch = get_orchestrator()
    if not orch:
        return jsonify({
            'message': 'Failed to initialize orchestrator'
        }), 500
    
    try:
        job_config = orch.get_job_config(job_name)
        
        if not job_config:
            return jsonify({
                'message': f'Job "{job_name}" not found'
            }), 404
        
        return jsonify({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'job_name': job_name,
            'config': job_config
        })
        
    except Exception as e:
        logger.error(f"Error getting scraper job {job_name}: {str(e)}")
        return jsonify({
            'message': str(e)
        }), 500

@scrapers_bp.route('/jobs/<job_name>/run', methods=['POST'])
@rate_limit(requests_per_minute=10)
@monitor_function("run_scraper_job")
def run_scraper_job(job_name):
    """Run a scraper job immediately"""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({
            'success': False,
            'message': 'Scraper orchestrator not available'
        }), 503
    
    orch = get_orchestrator()
    if not orch:
        return jsonify({
            'success': False,
            'message': 'Failed to initialize orchestrator'
        }), 500
    
    try:
        # Get any additional parameters from request (handle missing Content-Type)
        params = {}
        if request.is_json:
            params = request.get_json() or {}
        elif request.form:
            params = request.form.to_dict()
        
        # Run the job asynchronously
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(orch.run_job_once(job_name, **params))
            
            return jsonify({
                'success': result.success,
                'job_name': result.job_name,
                'start_time': result.start_time.isoformat(),
                'end_time': result.end_time.isoformat(),
                'duration_seconds': result.duration_seconds,
                'items_processed': result.items_processed,
                'quality_score': result.quality_score,
                'errors': result.errors,
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
            
        finally:
            loop.close()
        
    except Exception as e:
        logger.error(f"Error running scraper job {job_name}: {str(e)}")
        return jsonify({
            'success': False,
            'message': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 500

@scrapers_bp.route('/jobs/<job_name>/enable', methods=['POST'])
@rate_limit(requests_per_minute=20)
@monitor_function("enable_scraper_job")
def enable_scraper_job(job_name):
    """Enable a scraper job"""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({
            'success': False,
            'message': 'Scraper orchestrator not available'
        }), 503
    
    orch = get_orchestrator()
    if not orch:
        return jsonify({
            'success': False,
            'message': 'Failed to initialize orchestrator'
        }), 500
    
    try:
        orch.enable_job(job_name)
        
        return jsonify({
            'success': True,
            'message': f'Job "{job_name}" enabled',
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error enabling scraper job {job_name}: {str(e)}")
        return jsonify({
            'success': False,
            'message': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 500

@scrapers_bp.route('/jobs/<job_name>/disable', methods=['POST'])
@rate_limit(requests_per_minute=20)
@monitor_function("disable_scraper_job")
def disable_scraper_job(job_name):
    """Disable a scraper job"""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({
            'success': False,
            'message': 'Scraper orchestrator not available'
        }), 503
    
    orch = get_orchestrator()
    if not orch:
        return jsonify({
            'success': False,
            'message': 'Failed to initialize orchestrator'
        }), 500
    
    try:
        orch.disable_job(job_name)
        
        return jsonify({
            'success': True,
            'message': f'Job "{job_name}" disabled',
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error disabling scraper job {job_name}: {str(e)}")
        return jsonify({
            'success': False,
            'message': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 500

@scrapers_bp.route('/health', methods=['GET'])
@rate_limit(requests_per_minute=60)
@monitor_function("scrapers_health_check")
def scrapers_health_check():
    """Run health checks on all scrapers"""
    if not ORCHESTRATOR_AVAILABLE:
        return jsonify({
            'overall_status': 'unavailable',
            'message': 'Scraper orchestrator not available',
            'scrapers': {}
        }), 503
    
    orch = get_orchestrator()
    if not orch:
        return jsonify({
            'overall_status': 'error',
            'message': 'Failed to initialize orchestrator',
            'scrapers': {}
        }), 500
    
    try:
        # Run health checks asynchronously
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            health_results = loop.run_until_complete(orch.health_check_all())
            
            # Determine overall status
            healthy_count = sum(1 for result in health_results.values() 
                              if result.get('status') == 'healthy')
            total_scrapers = len(health_results)
            
            if healthy_count == total_scrapers:
                overall_status = 'healthy'
            elif healthy_count > 0:
                overall_status = 'degraded'
            else:
                overall_status = 'unhealthy'
            
            return jsonify({
                'overall_status': overall_status,
                'healthy_scrapers': healthy_count,
                'total_scrapers': total_scrapers,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'scrapers': health_results
            })
            
        finally:
            loop.close()
        
    except Exception as e:
        logger.error(f"Error in scrapers health check: {str(e)}")
        return jsonify({
            'overall_status': 'error',
            'message': str(e),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'scrapers': {}
        }), 500