"""
API Key Administration Endpoints
Admin interface for managing API keys and monitoring usage
"""

from flask import Blueprint, request, jsonify, render_template_string
from datetime import datetime
import sys
sys.path.append('/home/runner/workspace')
from auth.api_key_manager import api_key_manager, ProductType, AccessScope

auth_admin_api = Blueprint('auth_admin', __name__)

@auth_admin_api.route('/admin/api-keys')
def api_keys_dashboard():
    """API keys management dashboard"""
    
    dashboard_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>WizData API Key Management</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
            .container { max-width: 1200px; margin: 0 auto; }
            .header { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .card { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
            .key-item { border: 1px solid #ddd; border-radius: 6px; padding: 15px; margin-bottom: 15px; }
            .key-item.active { border-color: #28a745; background: #f8fff9; }
            .key-item.inactive { border-color: #dc3545; background: #fff8f8; }
            .badge { display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }
            .badge.active { background: #28a745; color: white; }
            .badge.inactive { background: #dc3545; color: white; }
            .badge.scope { background: #007bff; color: white; margin: 2px; }
            .btn { padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer; text-decoration: none; }
            .btn-primary { background: #007bff; color: white; }
            .btn-danger { background: #dc3545; color: white; }
            .btn-success { background: #28a745; color: white; }
            .form-group { margin-bottom: 15px; }
            .form-control { width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px; }
            .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
            .stat-item { text-align: center; }
            .stat-value { font-size: 24px; font-weight: bold; color: #007bff; }
            .stat-label { color: #666; font-size: 14px; }
            .hidden { display: none; }
            .alert { padding: 15px; border-radius: 4px; margin-bottom: 20px; }
            .alert-success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
            .alert-danger { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🔐 WizData API Key Management</h1>
                <p>Manage API keys for inter-product integration across the WizData ecosystem</p>
            </div>
            
            <div id="alerts"></div>
            
            <div class="grid">
                <div class="card">
                    <h3>📊 Quick Stats</h3>
                    <div class="stats" id="stats">
                        <div class="stat-item">
                            <div class="stat-value" id="total-keys">-</div>
                            <div class="stat-label">Total Keys</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value" id="active-keys">-</div>
                            <div class="stat-label">Active Keys</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value" id="total-products">-</div>
                            <div class="stat-label">Products</div>
                        </div>
                    </div>
                </div>
                
                <div class="card">
                    <h3>➕ Create New API Key</h3>
                    <form id="create-key-form">
                        <div class="form-group">
                            <label>Product</label>
                            <select class="form-control" id="product" required>
                                <option value="">Select Product</option>
                                <option value="vueon">VueOn (Charts)</option>
                                <option value="trader">Trader (Strada)</option>
                                <option value="pulse">Pulse (Overview)</option>
                                <option value="wealth">Wealth (Novia)</option>
                                <option value="connect">Connect (Portal)</option>
                            </select>
                        </div>
                        <div class="form-group">
                            <label>Name</label>
                            <input type="text" class="form-control" id="name" placeholder="API Key Name" required>
                        </div>
                        <div class="form-group">
                            <label>Description</label>
                            <input type="text" class="form-control" id="description" placeholder="Optional description">
                        </div>
                        <div class="form-group">
                            <label>Rate Limit (requests/hour)</label>
                            <input type="number" class="form-control" id="rate-limit" value="1000" min="100" max="10000">
                        </div>
                        <button type="submit" class="btn btn-success">Create API Key</button>
                    </form>
                </div>
            </div>
            
            <div class="card">
                <h3>🔑 Existing API Keys</h3>
                <div id="api-keys-list">
                    Loading...
                </div>
            </div>
        </div>
        
        <script>
            // API functions
            async function loadStats() {
                try {
                    const response = await fetch('/api/admin/api-keys/stats');
                    const data = await response.json();
                    
                    document.getElementById('total-keys').textContent = data.total_keys;
                    document.getElementById('active-keys').textContent = data.active_keys;
                    document.getElementById('total-products').textContent = data.unique_products;
                } catch (error) {
                    console.error('Error loading stats:', error);
                }
            }
            
            async function loadApiKeys() {
                try {
                    const response = await fetch('/api/admin/api-keys');
                    const data = await response.json();
                    
                    const container = document.getElementById('api-keys-list');
                    
                    if (data.api_keys.length === 0) {
                        container.innerHTML = '<p>No API keys found.</p>';
                        return;
                    }
                    
                    container.innerHTML = data.api_keys.map(key => `
                        <div class="key-item ${key.is_active ? 'active' : 'inactive'}">
                            <div style="display: flex; justify-content: between; align-items: center;">
                                <div style="flex: 1;">
                                    <h4>${key.name}</h4>
                                    <p><strong>Product:</strong> ${key.product}</p>
                                    <p><strong>Key ID:</strong> <code>${key.key_id}</code></p>
                                    <p><strong>Rate Limit:</strong> ${key.rate_limit} req/hour</p>
                                    <p><strong>Usage:</strong> ${key.usage_count} total requests</p>
                                    <p><strong>Created:</strong> ${new Date(key.created_at).toLocaleString()}</p>
                                    ${key.last_used ? `<p><strong>Last Used:</strong> ${new Date(key.last_used).toLocaleString()}</p>` : ''}
                                    ${key.expires_at ? `<p><strong>Expires:</strong> ${new Date(key.expires_at).toLocaleString()}</p>` : ''}
                                    <div>
                                        <strong>Scopes:</strong>
                                        ${key.scopes.map(scope => `<span class="badge scope">${scope}</span>`).join('')}
                                    </div>
                                </div>
                                <div>
                                    <span class="badge ${key.is_active ? 'active' : 'inactive'}">
                                        ${key.is_active ? 'Active' : 'Inactive'}
                                    </span>
                                    ${key.is_active ? `
                                        <br><br>
                                        <button class="btn btn-danger" onclick="revokeKey('${key.key_id}')">Revoke</button>
                                    ` : ''}
                                </div>
                            </div>
                        </div>
                    `).join('');
                    
                } catch (error) {
                    console.error('Error loading API keys:', error);
                    document.getElementById('api-keys-list').innerHTML = '<p>Error loading API keys.</p>';
                }
            }
            
            async function createApiKey(formData) {
                try {
                    const response = await fetch('/api/admin/api-keys', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(formData)
                    });
                    
                    const data = await response.json();
                    
                    if (data.success) {
                        showAlert('success', `API Key created successfully! Key: ${data.api_key}`);
                        document.getElementById('create-key-form').reset();
                        loadApiKeys();
                        loadStats();
                    } else {
                        showAlert('danger', `Error: ${data.error}`);
                    }
                } catch (error) {
                    showAlert('danger', `Error creating API key: ${error.message}`);
                }
            }
            
            async function revokeKey(keyId) {
                if (!confirm('Are you sure you want to revoke this API key?')) return;
                
                try {
                    const response = await fetch(`/api/admin/api-keys/${keyId}/revoke`, {
                        method: 'POST'
                    });
                    
                    const data = await response.json();
                    
                    if (data.success) {
                        showAlert('success', 'API key revoked successfully');
                        loadApiKeys();
                        loadStats();
                    } else {
                        showAlert('danger', `Error: ${data.error}`);
                    }
                } catch (error) {
                    showAlert('danger', `Error revoking API key: ${error.message}`);
                }
            }
            
            function showAlert(type, message) {
                const alertsContainer = document.getElementById('alerts');
                const alert = document.createElement('div');
                alert.className = `alert alert-${type}`;
                alert.textContent = message;
                
                alertsContainer.appendChild(alert);
                
                setTimeout(() => {
                    alertsContainer.removeChild(alert);
                }, 5000);
            }
            
            // Event listeners
            document.getElementById('create-key-form').addEventListener('submit', function(e) {
                e.preventDefault();
                
                const formData = {
                    product: document.getElementById('product').value,
                    name: document.getElementById('name').value,
                    description: document.getElementById('description').value,
                    rate_limit: parseInt(document.getElementById('rate-limit').value)
                };
                
                createApiKey(formData);
            });
            
            // Load initial data
            loadStats();
            loadApiKeys();
            
            // Refresh every 30 seconds
            setInterval(() => {
                loadStats();
                loadApiKeys();
            }, 30000);
        </script>
    </body>
    </html>
    """
    
    return dashboard_html

@auth_admin_api.route('/api/admin/api-keys', methods=['GET'])
def list_api_keys():
    """List all API keys"""
    try:
        keys = api_key_manager.list_api_keys()
        return jsonify({
            "success": True,
            "api_keys": keys,
            "count": len(keys),
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@auth_admin_api.route('/api/admin/api-keys/stats')
def api_keys_stats():
    """Get API keys statistics"""
    try:
        keys = api_key_manager.list_api_keys()
        
        total_keys = len(keys)
        active_keys = sum(1 for key in keys if key['is_active'])
        unique_products = len(set(key['product'] for key in keys))
        
        # Usage statistics
        total_usage = sum(key['usage_count'] for key in keys)
        
        return jsonify({
            "success": True,
            "total_keys": total_keys,
            "active_keys": active_keys,
            "inactive_keys": total_keys - active_keys,
            "unique_products": unique_products,
            "total_usage": total_usage,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@auth_admin_api.route('/api/admin/api-keys', methods=['POST'])
def create_api_key():
    """Create new API key"""
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data.get('product') or not data.get('name'):
            return jsonify({
                "success": False,
                "error": "Product and name are required",
                "timestamp": datetime.now().isoformat()
            }), 400
        
        # Convert product string to enum
        try:
            product = ProductType(data['product'])
        except ValueError:
            return jsonify({
                "success": False,
                "error": f"Invalid product: {data['product']}",
                "timestamp": datetime.now().isoformat()
            }), 400
        
        # Generate API key
        key_info = api_key_manager.generate_api_key(
            product=product,
            name=data['name'],
            description=data.get('description', ''),
            rate_limit=data.get('rate_limit', 1000)
        )
        
        return jsonify({
            "success": True,
            **key_info,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@auth_admin_api.route('/api/admin/api-keys/<key_id>/revoke', methods=['POST'])
def revoke_api_key(key_id):
    """Revoke an API key"""
    try:
        success = api_key_manager.revoke_api_key(key_id)
        
        if success:
            return jsonify({
                "success": True,
                "message": f"API key {key_id} revoked successfully",
                "timestamp": datetime.now().isoformat()
            })
        else:
            return jsonify({
                "success": False,
                "error": "API key not found",
                "timestamp": datetime.now().isoformat()
            }), 404
            
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@auth_admin_api.route('/api/admin/api-keys/<key_id>/usage')
def get_api_key_usage(key_id):
    """Get usage statistics for an API key"""
    try:
        days = request.args.get('days', 7, type=int)
        usage_stats = api_key_manager.get_usage_stats(key_id, days)
        
        return jsonify({
            "success": True,
            "usage_stats": usage_stats,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@auth_admin_api.route('/api/admin/products')
def list_products():
    """List available products and their default scopes"""
    try:
        products = []
        
        for product in ProductType:
            default_scopes = api_key_manager.product_default_scopes.get(product, [])
            
            products.append({
                "value": product.value,
                "name": product.value.title(),
                "default_scopes": [scope.value for scope in default_scopes],
                "description": f"{product.value.title()} platform integration"
            })
        
        return jsonify({
            "success": True,
            "products": products,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@auth_admin_api.route('/api/admin/scopes')
def list_scopes():
    """List available access scopes and their permissions"""
    try:
        scopes = []
        
        for scope, permissions in api_key_manager.scope_permissions.items():
            scopes.append({
                "value": scope.value,
                "name": scope.value.replace('_', ' ').title(),
                "permissions": [
                    {
                        "path": perm.path,
                        "methods": perm.methods,
                        "description": perm.description
                    }
                    for perm in permissions
                ]
            })
        
        return jsonify({
            "success": True,
            "scopes": scopes,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500