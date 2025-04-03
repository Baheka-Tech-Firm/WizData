// WizData Dashboard JavaScript

// Update current time and last update time
function updateTimes() {
    const now = new Date();
    
    // Format the time as HH:MM:SS
    const timeString = now.toLocaleTimeString();
    
    // Update the last update element
    document.getElementById('lastUpdate').textContent = timeString;
}

// Initialize and render the data acquisition chart
function initAcquisitionChart() {
    const ctx = document.getElementById('acquisitionChart').getContext('2d');
    
    // Generate last 7 days for the X-axis
    const days = [];
    for (let i = 6; i >= 0; i--) {
        const date = new Date();
        date.setDate(date.getDate() - i);
        days.push(date.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' }));
    }
    
    // Sample data for demonstration
    const jsePrices = [120, 145, 132, 150, 180, 190, 210];
    const cryptoPrices = [450, 480, 460, 520, 540, 530, 580];
    const forexPrices = [210, 230, 250, 220, 240, 245, 260];
    
    // Create chart
    const acquisitionChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: days,
            datasets: [
                {
                    label: 'JSE (Records)',
                    data: jsePrices,
                    borderColor: '#3498db',
                    backgroundColor: 'rgba(52, 152, 219, 0.1)',
                    tension: 0.4,
                    borderWidth: 2,
                    fill: true
                },
                {
                    label: 'Crypto (Records)',
                    data: cryptoPrices,
                    borderColor: '#f1c40f',
                    backgroundColor: 'rgba(241, 196, 15, 0.1)',
                    tension: 0.4,
                    borderWidth: 2,
                    fill: true
                },
                {
                    label: 'Forex (Records)',
                    data: forexPrices,
                    borderColor: '#2ecc71',
                    backgroundColor: 'rgba(46, 204, 113, 0.1)',
                    tension: 0.4,
                    borderWidth: 2,
                    fill: true
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                },
                tooltip: {
                    mode: 'index',
                    intersect: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                },
                x: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.1)'
                    }
                }
            }
        }
    });
    
    return acquisitionChart;
}

// Initialize and render data type distribution chart
function initDataTypeChart() {
    const ctx = document.getElementById('dataTypeChart').getContext('2d');
    
    // Sample data for demonstration
    const dataTypeChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['OHLCV Prices', 'Fundamentals', 'News', 'Dividends'],
            datasets: [
                {
                    data: [65, 15, 12, 8],
                    backgroundColor: [
                        '#3498db', // blue
                        '#f1c40f', // yellow
                        '#2ecc71', // green
                        '#e74c3c'  // red
                    ],
                    borderWidth: 1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        boxWidth: 15
                    }
                },
                title: {
                    display: true,
                    text: 'Data Types',
                    position: 'top'
                }
            }
        }
    });
    
    return dataTypeChart;
}

// Initialize and render data source distribution chart
function initDataSourceChart() {
    const ctx = document.getElementById('dataSourceChart').getContext('2d');
    
    // Sample data for demonstration
    const dataSourceChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['JSE', 'CoinGecko', 'Alpha Vantage', 'Other'],
            datasets: [
                {
                    data: [35, 40, 20, 5],
                    backgroundColor: [
                        '#3498db', // blue
                        '#f1c40f', // yellow
                        '#2ecc71', // green
                        '#9b59b6'  // purple
                    ],
                    borderWidth: 1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        boxWidth: 15
                    }
                },
                title: {
                    display: true,
                    text: 'Data Sources',
                    position: 'top'
                }
            }
        }
    });
    
    return dataSourceChart;
}

// Function to fetch API health status
async function fetchApiHealth() {
    try {
        const response = await fetch('http://localhost:8000/health');
        
        if (response.ok) {
            const data = await response.json();
            // Update health status indicator in the UI
            document.querySelector('.navbar-text:first-child i').className = 'fas fa-circle text-success me-1';
        } else {
            // Update to show offline status
            document.querySelector('.navbar-text:first-child i').className = 'fas fa-circle text-danger me-1';
        }
    } catch (error) {
        console.error('Error fetching API health:', error);
        // Update to show offline status
        document.querySelector('.navbar-text:first-child i').className = 'fas fa-circle text-danger me-1';
    }
}

// Simulate loading symbols count with incremental counter
function animateSymbolsCount() {
    const element = document.getElementById('totalSymbols');
    const targetValue = 348; // Sample value
    let currentValue = 0;
    const duration = 2000; // ms
    const interval = 50; // ms
    const steps = duration / interval;
    const increment = targetValue / steps;
    
    const counter = setInterval(() => {
        currentValue += increment;
        if (currentValue >= targetValue) {
            clearInterval(counter);
            currentValue = targetValue;
        }
        
        element.textContent = Math.floor(currentValue);
    }, interval);
}

// Simulate loading API requests count with incremental counter
function animateApiRequests() {
    const element = document.getElementById('apiRequests');
    const targetValue = 12754; // Sample value
    let currentValue = 0;
    const duration = 2000; // ms
    const interval = 30; // ms
    const steps = duration / interval;
    const increment = targetValue / steps;
    
    const counter = setInterval(() => {
        currentValue += increment;
        if (currentValue >= targetValue) {
            clearInterval(counter);
            currentValue = targetValue;
        }
        
        element.textContent = Math.floor(currentValue).toLocaleString();
    }, interval);
    
    // Set the change percentage
    document.getElementById('requestChange').textContent = '+23%';
    document.getElementById('requestChange').className = 'text-success';
}

// Add event listener to refresh button
function setupRefreshButton() {
    const refreshBtn = document.getElementById('refreshBtn');
    
    refreshBtn.addEventListener('click', function() {
        // Add spinning animation to refresh icon
        const icon = refreshBtn.querySelector('i');
        icon.classList.add('fa-spin');
        
        // Disable button during refresh
        refreshBtn.disabled = true;
        
        // Simulate data refresh
        setTimeout(() => {
            // Update times
            updateTimes();
            
            // Refresh charts
            initAcquisitionChart();
            initDataTypeChart();
            initDataSourceChart();
            
            // Re-animate numbers
            animateSymbolsCount();
            animateApiRequests();
            
            // Update API health
            fetchApiHealth();
            
            // Remove spinning animation and re-enable button
            icon.classList.remove('fa-spin');
            refreshBtn.disabled = false;
        }, 1500);
    });
}

// Document ready function
document.addEventListener('DOMContentLoaded', function() {
    // Initial setup
    updateTimes();
    initAcquisitionChart();
    initDataTypeChart();
    initDataSourceChart();
    animateSymbolsCount();
    animateApiRequests();
    fetchApiHealth();
    setupRefreshButton();
    
    // Update time every second
    setInterval(updateTimes, 1000);
    
    // Periodically check API health status
    setInterval(fetchApiHealth, 30000); // every 30 seconds
});
