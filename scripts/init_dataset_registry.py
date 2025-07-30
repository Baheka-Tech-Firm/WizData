#!/usr/bin/env python3
"""
Dataset Registry Initialization Script
Seeds the database with initial datasets, licenses, and sample data
"""

from app import create_app, db
from models import (
    Dataset, DatasetLicense, User, DataSource, Region, Symbol,
    DatasetCategory, LicenseType, SubscriptionTier
)
from datetime import datetime, date
from decimal import Decimal
import json

def create_sample_datasets():
    """Create sample datasets for the B2B platform"""
    
    # JSE Market Data Dataset
    jse_dataset = Dataset(
        name="JSE Market Data",
        slug="jse-market-data",
        description="Real-time and historical market data from the Johannesburg Stock Exchange (JSE), including OHLCV data, corporate actions, and market indices.",
        category=DatasetCategory.MARKET_DATA,
        provider="JSE",
        version="1.0.0",
        schema_definition={
            "fields": [
                {"name": "symbol", "type": "string", "description": "Stock symbol"},
                {"name": "timestamp", "type": "datetime", "description": "Data timestamp"},
                {"name": "open", "type": "number", "description": "Opening price"},
                {"name": "high", "type": "number", "description": "Highest price"},
                {"name": "low", "type": "number", "description": "Lowest price"},
                {"name": "close", "type": "number", "description": "Closing price"},
                {"name": "volume", "type": "integer", "description": "Trading volume"}
            ]
        },
        update_frequency="real-time",
        historical_depth="20 years",
        geographic_coverage=["South Africa"],
        data_formats=["json", "csv"],
        api_endpoints=["/api/v1/data/market/jse-market-data/symbols", "/api/v1/data/market/jse-market-data/historical"],
        price_per_record=Decimal('0.001'),
        price_per_api_call=Decimal('0.01'),
        monthly_subscription_price=Decimal('299.00'),
        annual_subscription_price=Decimal('2990.00'),
        data_quality_score=95.5,
        completeness_score=98.2,
        accuracy_score=99.1,
        latency_score=92.8,
        is_active=True,
        is_public=True,
        documentation_url="https://docs.wizdata.co/datasets/jse-market-data",
        sample_data_url="https://samples.wizdata.co/jse-market-data.json",
        total_records=5000000
    )
    
    # SADC ESG Data Dataset
    sadc_esg_dataset = Dataset(
        name="SADC ESG Intelligence",
        slug="sadc-esg-intelligence",
        description="Comprehensive Environmental, Social, and Governance data for SADC (Southern African Development Community) countries, including infrastructure metrics, SDG indicators, and governance scores.",
        category=DatasetCategory.ESG_DATA,
        provider="WizData",
        version="2.1.0",
        schema_definition={
            "fields": [
                {"name": "region_id", "type": "integer", "description": "Region identifier"},
                {"name": "metric_type", "type": "string", "description": "Type of metric"},
                {"name": "value", "type": "number", "description": "Metric value"},
                {"name": "unit", "type": "string", "description": "Unit of measurement"},
                {"name": "date", "type": "date", "description": "Measurement date"},
                {"name": "confidence", "type": "number", "description": "Data confidence score"}
            ]
        },
        update_frequency="monthly",
        historical_depth="10 years",
        geographic_coverage=["Angola", "Botswana", "Comoros", "Democratic Republic of Congo", "Eswatini", "Lesotho", "Madagascar", "Malawi", "Mauritius", "Mozambique", "Namibia", "Seychelles", "South Africa", "Tanzania", "Zambia", "Zimbabwe"],
        data_formats=["json", "csv", "geojson"],
        api_endpoints=["/api/v1/data/esg/sadc-esg-intelligence/regions", "/api/v1/data/esg/sadc-esg-intelligence/metrics"],
        monthly_subscription_price=Decimal('499.00'),
        annual_subscription_price=Decimal('4990.00'),
        data_quality_score=88.7,
        completeness_score=85.3,
        accuracy_score=91.2,
        latency_score=78.5,
        is_active=True,
        is_public=True,
        documentation_url="https://docs.wizdata.co/datasets/sadc-esg-intelligence",
        sample_data_url="https://samples.wizdata.co/sadc-esg-intelligence.json",
        total_records=2500000
    )
    
    # African News Intelligence Dataset
    african_news_dataset = Dataset(
        name="African News Intelligence",
        slug="african-news-intelligence",
        description="Real-time news aggregation and sentiment analysis from major African news sources, with topic classification and entity extraction.",
        category=DatasetCategory.NEWS_DATA,
        provider="WizData",
        version="1.5.0",
        schema_definition={
            "fields": [
                {"name": "article_id", "type": "string", "description": "Unique article identifier"},
                {"name": "title", "type": "string", "description": "Article title"},
                {"name": "content", "type": "text", "description": "Article content"},
                {"name": "source", "type": "string", "description": "News source"},
                {"name": "published_date", "type": "datetime", "description": "Publication date"},
                {"name": "sentiment", "type": "number", "description": "Sentiment score (-1 to 1)"},
                {"name": "topics", "type": "array", "description": "Topic classifications"},
                {"name": "entities", "type": "array", "description": "Extracted entities"}
            ]
        },
        update_frequency="real-time",
        historical_depth="5 years",
        geographic_coverage=["Africa"],
        data_formats=["json"],
        api_endpoints=["/api/v1/data/news/african-news-intelligence/articles"],
        price_per_record=Decimal('0.005'),
        monthly_subscription_price=Decimal('199.00'),
        annual_subscription_price=Decimal('1990.00'),
        data_quality_score=92.1,
        completeness_score=94.7,
        accuracy_score=89.3,
        latency_score=96.2,
        is_active=True,
        is_public=True,
        documentation_url="https://docs.wizdata.co/datasets/african-news-intelligence",
        sample_data_url="https://samples.wizdata.co/african-news-intelligence.json",
        total_records=15000000
    )
    
    # Economic Indicators Dataset
    economic_dataset = Dataset(
        name="African Economic Indicators",
        slug="african-economic-indicators",
        description="Key economic indicators for African countries including GDP, inflation, unemployment, trade balance, and central bank rates.",
        category=DatasetCategory.ECONOMIC_DATA,
        provider="IMF/World Bank",
        version="1.0.0",
        schema_definition={
            "fields": [
                {"name": "country", "type": "string", "description": "Country name"},
                {"name": "indicator", "type": "string", "description": "Economic indicator"},
                {"name": "value", "type": "number", "description": "Indicator value"},
                {"name": "unit", "type": "string", "description": "Unit of measurement"},
                {"name": "period", "type": "string", "description": "Time period"},
                {"name": "frequency", "type": "string", "description": "Data frequency"}
            ]
        },
        update_frequency="quarterly",
        historical_depth="30 years",
        geographic_coverage=["Africa"],
        data_formats=["json", "csv"],
        api_endpoints=["/api/v1/data/economic/african-economic-indicators"],
        monthly_subscription_price=Decimal('399.00'),
        annual_subscription_price=Decimal('3990.00'),
        data_quality_score=96.3,
        completeness_score=91.8,
        accuracy_score=98.7,
        latency_score=71.2,
        is_active=True,
        is_public=True,
        documentation_url="https://docs.wizdata.co/datasets/african-economic-indicators",
        sample_data_url="https://samples.wizdata.co/african-economic-indicators.json",
        total_records=500000
    )
    
    datasets = [jse_dataset, sadc_esg_dataset, african_news_dataset, economic_dataset]
    
    for dataset in datasets:
        db.session.add(dataset)
    
    db.session.commit()
    print(f"Created {len(datasets)} sample datasets")
    
    return datasets

def create_dataset_licenses(datasets):
    """Create license tiers for each dataset"""
    
    licenses = []
    
    for dataset in datasets:
        # Free tier
        free_license = DatasetLicense(
            dataset_id=dataset.id,
            license_type=LicenseType.FREE,
            name=f"{dataset.name} - Free Tier",
            description="Limited access for evaluation and development",
            rate_limit_per_minute=10,
            rate_limit_per_day=1000,
            rate_limit_per_month=10000,
            max_records_per_request=100,
            historical_access_days=30,
            monthly_price=Decimal('0.00'),
            annual_price=Decimal('0.00'),
            real_time_access=False,
            bulk_download=False,
            api_access=True,
            webhook_support=False,
            custom_queries=False,
            white_label_access=False,
            redistribution_allowed=False,
            commercial_use_allowed=False,
            attribution_required=True,
            terms_of_use="For evaluation and non-commercial use only. Attribution required."
        )
        
        # Basic tier
        basic_license = DatasetLicense(
            dataset_id=dataset.id,
            license_type=LicenseType.BASIC,
            name=f"{dataset.name} - Basic",
            description="Standard access for small businesses and startups",
            rate_limit_per_minute=60,
            rate_limit_per_day=10000,
            rate_limit_per_month=100000,
            max_records_per_request=1000,
            historical_access_days=365,
            monthly_price=dataset.monthly_subscription_price * Decimal('0.5') if dataset.monthly_subscription_price else Decimal('99.00'),
            annual_price=dataset.annual_subscription_price * Decimal('0.5') if dataset.annual_subscription_price else Decimal('990.00'),
            real_time_access=False,
            bulk_download=False,
            api_access=True,
            webhook_support=False,
            custom_queries=True,
            white_label_access=False,
            redistribution_allowed=False,
            commercial_use_allowed=True,
            attribution_required=True
        )
        
        # Premium tier
        premium_license = DatasetLicense(
            dataset_id=dataset.id,
            license_type=LicenseType.PREMIUM,
            name=f"{dataset.name} - Premium",
            description="Full access for professional use with real-time data",
            rate_limit_per_minute=200,
            rate_limit_per_day=50000,
            rate_limit_per_month=500000,
            max_records_per_request=5000,
            historical_access_days=None,  # Unlimited
            monthly_price=dataset.monthly_subscription_price or Decimal('299.00'),
            annual_price=dataset.annual_subscription_price or Decimal('2990.00'),
            real_time_access=True,
            bulk_download=True,
            api_access=True,
            webhook_support=True,
            custom_queries=True,
            white_label_access=False,
            redistribution_allowed=False,
            commercial_use_allowed=True,
            attribution_required=True
        )
        
        # Enterprise tier
        enterprise_license = DatasetLicense(
            dataset_id=dataset.id,
            license_type=LicenseType.ENTERPRISE,
            name=f"{dataset.name} - Enterprise",
            description="Unlimited access for large organizations with white-label options",
            rate_limit_per_minute=1000,
            rate_limit_per_day=1000000,
            rate_limit_per_month=10000000,
            max_records_per_request=50000,
            historical_access_days=None,  # Unlimited
            monthly_price=dataset.monthly_subscription_price * Decimal('3.0') if dataset.monthly_subscription_price else Decimal('999.00'),
            annual_price=dataset.annual_subscription_price * Decimal('3.0') if dataset.annual_subscription_price else Decimal('9990.00'),
            real_time_access=True,
            bulk_download=True,
            api_access=True,
            webhook_support=True,
            custom_queries=True,
            white_label_access=True,
            redistribution_allowed=True,
            commercial_use_allowed=True,
            attribution_required=False
        )
        
        dataset_licenses = [free_license, basic_license, premium_license, enterprise_license]
        
        for license in dataset_licenses:
            db.session.add(license)
            licenses.append(license)
    
    db.session.commit()
    print(f"Created {len(licenses)} dataset licenses")
    
    return licenses

def create_sample_users():
    """Create sample B2B users"""
    
    users = [
        User(
            email="demo@fintech-startup.co.za",
            company_name="FinTech Innovations (Pty) Ltd",
            contact_name="Sarah Johnson",
            phone="+27 11 123 4567",
            country="South Africa",
            industry="Financial Services",
            company_size="startup",
            use_case="Building a robo-advisor platform for South African investors",
            subscription_tier=SubscriptionTier.STARTER,
            monthly_spend_limit=Decimal('500.00'),
            is_active=True,
            is_verified=True,
            onboarded_at=datetime.utcnow()
        ),
        User(
            email="analytics@mining-corp.com",
            company_name="Southern Mining Analytics",
            contact_name="Michael Chen",
            phone="+27 21 987 6543",
            country="South Africa",
            industry="Mining & Resources",
            company_size="enterprise",
            use_case="ESG risk assessment and compliance monitoring for mining operations across SADC region",
            subscription_tier=SubscriptionTier.ENTERPRISE,
            monthly_spend_limit=Decimal('10000.00'),
            is_active=True,
            is_verified=True,
            onboarded_at=datetime.utcnow()
        ),
        User(
            email="research@university.ac.za",
            company_name="University of Cape Town - Data Science Institute",
            contact_name="Dr. Amara Okafor",
            phone="+27 21 650 9111",
            country="South Africa",
            industry="Education & Research",
            company_size="sme",
            use_case="Academic research on African economic development and SDG progress tracking",
            subscription_tier=SubscriptionTier.PROFESSIONAL,
            monthly_spend_limit=Decimal('1000.00'),
            is_active=True,
            is_verified=True,
            onboarded_at=datetime.utcnow()
        ),
        User(
            email="data@consulting-firm.com",
            company_name="African Insights Consulting",
            contact_name="James Mwangi",
            phone="+254 20 123 4567",
            country="Kenya",
            industry="Consulting",
            company_size="sme",
            use_case="Market research and business intelligence for multinational clients entering African markets",
            subscription_tier=SubscriptionTier.PROFESSIONAL,
            monthly_spend_limit=Decimal('2000.00'),
            is_active=True,
            is_verified=True,
            onboarded_at=datetime.utcnow()
        )
    ]
    
    for user in users:
        db.session.add(user)
    
    db.session.commit()
    print(f"Created {len(users)} sample users")
    
    return users

def create_sample_data_sources():
    """Create sample data sources"""
    
    sources = [
        DataSource(
            name="JSE Market Data Feed",
            type="market_data",
            url="https://api.jse.co.za/v1/",
            description="Official market data from Johannesburg Stock Exchange",
            is_active=True,
            fetch_frequency=1  # Every minute
        ),
        DataSource(
            name="Alpha Vantage",
            type="market_data",
            url="https://www.alphavantage.co/",
            description="Global market data provider",
            is_active=True,
            fetch_frequency=5
        ),
        DataSource(
            name="GDELT News Feed",
            type="news",
            url="https://api.gdeltproject.org/",
            description="Global news and events database",
            is_active=True,
            fetch_frequency=15
        ),
        DataSource(
            name="World Bank Open Data",
            type="economic",
            url="https://api.worldbank.org/",
            description="World Bank economic indicators",
            is_active=True,
            fetch_frequency=1440  # Daily
        ),
        DataSource(
            name="IMF Data",
            type="economic",
            url="https://data.imf.org/",
            description="International Monetary Fund economic data",
            is_active=True,
            fetch_frequency=1440
        ),
        DataSource(
            name="African Development Bank",
            type="economic",
            url="https://dataportal.opendataforafrica.org/",
            description="Economic and development data for Africa",
            is_active=True,
            fetch_frequency=1440
        )
    ]
    
    for source in sources:
        db.session.add(source)
    
    db.session.commit()
    print(f"Created {len(sources)} data sources")
    
    return sources

def create_sample_regions():
    """Create sample African regions"""
    
    regions = [
        # Countries
        Region(
            name="South Africa",
            code="ZA",
            region_type="country",
            country="South Africa",
            population=59308690,
            area_km2=1221037
        ),
        Region(
            name="Nigeria",
            code="NG",
            region_type="country",
            country="Nigeria",
            population=206139589,
            area_km2=923768
        ),
        Region(
            name="Kenya",
            code="KE",
            region_type="country",
            country="Kenya",
            population=53771296,
            area_km2=580367
        ),
        Region(
            name="Ghana",
            code="GH",
            region_type="country",
            country="Ghana",
            population=31072940,
            area_km2=238533
        ),
        Region(
            name="Botswana",
            code="BW",
            region_type="country",
            country="Botswana",
            population=2351627,
            area_km2=581730
        )
    ]
    
    for region in regions:
        db.session.add(region)
    
    db.session.commit()
    
    # Add provinces for South Africa
    sa_region = Region.query.filter_by(code="ZA").first()
    
    provinces = [
        Region(
            name="Gauteng",
            code="ZA-GP",
            region_type="province",
            parent_id=sa_region.id,
            country="South Africa",
            population=15176115,
            area_km2=18178
        ),
        Region(
            name="Western Cape",
            code="ZA-WC",
            region_type="province",
            parent_id=sa_region.id,
            country="South Africa",
            population=6844272,
            area_km2=129462
        ),
        Region(
            name="KwaZulu-Natal",
            code="ZA-KZN",
            region_type="province",
            parent_id=sa_region.id,
            country="South Africa",
            population=11289086,
            area_km2=94361
        )
    ]
    
    for province in provinces:
        db.session.add(province)
    
    db.session.commit()
    print(f"Created {len(regions) + len(provinces)} regions")
    
    return regions + provinces

def create_sample_symbols():
    """Create sample JSE symbols"""
    
    symbols = [
        Symbol(
            symbol="SBK",
            name="Standard Bank Group Limited",
            exchange="JSE",
            asset_type="stock",
            sector="Financial Services",
            country="South Africa",
            is_active=True,
            last_price=185.50
        ),
        Symbol(
            symbol="FSR",
            name="FirstRand Limited",
            exchange="JSE",
            asset_type="stock",
            sector="Financial Services",
            country="South Africa",
            is_active=True,
            last_price=65.20
        ),
        Symbol(
            symbol="NED",
            name="Nedbank Group Limited",
            exchange="JSE",
            asset_type="stock",
            sector="Financial Services",
            country="South Africa",
            is_active=True,
            last_price=175.80
        ),
        Symbol(
            symbol="AGL",
            name="Anglo American plc",
            exchange="JSE",
            asset_type="stock",
            sector="Mining",
            country="South Africa",
            is_active=True,
            last_price=420.75
        ),
        Symbol(
            symbol="BHP",
            name="BHP Group Limited",
            exchange="JSE",
            asset_type="stock",
            sector="Mining",
            country="South Africa",
            is_active=True,
            last_price=385.90
        ),
        Symbol(
            symbol="MTN",
            name="MTN Group Limited",
            exchange="JSE",
            asset_type="stock",
            sector="Telecommunications",
            country="South Africa",
            is_active=True,
            last_price=95.40
        ),
        Symbol(
            symbol="VOD",
            name="Vodacom Group Limited",
            exchange="JSE",
            asset_type="stock",
            sector="Telecommunications",
            country="South Africa",
            is_active=True,
            last_price=125.30
        ),
        Symbol(
            symbol="SHP",
            name="Shoprite Holdings Limited",
            exchange="JSE",
            asset_type="stock",
            sector="Consumer Goods",
            country="South Africa",
            is_active=True,
            last_price=165.25
        ),
        Symbol(
            symbol="PIK",
            name="Pick n Pay Stores Limited",
            exchange="JSE",
            asset_type="stock",
            sector="Consumer Goods",
            country="South Africa",
            is_active=True,
            last_price=55.80
        ),
        Symbol(
            symbol="NPN",
            name="Naspers Limited",
            exchange="JSE",
            asset_type="stock",
            sector="Technology",
            country="South Africa",
            is_active=True,
            last_price=3250.00
        )
    ]
    
    for symbol in symbols:
        db.session.add(symbol)
    
    db.session.commit()
    print(f"Created {len(symbols)} sample symbols")
    
    return symbols

def main():
    """Initialize the dataset registry with sample data"""
    
    app = create_app()
    
    with app.app_context():
        print("Initializing WizData Dataset Registry...")
        
        # Create all tables
        print("Creating database tables...")
        db.create_all()
        
        # Check if data already exists
        if Dataset.query.first():
            print("Dataset registry already initialized. Skipping...")
            return
        
        # Create sample data
        print("\nCreating sample data...")
        
        # Create data sources first
        sources = create_sample_data_sources()
        
        # Create regions and symbols
        regions = create_sample_regions()
        symbols = create_sample_symbols()
        
        # Create datasets
        datasets = create_sample_datasets()
        
        # Create licenses for datasets
        licenses = create_dataset_licenses(datasets)
        
        # Create sample users
        users = create_sample_users()
        
        print(f"\n✅ Dataset registry initialized successfully!")
        print(f"   - {len(datasets)} datasets")
        print(f"   - {len(licenses)} licenses")
        print(f"   - {len(users)} sample users")
        print(f"   - {len(sources)} data sources")
        print(f"   - {len(regions)} regions")
        print(f"   - {len(symbols)} market symbols")
        
        print(f"\n🔗 Access the dataset catalog at: http://localhost:5000/api/v1/datasets/")
        print(f"📖 API documentation at: http://localhost:5000/api/docs")

if __name__ == "__main__":
    main()
