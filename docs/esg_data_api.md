# African ESG and Infrastructure Data API

## Overview

The ESG (Environmental, Social, Governance) and Infrastructure data API provides access to comprehensive metrics and indicators about regions in Africa. This specialized data vertical focuses on hard-to-find, messy, or unstructured data that has been standardized and made accessible through our API.

The API includes:

1. **Environmental metrics** - Electricity access, water access, waste collection, etc.
2. **Social indicators** - Education enrollment, literacy rates, healthcare access, etc.
3. **Governance assessments** - Service satisfaction, municipal performance, etc.
4. **Infrastructure details** - Schools, healthcare facilities, roads, telecommunications, etc.

## Data Sources

Our ESG data is sourced from:

1. **Statistics South Africa (Stats SA)** - Official statistics and surveys including General Household Survey and Community Survey
2. **OpenStreetMap (OSM)** - Crowdsourced geographic data for infrastructure mapping
3. **Additional government sources** - Provincial and municipal data portals
4. **NGO reports** - Development indicators from international organizations

## API Endpoints

### Regions

Get a list of available regions with ESG data.

```
GET /api/esg/regions
```

**Query Parameters**
- `country` (optional) - Filter by country code (e.g., 'ZA' for South Africa)
- `region_type` (optional) - Filter by region type ('country', 'province', 'municipality')
- `page` (optional) - Page number for pagination
- `page_size` (optional) - Number of items per page

### Available Metrics

Get a list of available metrics for a specific region.

```
GET /api/esg/metrics
```

**Query Parameters**
- `region_code` (required) - Region code (e.g., 'ZA-GT' for Gauteng)
- `dimension` (optional) - Filter by dimension ('environmental', 'social', 'governance', 'infrastructure')

### Environmental Data

Get environmental metrics for a region.

```
GET /api/esg/environmental
```

**Query Parameters**
- `region_code` (required) - Region code
- `metric_types` (optional) - Comma-separated list of metric types
- `start_date` (optional) - Start date in YYYY-MM-DD format
- `end_date` (optional) - End date in YYYY-MM-DD format

### Social Data

Get social metrics for a region.

```
GET /api/esg/social
```

**Query Parameters**
- `region_code` (required) - Region code
- `metric_types` (optional) - Comma-separated list of metric types
- `start_date` (optional) - Start date in YYYY-MM-DD format
- `end_date` (optional) - End date in YYYY-MM-DD format

### Governance Data

Get governance metrics for a region.

```
GET /api/esg/governance
```

**Query Parameters**
- `region_code` (required) - Region code
- `metric_types` (optional) - Comma-separated list of metric types
- `start_date` (optional) - Start date in YYYY-MM-DD format
- `end_date` (optional) - End date in YYYY-MM-DD format

### Infrastructure Data

Get infrastructure metrics for a region.

```
GET /api/esg/infrastructure
```

**Query Parameters**
- `region_code` (required) - Region code
- `metric_types` (optional) - Comma-separated list of metric types
- `start_date` (optional) - Start date in YYYY-MM-DD format
- `end_date` (optional) - End date in YYYY-MM-DD format

### Infrastructure Facilities

Get detailed infrastructure facilities for a region.

```
GET /api/esg/infrastructure/facilities
```

**Query Parameters**
- `region_code` (required) - Region code
- `facility_type` (optional) - Type of facility (school, healthcare, water, power, road)

### ESG Scores

Get composite ESG scores for a region.

```
GET /api/esg/scores
```

**Query Parameters**
- `region_code` (required) - Region code
- `date` (optional) - Date in YYYY-MM-DD format

### Compare Regions

Compare ESG metrics across multiple regions.

```
GET /api/esg/compare
```

**Query Parameters**
- `region_codes` (required) - Comma-separated list of region codes
- `dimension` (optional) - ESG dimension to compare
- `metrics` (optional) - Comma-separated list of metrics to compare
- `date` (optional) - Date in YYYY-MM-DD format

## Examples

### Get available regions in South Africa

```bash
curl -X GET "https://api.wizdata.io/api/esg/regions?country=ZA" \
  -H "Authorization: Bearer YOUR_API_KEY"
```

### Get environmental data for Gauteng province

```bash
curl -X GET "https://api.wizdata.io/api/esg/environmental?region_code=ZA-GT" \
  -H "Authorization: Bearer YOUR_API_KEY"
```

### Compare infrastructure scores across provinces

```bash
curl -X GET "https://api.wizdata.io/api/esg/compare?region_codes=ZA-GT,ZA-WC,ZA-EC&dimension=infrastructure" \
  -H "Authorization: Bearer YOUR_API_KEY"
```

## ESG Scoring Methodology

Our ESG scoring system evaluates regions on a scale of 0-100 across multiple dimensions:

1. **Environmental Score** - Measures environmental sustainability and resource access.
2. **Social Score** - Evaluates social welfare and access to education and healthcare.
3. **Governance Score** - Assesses effectiveness and satisfaction with government services.
4. **Infrastructure Score** - Measures the quality and coverage of physical infrastructure.

The overall ESG score is a weighted average of these dimension scores. Each dimension contains multiple metrics that are normalized and weighted according to their relative importance.

## Data Update Frequency

- Stats SA data: Updated annually or as new surveys are released
- OpenStreetMap data: Updated weekly
- Composite scores: Updated monthly

## Additional Resources

- [Full API documentation](https://docs.wizdata.io/esg-api)
- [ESG data schemas](https://docs.wizdata.io/esg-schemas)
- [Code examples](https://github.com/wizdata/esg-examples)
- [Data coverage map](https://wizdata.io/esg-coverage)