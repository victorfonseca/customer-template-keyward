# Cost of Living Data Pipeline

A comprehensive data pipeline for fetching, analyzing, and visualizing cost of living data across global cities. This project provides automated data collection, statistical analysis, and interactive visualizations to understand cost variations worldwide.

## 🌟 Features

- **Automated Data Fetching**: Retrieves cost of living data from external sources with retry logic and fallback mechanisms
- **Comprehensive Analysis**: Performs statistical analysis on 50+ cost metrics across hundreds of cities
- **Rich Visualizations**: Generates 6 different types of charts and plots for data exploration
- **Interactive Reports**: Creates HTML reports with embedded visualizations and key insights
- **Cloud Storage Integration**: Uses MinIO for distributed storage and data persistence
- **Pipeline Orchestration**: Elyra/Apache Airflow integration for automated workflow execution
- **Configurable Deployments**: Supports multiple environments through DAG key configuration

## 📊 Generated Visualizations

1. **Dataset Overview** - Summary statistics and data quality metrics
2. **City Rankings** - Top and bottom performing cities by key metrics
3. **Correlation Analysis** - Heatmap showing relationships between cost metrics
4. **Country Analysis** - Comparative analysis across different countries
5. **Distribution Analysis** - Statistical distributions with KDE curves
6. **Scatter Matrix** - Multi-dimensional relationship exploration

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Data Fetcher   │───▶│   MinIO Storage  │───▶│   Visualizer    │
│                 │    │                  │    │                 │
│ • HTTP requests │    │ • CSV/JSON data  │    │ • Chart generation
│ • Retry logic   │    │ • Configuration  │    │ • HTML reports
│ • Fallback data │    │ • Results        │    │ • Statistical analysis
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 📁 Project Structure

```
cost-of-living-pipeline/
├── cost_data_fetcher.py      # Data acquisition module
├── cost_data_visualizer.py   # Visualization and analysis module
├── cost-of-living.pipeline   # Elyra pipeline configuration
├── data/                     # Local data storage (auto-created)
├── visualizations/           # Generated charts and reports (auto-created)
└── README.md                # This file
```

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DAG_KEY` | Pipeline instance identifier | `default` |
| `MINIO_ENDPOINT` | MinIO server endpoint | `minio.minio-system.svc.cluster.local:9000` |
| `AWS_ACCESS_KEY_ID` | MinIO access key | `minio` |
| `AWS_SECRET_ACCESS_KEY` | MinIO secret key | `minio123` |


## 📈 Output Files

### Data Files
- `cost_of_living.csv` - Raw cost of living data
- `cost_of_living.json` - JSON formatted data

### Visualizations
- `data_overview.png` - Dataset summary and quality metrics
- `city_rankings.png` - Top/bottom city rankings
- `correlation_matrix.png` - Metric correlation heatmap
- `country_analysis.png` - Country-level comparisons
- `distribution_analysis.png` - Statistical distributions
- `scatter_matrix.png` - Multi-dimensional scatter plots

### Reports
- `comprehensive_cost_analysis.html` - Interactive HTML report with all visualizations and insights


## 📊 Sample Data Structure

The pipeline works with datasets containing these columns:

| Column | Type | Description |
|--------|------|-------------|
| `city` | string | City name |
| `country` | string | Country name |
| `x1`, `x2`, ... | numeric | Cost metrics (housing, food, transport, etc.) |
| `data_quality` | numeric | Data quality score (0-100) |