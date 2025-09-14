# Jaffle Shop DLT Pipeline

This repository contains a DLT (Data Load Tool) pipeline that extracts data from the Jaffle Shop API and loads it into a DuckDB database.

## Features

- Parallel data extraction from multiple endpoints (customers, orders, products)
- Optimized performance with worker configuration
- Automated daily runs via GitHub Actions
- Configurable destination and dataset name

## Setup

1. Clone this repository:
```bash
git clone <your-repo-url>
cd dlthub
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configuration:
The pipeline is configured using the `.dlt/config.toml` file, which specifies:
- Database configuration (DuckDB)
- Schema settings
- Runtime configuration

You can also override settings with environment variables:
```bash
export DLT_DESTINATION=duckdb  # or your preferred destination
export DLT_DATASET=jaffle_shop  # or your preferred dataset name
```

## Running the Pipeline

To run the pipeline locally:
```bash
python jaffle_shop_pipeline.py
```

## GitHub Actions Deployment

The pipeline is configured to run automatically every day at midnight UTC. You can also trigger it manually from the Actions tab in GitHub.

### Setting up GitHub Actions

1. Go to your repository's Settings > Secrets and Variables > Actions
2. Add the following secrets if needed:
   - `DLT_DESTINATION`: Your desired destination (defaults to "duckdb")
   - `DLT_DATASET`: Your desired dataset name (defaults to "jaffle_shop")

## Pipeline Structure

- `jaffle_shop_pipeline.py`: Main pipeline code
- `.github/workflows/pipeline.yml`: GitHub Actions workflow configuration
- `requirements.txt`: Python dependencies

## Performance Optimizations

The pipeline includes several optimizations:
- Parallel data extraction with configurable workers
- Chunked data loading
- Configurable buffer sizes
- File rotation for large datasets

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request
