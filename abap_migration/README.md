# ETL Pipeline - PySpark Migration

This project contains a production-ready PySpark ETL pipeline migrated from ABAP. It includes:
- Modular extract/transform/load components
- Configuration management
- Data quality checks
- Monitoring and orchestration
- Comprehensive testing
- CI/CD pipeline with GitHub Actions

## Project Structure
```
.
├── src/
│   ├── extract.py          # Data extraction module
│   ├── transform.py        # Data transformation module
│   ├── load.py             # Data loading module
│   ├── orchestrator.py     # ETL orchestration
│   ├── data_quality.py     # Data quality checks
│   ├── monitor.py          # Monitoring and metrics
│   └── logger.py           # Logging utilities
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   ├── test_load.py
│   ├── test_orchestrator.py
│   └── test_integration.py
├── config.yaml             # Configuration file
├── requirements.txt        # Python dependencies
├── setup.py               # Package setup
├── .github/
│   └── workflows/
│       └── ci-cd.yml      # CI/CD pipeline
└── README.md
```

## Quick Start

### Installation
```bash
pip install -r requirements.txt
```

### Configuration
Edit `config.yaml` to set your environment-specific parameters.

### Running the ETL
```bash
spark-submit src/orchestrator.py --config config.yaml
```

### Running Tests
```bash
pytest tests/ -v --cov=src --cov-report=html
```

## CI/CD Pipeline
The project uses GitHub Actions for automated testing and deployment:
- Automated testing on pull requests
- Code quality checks (linting, type checking)
- Docker image building and publishing
- Deployment to staging/production environments