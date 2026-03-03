# Sample ETL ABAP Project

This is a sample ETL (Extract, Transform, Load) ABAP project for testing purposes.

## Project Structure

```
├── src/               # Source code
│   ├── extract/       # Data extraction programs
│   ├── transform/     # Data transformation logic
│   ├── load/          # Data loading programs
│   └── utils/         # Utility classes and functions
├── data/              # Sample data files
│   ├── input/         # Input data files
│   └── output/        # Output data files
├── config/            # Configuration files
└── tests/             # Test programs
```

## Components

### Extraction
- **ZCL_ETL_EXTRACTOR**: Main extraction class
- **Z_EXTRACT_DATA**: Report program for data extraction

### Transformation
- **ZCL_ETL_TRANSFORMER**: Data transformation class
- **Z_TRANSFORM_DATA**: Report program for transformations

### Loading
- **ZCL_ETL_LOADER**: Data loading class
- **Z_LOAD_DATA**: Report program for data loading

## Usage

1. Configure source and target systems in config files
2. Run extraction program to pull data
3. Apply transformations as needed
4. Load transformed data to target
