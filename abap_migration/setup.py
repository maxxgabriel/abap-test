"""
Setup configuration for ETL Data Generator package.
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="etl-data-generator",
    version="1.0.0",
    author="ETL Team",
    author_email="etl-team@example.com",
    description="PySpark utility for generating ETL test datasets",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/example/etl-data-generator",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Testing",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.3.0,<4.0.0",
        "PyYAML>=6.0",
        "python-dateutil>=2.8.2",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "pylint>=2.17.0",
            "mypy>=1.4.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "etl-generate=src.data_generator:main",
        ],
    },
)