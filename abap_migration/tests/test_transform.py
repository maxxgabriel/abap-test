"""
Unit tests for Transform module
"""

import pytest
from decimal import Decimal
from pyspark.sql.functions import col
from src.transform import Transformer
from src.test_utils import (
    SparkTestFixture,
    DataFrameAssertions,
    TestDataFactory,
    MockLogger
)


class TestTransformer:
    """Test suite for Transformer class"""
    
    def test_basic_transformation(self, spark_fixture: SparkTestFixture):
        """Test basic data transformation"""
        # Setup
        source_df = spark_fixture.create_source_table(count=10)
        transformer = Transformer(
            spark=spark_fixture.spark,
            run_id="TEST001"
        )
        
        # Execute
        transformed_df = transformer.transform_data(source_df)
        
        # Assert
        assert transformed_df.count() == 10
        assert "transformed_value" in transformed_df.columns
        assert "priority" in transformed_df.columns
    
    def test_name_normalization(self, spark_fixture: SparkTestFixture):
        """Test name field normalization"""
        # Setup
        data = TestDataFactory.create_source_records(count=5)
        data[0]["name"] = "  test  product  "
        data[1]["name"] = "lowercase name"
        
        source_df = spark_fixture.create_temp_table(
            "test_source",
            data,
            TestSchemas.source_schema()
        )
        
        transformer = Transformer(
            spark=spark_fixture.spark,
            run_id="TEST002"
        )
        
        # Execute
        transformed_df = transformer.transform_data(source_df)
        
        # Assert - names should be uppercase and trimmed
        names = [row.name for row in transformed_df.collect()]
        assert all(name == name.upper() for name in names)
        assert all(" =" not in name for name in names)  # No double spaces
    
    def test_value_calculation(self, spark_fixture: SparkTestFixture):
        """Test transformed value calculation"""
        # Setup
        source_df = spark_fixture.create_source_table(count=10, base_value=Decimal("100"))
        transformer = Transformer(
            spark=spark_fixture.spark,
            run_id="TEST003"
        )
        
        # Execute
        transformed_df = transformer.transform_data(source_df)
        
        # Assert - transformed_value should be different from value
        for row in transformed_df.collect():
            assert row.transformed_value != row.value
            assert row.transformed_value > 0
    
    def test_priority_calculation(self, spark_fixture: SparkTestFixture):
        """Test priority calculation"""
        # Setup
        data = TestDataFactory.create_source_records(count=3)
        data[0]["value"] = Decimal("1500")  # High priority
        data[1]["value"] = Decimal("500")   # Medium priority
        data[2]["value"] = Decimal("100")   # Low priority
        
        source_df = spark_fixture.create_temp_table(
            "priority_test",
            data,
            TestSchemas.source_schema()
        )
        
        transformer = Transformer(
            spark=spark_fixture.spark,
            run_id="TEST004"
        )
        
        # Execute
        transformed_df = transformer.transform_data(source_df)
        
        # Assert
        rows = transformed_df.collect()
        assert rows[0].priority >= 1
        assert rows[0].priority <= 5
    
    def test_category_rules(self, spark_fixture: SparkTestFixture):
        """Test category-specific transformation rules"""
        # Setup
        data = TestDataFactory.create_source_records(count=2)
        data[0]["category"] = "PREMIUM"
        data[1]["category"] = "STANDARD"
        
        source_df = spark_fixture.create_temp_table(
            "category_test",
            data,
            TestSchemas.source_schema()
        )
        
        transformer = Transformer(
            spark=spark_fixture.spark,
            run_id="TEST005"
        )
        
        # Execute
        transformed_df = transformer.transform_data(source_df)
        
        # Assert - PREMIUM should have higher transformed value
        rows = transformed_df.collect()
        premium_row = [r for r in rows if r.category == "PREMIUM"][0]
        standard_row = [r for r in rows if r.category == "STANDARD"][0]
        
        # Premium multiplier should be applied
        assert premium_row.transformed_value > premium_row.value
    
    def test_status_assignment(self, spark_fixture: SparkTestFixture):
        """Test status field assignment based on value"""
        # Setup
        data = TestDataFactory.create_source_records(count=3)
        data[0]["value"] = Decimal("1000")
        data[1]["value"] = Decimal("400")
        data[2]["value"] = Decimal("100")
        
        source_df = spark_fixture.create_temp_table(
            "status_test",
            data,
            TestSchemas.source_schema()
        )
        
        transformer = Transformer(
            spark=spark_fixture.spark,
            run_id="TEST006"
        )
        
        # Execute
        transformed_df = transformer.transform_data(source_df)
        
        # Assert
        rows = transformed_df.collect()
        assert rows[0].status in ["HIGH_VALUE", "MEDIUM_VALUE", "LOW_VALUE"]
    
    def test_run_id_stamping(self, spark_fixture: SparkTestFixture):
        """Test run ID is stamped on all records"""
        # Setup
        source_df = spark_fixture.create_source_table(count=5)
        run_id = "TEST007"
        transformer = Transformer(
            spark=spark_fixture.spark,
            run_id=run_id
        )
        
        # Execute
        transformed_df = transformer.transform_data(source_df)
        
        # Assert
        for row in transformed_df.collect():
            assert row.etl_run_id == run_id
    
    def test_timestamp_stamping(self, spark_fixture: SparkTestFixture):
        """Test timestamp is added to all records"""
        # Setup
        source_df = spark_fixture.create_source_table(count=5)
        transformer = Transformer(
            spark=spark_fixture.spark,
            run_id="TEST008"
        )
        
        # Execute
        transformed_df = transformer.transform_data(source_df)
        
        # Assert
        for row in transformed_df.collect():
            assert row.processed_at is not None
            assert row.processed_by is not None