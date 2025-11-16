"""
NYC Taxi Data Pipeline - Production-Grade OOP Implementation

A modular, object-oriented ETL pipeline for processing NYC taxi data.
Reads from PostgreSQL weekly tables, performs data quality checks,
and generates multiple aggregation outputs.

Architecture:
- Strategy Pattern: Pluggable aggregation strategies
- Builder Pattern: Fluent API for engine configuration
- Dependency Injection: Configurable pipeline components
- SOLID Principles: Single responsibility, extensibility

Standardization:
- Date Format: YYYY-MM-DD (ISO 8601) - sortable, unambiguous, database-friendly
- File Naming: {YYYY}_{MM}_week{N}_{metric}.csv (e.g., 2025_09_week1_trips_per_day.csv)
- Output Structure: output/{week_name}/ (e.g., output/week_1_september_2025/)
- Benefits: Self-documenting, sortable, searchable, organized, portable, archivable
"""

import os
import argparse
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, to_date, date_format, hour, count, sum as spark_sum,
    avg, stddev, round as spark_round, when, lit, unix_timestamp, lag
)
from pyspark.sql.window import Window


# ============================================================================
# DATE STANDARDIZATION
# ============================================================================
# Standard date format: YYYY-MM-DD (ISO 8601)
#
# Reasons for using YYYY-MM-DD format:
# 1. ISO 8601 international standard - universally recognized worldwide
# 2. Sortable - alphabetical sorting equals chronological sorting
# 3. Unambiguous - no confusion between day/month/year order (unlike MM/DD/YYYY vs DD/MM/YYYY)
# 4. Database-friendly - compatible with PostgreSQL DATE type and other SQL databases
# 5. Human-readable - easy to understand at a glance (e.g., 2025-09-01 = September 1, 2025)
# 6. CSV-friendly - works perfectly in Excel, Google Sheets, and data analysis tools
# 7. Programming-friendly - consistent with datetime libraries (Python, JavaScript, etc.)
# 8. No locale issues - works the same across different regional settings
#
# Example: 2025-09-01 (Year-Month-Day)
STANDARD_DATE_FORMAT = "yyyy-MM-dd"

# ============================================================================
# OUTPUT DIRECTORY STRUCTURE
# ============================================================================
# CSV files are organized in week-specific folders with descriptive filenames.
#
# Structure:
# output/
# ├── week_1_september_2025/
# │   ├── 2025_09_week1_trips_per_day.csv
# │   ├── 2025_09_week1_revenue_per_day.csv
# │   ├── 2025_09_week1_peak_hour_per_day.csv
# │   ├── 2025_09_week1_daily_avg_metrics.csv
# │   └── 2025_09_week1_anomaly_monitoring.csv
# └── week_2_september_2025/
#     ├── 2025_09_week2_trips_per_day.csv
#     └── ...
#
# Filename Format: {YYYY}_{MM}_week{N}_{metric}.csv
# Example: 2025_09_week1_trips_per_day.csv
#
# Benefits:
# 1. Self-documenting - Filename shows year, month, week, and metric at a glance
# 2. Sortable - Files naturally sort by time when listed
# 3. Searchable - Easy to find files with patterns (e.g., *week1* or 2025_09*)
# 4. No conflicts - Unique names prevent accidental overwrites
# 5. Folder organized - Each week has its own folder for clean separation
# 6. Portable - Files can be moved/copied independently with full context
# 7. Archive-friendly - Historical data is clearly labeled
# 8. Professional - Standard naming convention for data files

# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass(frozen=True)
class PipelineConfig:
    """Immutable configuration for the data pipeline."""

    # Directory paths
    root_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parent)
    output_dir: Path = field(init=False)

    # PostgreSQL Configuration
    postgres_host: str = field(default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"))
    postgres_port: str = field(default_factory=lambda: os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = field(default_factory=lambda: os.getenv("POSTGRES_DB", "nyc_taxi_db"))
    postgres_user: str = field(default_factory=lambda: os.getenv("POSTGRES_USER", "postgres"))
    postgres_password: str = field(default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "postgres"))

    # PostgreSQL table name (to be set via argument)
    postgres_table: str = "week_1_september_2025"

    # Spark configuration
    app_name: str = "NYC_Taxi_Pipeline"
    adaptive_execution: bool = True
    log_level: str = "ERROR"

    # Data validation thresholds
    min_trip_distance: float = 0.0
    min_total_amount: float = 0.0
    min_passenger_count: int = 0

    # Anomaly detection parameters
    anomaly_std_threshold: float = 1.5
    anomaly_pct_change_threshold: float = -20.0

    def __post_init__(self):
        """Initialize computed fields with week-specific output directory."""
        # Create week-specific folder: output/week_1_september_2025/
        week_output_dir = self.root_dir / "output" / self.postgres_table
        object.__setattr__(self, "output_dir", week_output_dir)

    @property
    def jdbc_url(self) -> str:
        """Get JDBC URL for PostgreSQL connection."""
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"


# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================

class PipelineError(Exception):
    """Base exception for pipeline errors."""
    pass


class DataValidationError(PipelineError):
    """Raised when data validation fails."""
    pass


class SchemaError(PipelineError):
    """Raised when schema transformation fails."""
    pass


class AggregationError(PipelineError):
    """Raised when aggregation computation fails."""
    pass


# ============================================================================
# DATA LOADING
# ============================================================================

class DataLoader:
    """Handles loading and validation of data from PostgreSQL."""

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        """Initialize data loader with Spark session and config."""
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def load_from_postgres(self, table_name: str) -> DataFrame:
        """
        Load data from PostgreSQL table with validation.

        Args:
            table_name: Name of the PostgreSQL table to load

        Returns:
            Loaded DataFrame

        Raises:
            DataValidationError: If table is empty or connection fails
        """
        self.logger.info(f"Loading data from PostgreSQL table: {table_name}")
        self.logger.info(f"Database: {self.config.postgres_host}:{self.config.postgres_port}/{self.config.postgres_db}")

        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.config.jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", self.config.postgres_user) \
                .option("password", self.config.postgres_password) \
                .option("driver", "org.postgresql.Driver") \
                .load()

            row_count = df.count()
            if row_count == 0:
                raise DataValidationError(f"Table '{table_name}' is empty")

            self.logger.info(f"✓ Loaded {row_count:,} rows, {len(df.columns)} columns from '{table_name}'")
            return df

        except Exception as e:
            raise DataValidationError(f"Failed to load from PostgreSQL table '{table_name}': {str(e)}")


# ============================================================================
# SCHEMA TRANSFORMATION
# ============================================================================

class SchemaTransformer(ABC):
    """Abstract base class for schema transformation strategies."""

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform DataFrame schema to normalized format."""
        pass

    @abstractmethod
    def get_taxi_type(self) -> str:
        """Get taxi type identifier."""
        pass


class GreenTaxiTransformer(SchemaTransformer):
    """Transforms Green taxi schema to normalized format."""

    def get_taxi_type(self) -> str:
        return "green"

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Normalize Green taxi schema.

        Renames lpep_pickup_datetime to pickup_datetime and adds taxi_type.
        """
        return (df
                .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
                .withColumn("taxi_type", lit(self.get_taxi_type())))


class YellowTaxiTransformer(SchemaTransformer):
    """Transforms Yellow taxi schema to normalized format."""

    def get_taxi_type(self) -> str:
        return "yellow"

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Normalize Yellow taxi schema.

        Renames tpep_pickup_datetime to pickup_datetime and adds taxi_type.
        """
        return (df
                .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
                .withColumn("taxi_type", lit(self.get_taxi_type())))


# ============================================================================
# DATA CLEANING
# ============================================================================

class DataCleaner:
    """Handles data validation and cleaning operations."""

    def __init__(self, config: PipelineConfig):
        """Initialize cleaner with configuration."""
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def clean(self, df: DataFrame) -> DataFrame:
        """
        Clean and validate DataFrame.

        Removes rows with:
        - Invalid trip distances (< min_trip_distance)
        - Invalid amounts (< min_total_amount)
        - Invalid passenger counts (< min_passenger_count)

        Args:
            df: Input DataFrame

        Returns:
            Cleaned DataFrame
        """
        initial_count = df.count()
        self.logger.info(f"Cleaning data (initial rows: {initial_count:,})")

        cleaned_df = df.filter(
            (col("trip_distance") > self.config.min_trip_distance) &
            (col("total_amount") > self.config.min_total_amount) &
            (col("passenger_count") > self.config.min_passenger_count)
        )

        final_count = cleaned_df.count()
        removed_count = initial_count - final_count
        removal_pct = (removed_count / initial_count) * 100 if initial_count > 0 else 0

        self.logger.info(f"✓ Removed {removed_count:,} invalid rows ({removal_pct:.2f}%)")
        self.logger.info(f"✓ Final clean dataset: {final_count:,} rows")

        return cleaned_df


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def parse_week_name_to_filename_prefix(week_name: str) -> str:
    """
    Parse week_name to generate filename prefix.

    Input: "week_1_september_2025"
    Output: "2025_09_week1"

    Args:
        week_name: Week identifier (e.g., 'week_1_september_2025')

    Returns:
        Filename prefix (e.g., '2025_09_week1')
    """
    import re
    from datetime import datetime

    # Pattern: week_{number}_{month}_{year}
    pattern = r'week_(\d+)_(\w+)_(\d{4})'
    match = re.match(pattern, week_name)

    if not match:
        # Fallback to simple name if pattern doesn't match
        return week_name.replace("_", "-")

    week_num = match.group(1)
    month_name = match.group(2)
    year = match.group(3)

    # Convert month name to number
    try:
        month_obj = datetime.strptime(month_name, "%B")
        month_num = f"{month_obj.month:02d}"
    except:
        month_obj = datetime.strptime(month_name.capitalize(), "%B")
        month_num = f"{month_obj.month:02d}"

    return f"{year}_{month_num}_week{week_num}"


def standardize_date_column(df: DataFrame, date_column: str) -> DataFrame:
    """
    Standardize date column to YYYY-MM-DD format.

    Ensures consistent date formatting across all CSV outputs.

    Args:
        df: Input DataFrame
        date_column: Name of the date column to standardize

    Returns:
        DataFrame with standardized date column
    """
    return df.withColumn(
        date_column,
        date_format(col(date_column), STANDARD_DATE_FORMAT)
    )


# ============================================================================
# AGGREGATION STRATEGIES
# ============================================================================

class AggregationStrategy(ABC):
    """Abstract base class for aggregation computation strategies."""

    @abstractmethod
    def compute(self, df: DataFrame) -> DataFrame:
        """
        Compute aggregation on DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            Aggregated DataFrame
        """
        pass

    @abstractmethod
    def get_name(self) -> str:
        """Get aggregation name for logging and output."""
        pass

    @abstractmethod
    def get_csv_filename(self, week_name: str = "") -> str:
        """
        Get CSV output filename with optional week prefix.

        Args:
            week_name: Week identifier (e.g., 'week_1_september_2025')

        Returns:
            CSV filename with week prefix if provided
        """
        pass


class TripsPerDayAggregation(AggregationStrategy):
    """Computes trip counts per day and taxi type."""

    def get_name(self) -> str:
        return "Trips Per Day"

    def get_csv_filename(self, week_name: str = "") -> str:
        """Generate filename: 2025_09_week1_trips_per_day.csv"""
        if week_name:
            prefix = parse_week_name_to_filename_prefix(week_name)
            return f"{prefix}_trips_per_day.csv"
        return "trips_per_day.csv"

    def compute(self, df: DataFrame) -> DataFrame:
        """Count trips by date and taxi type."""
        df_with_date = df.withColumn("pickup_date", to_date(col("pickup_datetime")))

        result = (df_with_date
                  .groupBy("pickup_date", "taxi_type")
                  .agg(count("*").alias("total_trips"))
                  .withColumnRenamed("pickup_date", "date")
                  .orderBy("date", "taxi_type"))

        # Standardize date format to YYYY-MM-DD
        return standardize_date_column(result, "date")


class RevenuePerDayAggregation(AggregationStrategy):
    """Computes total revenue per day and taxi type."""

    def get_name(self) -> str:
        return "Revenue Per Day"

    def get_csv_filename(self, week_name: str = "") -> str:
        """Generate filename: 2025_09_week1_revenue_per_day.csv"""
        if week_name:
            prefix = parse_week_name_to_filename_prefix(week_name)
            return f"{prefix}_revenue_per_day.csv"
        return "revenue_per_day.csv"

    def compute(self, df: DataFrame) -> DataFrame:
        """Sum revenue by date and taxi type."""
        df_with_date = df.withColumn("pickup_date", to_date(col("pickup_datetime")))

        result = (df_with_date
                  .groupBy("pickup_date", "taxi_type")
                  .agg(spark_round(spark_sum("total_amount"), 2).alias("total_revenue_per_day"))
                  .withColumnRenamed("pickup_date", "date")
                  .orderBy("date", "taxi_type"))

        # Standardize date format to YYYY-MM-DD
        return standardize_date_column(result, "date")


class PeakHourAggregation(AggregationStrategy):
    """Identifies peak hour per day and taxi type."""

    def get_name(self) -> str:
        return "Peak Hour Per Day"

    def get_csv_filename(self, week_name: str = "") -> str:
        """Generate filename: 2025_09_week1_peak_hour_per_day.csv"""
        if week_name:
            prefix = parse_week_name_to_filename_prefix(week_name)
            return f"{prefix}_peak_hour_per_day.csv"
        return "peak_hour_per_day.csv"

    def compute(self, df: DataFrame) -> DataFrame:
        """Find trips per hour for each date and taxi type."""
        df_with_datetime = df.withColumn("pickup_date", to_date(col("pickup_datetime"))) \
                            .withColumn("pickup_hour", hour(col("pickup_datetime")))

        # Return all hourly counts (not just peak) for reporting flexibility
        result = (df_with_datetime
                  .groupBy("pickup_date", "taxi_type", "pickup_hour")
                  .agg(count("*").alias("trips_per_hour"))
                  .withColumnRenamed("pickup_date", "date")
                  .orderBy("date", "taxi_type", "pickup_hour"))

        # Standardize date format to YYYY-MM-DD
        return standardize_date_column(result, "date")


class DailyAvgMetricsAggregation(AggregationStrategy):
    """Computes daily average metrics (distance, fare, duration, passengers)."""

    def get_name(self) -> str:
        return "Daily Average Metrics"

    def get_csv_filename(self, week_name: str = "") -> str:
        """Generate filename: 2025_09_week1_daily_avg_metrics.csv"""
        if week_name:
            prefix = parse_week_name_to_filename_prefix(week_name)
            return f"{prefix}_daily_avg_metrics.csv"
        return "daily_avg_metrics.csv"

    def compute(self, df: DataFrame) -> DataFrame:
        """Calculate average metrics by date and taxi type."""
        df_with_date = df.withColumn("pickup_date", to_date(col("pickup_datetime")))

        result = (df_with_date
                  .groupBy("pickup_date", "taxi_type")
                  .agg(
                      spark_round(avg("trip_distance"), 2).alias("avg_trip_distance"),
                      spark_round(avg("fare_amount"), 2).alias("avg_fare_amount"),
                      spark_round(avg("total_amount"), 2).alias("avg_total_amount"),
                      spark_round(avg("passenger_count"), 2).alias("avg_passenger_count"),
                      spark_round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes")
                  )
                  .withColumnRenamed("pickup_date", "date")
                  .orderBy("date", "taxi_type"))

        # Standardize date format to YYYY-MM-DD
        return standardize_date_column(result, "date")


class AnomalyDetectionAggregation(AggregationStrategy):
    """Detects statistical anomalies in daily trip patterns."""

    def __init__(self, config: PipelineConfig):
        """Initialize with configuration for thresholds."""
        self.config = config

    def get_name(self) -> str:
        return "Anomaly Monitoring"

    def get_csv_filename(self, week_name: str = "") -> str:
        """Generate filename: 2025_09_week1_anomaly_monitoring.csv"""
        if week_name:
            prefix = parse_week_name_to_filename_prefix(week_name)
            return f"{prefix}_anomaly_monitoring.csv"
        return "anomaly_monitoring.csv"

    def compute(self, df: DataFrame) -> DataFrame:
        """
        Detect anomalies using statistical analysis.

        Flags days with:
        - Trip counts beyond std_threshold standard deviations from mean
        - Revenue drops exceeding pct_change_threshold
        - Average passenger count above 2 (unusual for taxis)
        """
        df_with_date = df.withColumn("pickup_date", to_date(col("pickup_datetime")))

        daily_stats = (df_with_date
                      .groupBy("pickup_date", "taxi_type")
                      .agg(
                          count("*").alias("daily_trips"),
                          spark_round(spark_sum("total_amount"), 2).alias("total_revenue_per_day"),
                          spark_round(avg("passenger_count"), 2).alias("avg_passenger_count")
                      ))

        # Compute global statistics per taxi type
        window_spec = Window.partitionBy("taxi_type").orderBy("pickup_date")
        stats_window = Window.partitionBy("taxi_type")

        anomaly_df = daily_stats.withColumn("prev_day_trips", lag("daily_trips", 1).over(window_spec)) \
                                .withColumn("avg_trips", avg("daily_trips").over(stats_window)) \
                                .withColumn("stddev_trips", stddev("daily_trips").over(stats_window))

        # Calculate percentage change
        anomaly_df = anomaly_df.withColumn(
            "pct_change_trips",
            when(col("prev_day_trips").isNotNull() & (col("prev_day_trips") > 0),
                 spark_round(((col("daily_trips") - col("prev_day_trips")) / col("prev_day_trips")) * 100, 2))
            .otherwise(lit(None))
        )

        # Flag anomalies - convert to boolean True/False instead of YES/NO
        std_threshold = self.config.anomaly_std_threshold
        pct_threshold = self.config.anomaly_pct_change_threshold

        anomaly_df = anomaly_df.withColumn(
            "is_anomaly",
            when(
                (col("daily_trips") > col("avg_trips") + std_threshold * col("stddev_trips")) |
                (col("daily_trips") < col("avg_trips") - std_threshold * col("stddev_trips")) |
                (col("pct_change_trips") < pct_threshold) |
                (col("avg_passenger_count") > 2),  # New condition: avg passenger count > 2
                lit(True)
            ).otherwise(lit(False))
        )

        result = (anomaly_df
                  .select("pickup_date", "taxi_type", "daily_trips", "total_revenue_per_day",
                         "avg_passenger_count", "avg_trips", "stddev_trips", "pct_change_trips", "is_anomaly")
                  .withColumnRenamed("pickup_date", "date")
                  .orderBy("date", "taxi_type"))

        # Standardize date format to YYYY-MM-DD
        return standardize_date_column(result, "date")


# ============================================================================
# AGGREGATION ENGINE
# ============================================================================

class AggregationEngine:
    """Manages and executes multiple aggregation strategies."""

    def __init__(self, config: PipelineConfig):
        """Initialize engine with configuration."""
        self.config = config
        self.strategies: List[AggregationStrategy] = []
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def register_strategy(self, strategy: AggregationStrategy) -> 'AggregationEngine':
        """
        Register an aggregation strategy (builder pattern).

        Args:
            strategy: Aggregation strategy to register

        Returns:
            Self for method chaining
        """
        self.strategies.append(strategy)
        return self

    def compute_all(self, df: DataFrame, week_name: str = "") -> Dict[str, Tuple[DataFrame, str]]:
        """
        Execute all registered aggregation strategies.

        Args:
            df: Input DataFrame
            week_name: Week identifier for filename (e.g., 'week_1_september_2025')

        Returns:
            Dictionary mapping strategy names to (DataFrame, csv_filename) tuples

        Raises:
            AggregationError: If any aggregation fails
        """
        results = {}

        for strategy in self.strategies:
            name = strategy.get_name()
            try:
                self.logger.info(f"Computing: {name}")
                result_df = strategy.compute(df)
                csv_filename = strategy.get_csv_filename(week_name)
                results[name] = (result_df, csv_filename)
                self.logger.info(f"✓ {name} completed ({result_df.count():,} rows)")
                self.logger.info(f"  Output: {csv_filename}")
            except Exception as e:
                raise AggregationError(f"Failed to compute {name}: {e}") from e

        return results


# ============================================================================
# OUTPUT MANAGEMENT
# ============================================================================

class OutputManager:
    """Handles saving aggregation results to CSV files."""

    def __init__(self, config: PipelineConfig):
        """Initialize output manager with configuration."""
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def save_csv(self, df: DataFrame, csv_filename: str, display_name: str) -> None:
        """
        Save DataFrame to CSV with display.

        Args:
            df: DataFrame to save
            csv_filename: Output CSV filename
            display_name: Name for logging
        """
        self.logger.info(f"Saving {display_name} to {csv_filename}")

        # Display sample
        df.show(10, truncate=False)

        # Save to CSV
        output_path = self.config.output_dir / csv_filename
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(output_path))

        self.logger.info(f"✓ {display_name} saved to {output_path}")

    def save_all(self, results: Dict[str, Tuple[DataFrame, str]]) -> None:
        """
        Save all aggregation results to CSV files.

        Args:
            results: Dictionary mapping names to (DataFrame, csv_filename) tuples
        """
        # Ensure output directory exists
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

        for display_name, (df, csv_filename) in results.items():
            self.save_csv(df, csv_filename, display_name)


# ============================================================================
# MAIN PIPELINE ORCHESTRATOR
# ============================================================================

class NYCTaxiPipeline:
    """Main pipeline orchestrator using dependency injection."""

    def __init__(self, config: Optional[PipelineConfig] = None):
        """
        Initialize pipeline with configuration.

        Args:
            config: Pipeline configuration (uses default if None)
        """
        self.config = config or PipelineConfig()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.spark: Optional[SparkSession] = None

    def _create_spark_session(self, jdbc_path: str) -> SparkSession:
        """Create and configure Spark session with JDBC driver."""
        self.logger.info("Initializing Spark session")

        builder = SparkSession.builder.appName(self.config.app_name)

        if self.config.adaptive_execution:
            builder = builder.config("spark.sql.adaptive.enabled", "true")

        # Add JDBC driver to Spark classpath
        builder = builder.config("spark.jars", jdbc_path)
        builder = builder.config("spark.driver.extraClassPath", jdbc_path)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(self.config.log_level)

        self.logger.info("✓ Spark session created with PostgreSQL JDBC driver")
        return spark

    def run(self, jdbc_path: str) -> None:
        """
        Execute the complete ETL pipeline.

        Pipeline steps:
        1. Load data from PostgreSQL weekly table
        2. Add trip duration calculation
        3. Clean data
        4. Compute aggregations
        5. Save results to CSV

        Args:
            jdbc_path: Path to PostgreSQL JDBC driver JAR file
        """
        try:
            # Initialize Spark
            self.spark = self._create_spark_session(jdbc_path)

            # Step 1: Load data from PostgreSQL
            self.logger.info("=" * 70)
            self.logger.info("STEP 1: LOADING DATA FROM POSTGRESQL")
            self.logger.info("=" * 70)

            loader = DataLoader(self.spark, self.config)
            all_trips_df = loader.load_from_postgres(self.config.postgres_table)

            # Step 2: Add trip duration
            self.logger.info("\n" + "=" * 70)
            self.logger.info("STEP 2: COMPUTING TRIP DURATIONS")
            self.logger.info("=" * 70)

            # Check if trip_duration_minutes already exists
            if "trip_duration_minutes" not in all_trips_df.columns:
                # Determine which dropoff column to use
                if "lpep_dropoff_datetime" in all_trips_df.columns:
                    dropoff_col = "lpep_dropoff_datetime"
                elif "dropoff_datetime" in all_trips_df.columns:
                    dropoff_col = "dropoff_datetime"
                else:
                    raise DataValidationError("No dropoff datetime column found")

                all_trips_df = all_trips_df.withColumn(
                    "trip_duration_minutes",
                    spark_round(
                        (unix_timestamp(dropoff_col) - unix_timestamp("pickup_datetime")) / 60,
                        2
                    )
                )
                self.logger.info("✓ Trip duration calculated")
            else:
                self.logger.info("✓ Trip duration already exists in data")

            # Step 3: Clean data
            self.logger.info("\n" + "=" * 70)
            self.logger.info("STEP 3: CLEANING DATA")
            self.logger.info("=" * 70)

            cleaner = DataCleaner(self.config)
            cleaned_df = cleaner.clean(all_trips_df)

            # Step 4: Compute aggregations
            self.logger.info("\n" + "=" * 70)
            self.logger.info("STEP 4: COMPUTING AGGREGATIONS")
            self.logger.info("=" * 70)

            engine = AggregationEngine(self.config)
            engine.register_strategy(TripsPerDayAggregation()) \
                  .register_strategy(RevenuePerDayAggregation()) \
                  .register_strategy(PeakHourAggregation()) \
                  .register_strategy(DailyAvgMetricsAggregation()) \
                  .register_strategy(AnomalyDetectionAggregation(self.config))

            # Compute aggregations (saved to output/week_1_september_2025/*.csv)
            aggregation_results = engine.compute_all(cleaned_df, week_name=self.config.postgres_table)

            # Step 5: Save results
            self.logger.info("\n" + "=" * 70)
            self.logger.info("STEP 5: SAVING RESULTS")
            self.logger.info("=" * 70)

            output_manager = OutputManager(self.config)
            output_manager.save_all(aggregation_results)

            # Success
            self.logger.info("\n" + "=" * 70)
            self.logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 70)
            self.logger.info(f"Table processed: {self.config.postgres_table}")
            self.logger.info(f"Results saved to: {self.config.output_dir}")

        except PipelineError as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            raise PipelineError(f"Pipeline execution failed: {e}") from e
        finally:
            if self.spark:
                self.logger.info("Stopping Spark session")
                self.spark.stop()


# ============================================================================
# JDBC DRIVER MANAGEMENT
# ============================================================================

def download_postgres_jdbc() -> str:
    """
    Download PostgreSQL JDBC driver if not present.

    Returns:
        Path to JDBC driver JAR file
    """
    import urllib.request

    root_dir = Path(__file__).resolve().parent
    jdbc_dir = root_dir / "jdbc"
    jdbc_file = jdbc_dir / "postgresql-42.6.0.jar"

    if jdbc_file.exists():
        return str(jdbc_file)

    print("Downloading PostgreSQL JDBC driver...")
    jdbc_dir.mkdir(exist_ok=True)

    jdbc_url = "https://jdbc.postgresql.org/download/postgresql-42.6.0.jar"
    urllib.request.urlretrieve(jdbc_url, str(jdbc_file))

    print(f"✓ JDBC driver downloaded to: {jdbc_file}\n")
    return str(jdbc_file)


# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    """Main entry point for the pipeline."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="NYC Taxi Data Pipeline - Process weekly data from PostgreSQL"
    )
    parser.add_argument(
        "--table",
        type=str,
        default="week_1_september_2025",
        help="PostgreSQL table name to process (e.g., week_1_september_2025)"
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    print("\n" + "█" * 70)
    print("  NYC TAXI DATA PIPELINE - PostgreSQL Weekly Processing")
    print("█" * 70)
    print(f"  Table: {args.table}")
    print("█" * 70 + "\n")

    # Download JDBC driver
    jdbc_path = download_postgres_jdbc()

    # Create config with specified table
    config = PipelineConfig(postgres_table=args.table)

    # Run pipeline
    pipeline = NYCTaxiPipeline(config)
    pipeline.run(jdbc_path)


if __name__ == "__main__":
    main()
