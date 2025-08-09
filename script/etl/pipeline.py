#!/usr/bin/env python3

import polars as pl
import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

db_user = os.getenv("MYSQL_USER", "")
db_password = os.getenv("MYSQL_PASSWORD", "")
db_host = os.getenv("MYSQL_HOST", "")
db_name = os.getenv("MYSQL_DATABASE", "")

IOT_DATA_PATH = "./data/iot/equipment_sensors.csv"


def load_sql_data(
    host: str, user: str, password: str, database: str, query: str
) -> pl.DataFrame:
    """
    Load data from a SQL database using Polars.

    Args:
        host (str): Database host.
        user (str): Database user.
        password (str): Database password.
        database (str): Database name.
        query (str): SQL query to execute.

    Returns:dock
        pl.DataFrame: DataFrame containing the result of the query.
    """
    if not host:
        raise ValueError("Database host is not set")
    if not user:
        raise ValueError("Database user is not set")
    if not password:
        raise ValueError("Database password is not set")
    if not database:
        raise ValueError("Database name is not set")
    if not query:
        raise ValueError("SQL query is not set")

    return pl.read_database_uri(query, f"mysql://{user}:{password}@{host}/{database}")


def load_iot_data(PATH: str) -> pl.DataFrame:
    """
    Load IoT data from a CSV file using Polars.

    Args:
        PATH (str): Path to the CSV file.

    Returns:
        pl.DataFrame: DataFrame containing the IoT data.
    """
    if not PATH:
        raise ValueError("Path to IoT data is not set")

    if not os.path.exists(PATH):
        raise FileNotFoundError(f"The file at {PATH} does not exist")

    return pl.read_csv(PATH, infer_schema_length=0, use_pyarrow=True)


def load_weather_data(start_date, end_date) -> pl.DataFrame:
    """
    Load weather data from a CSV file using Polars.

    Args:
        start_date (str): Start date for filtering the data. With format "YYYY-MM-DD".
        end_date (str): End date for filtering the data. With format "YYYY-MM-DD".

    Returns:
        pl.DataFrame: DataFrame containing the weather data.
    """

    def is_valid_date(start_date: str) -> bool:
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    if not start_date or not end_date:
        raise ValueError("Start date and end date must be provided")

    if not is_valid_date(start_date) or not is_valid_date(end_date):
        raise ValueError("Start date and end date must be in the format YYYY-MM-DD")

    if datetime.strptime(start_date, "%Y-%m-%d") > datetime.strptime(
        end_date, "%Y-%m-%d"
    ):
        raise ValueError("Start date must be before end date")

    start_date_ = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_ = datetime.strptime(end_date, "%Y-%m-%d")
    split_date = datetime.now() - timedelta(days=90)
    split_date_ = split_date.strftime("%Y-%m-%d")

    if start_date_ < split_date and end_date_ >= split_date:
        API_PATH_ARCHIVE = f"https://archive-api.open-meteo.com/v1/archive?latitude=2.0167&longitude=117.3000&start_date={start_date}&end_date={split_date_}&daily=temperature_2m_mean,precipitation_sum&timezone=Asia/Jakarta"
        API_PATH_FORECAST = f"https://api.open-meteo.com/v1/forecast?latitude=2.0167&longitude=117.3000&daily=temperature_2m_mean,precipitation_sum&timezone=Asia/Jakarta&past_days=0&start_date={split_date_}&end_date={end_date}"

        response_archive = requests.get(API_PATH_ARCHIVE)
        response_forecast = requests.get(API_PATH_FORECAST)

        if response_archive.status_code != 200:
            raise ConnectionError(f"Failed to fetch data from {API_PATH_ARCHIVE}")

        if response_forecast.status_code != 200:
            raise ConnectionError(f"Failed to fetch data from {API_PATH_FORECAST}")

        response_archive = response_archive.json()["daily"]
        response_forecast = response_forecast.json()["daily"]
        response = response_archive
        response["time"] += response_forecast["time"]
        response["temperature_2m_mean"] += response_forecast["temperature_2m_mean"]
        response["precipitation_sum"] += response_forecast["precipitation_sum"]

    elif start_date_ < split_date and end_date_ < split_date:
        API_PATH_ARCHIVE = f"https://archive-api.open-meteo.com/v1/archive?latitude=2.0167&longitude=117.3000&start_date={start_date}&end_date={end_date}&daily=temperature_2m_mean,precipitation_sum&timezone=Asia/Jakarta"

        response_archive = requests.get(API_PATH_ARCHIVE)

        if response_archive.status_code != 200:
            raise ConnectionError(f"Failed to fetch data from {API_PATH_ARCHIVE}")

        response = response_archive.json()["daily"]
    else:
        API_PATH_FORECAST = f"https://api.open-meteo.com/v1/forecast?latitude=2.0167&longitude=117.3000&daily=temperature_2m_mean,precipitation_sum&timezone=Asia/Jakarta&past_days=0&start_date={start_date}&end_date={end_date}"

        response_forecast = requests.get(API_PATH_FORECAST)

        if response_forecast.status_code != 200:
            raise ConnectionError(f"Failed to fetch data from {API_PATH_FORECAST}")

        response = response_forecast.json()["daily"]

    weather_data = pl.from_dict(response)

    weather_data = (
        weather_data.with_columns(
            pl.col("time").str.to_date().alias("date"),
        )
        .drop("time")
        .rename(
            {
                "temperature_2m_mean": "mean_temperature",
                "precipitation_sum": "total_precipitation",
            }
        )
    )

    return weather_data


def get_anomaly_data(sql_data: pl.DataFrame) -> pl.DataFrame:
    """
    Get anomaly data from SQL data by filtering out rows with negative tons extracted.

    Args:
        sql_data (pl.DataFrame): DataFrame containing the SQL data.

    Returns:
        pl.DataFrame: DataFrame containing the anomaly data.
    """
    if sql_data.is_empty():
        raise ValueError("SQL data is empty")

    if "tons_extracted" not in sql_data.columns:
        raise ValueError("SQL data must contain 'tons_extracted' column")

    anomaly_data = sql_data.filter(pl.col("tons_extracted") < 0).sort("date")
    return anomaly_data


def transform_sql_data(sql_data: pl.DataFrame) -> pl.DataFrame:
    """
    Transform SQL data by renaming columns and converting types.

    Args:
        df (pl.DataFrame): DataFrame containing the SQL data.

    Returns:
        pl.DataFrame: Transformed DataFrame.
    """
    if sql_data.is_empty():
        raise ValueError("SQL data is empty")
    if "tons_extracted" not in sql_data.columns:
        raise ValueError("SQL data must contain 'tons_extracted' column")
    if "quality_grade" not in sql_data.columns:
        raise ValueError("SQL data must contain 'quality_grade' column")
    if "date" not in sql_data.columns:
        raise ValueError("SQL data must contain 'date' column")

    daily_production = (
        sql_data.group_by("date")
        .agg(
            pl.when(pl.col("tons_extracted") >= 0)
            .then(pl.col("tons_extracted"))
            .otherwise(0)
            .sum()
            .alias("total_production_daily"),
            pl.col("quality_grade").mean().alias("average_quality_grade"),
        )
        .sort("date")
    )

    return daily_production


def transform_iot_data(iot_data: pl.DataFrame) -> pl.DataFrame:
    """
    Transform IoT data by renaming columns and converting types.

    Args:
        iot_data (pl.DataFrame): DataFrame containing the IoT data.

    Returns:
        pl.DataFrame: Transformed DataFrame.
    """
    if iot_data.is_empty():
        raise ValueError("IoT data is empty")

    total_equipment = iot_data["equipment_id"].n_unique()

    iot_data_daily = (
        iot_data.group_by(pl.col("timestamp").dt.date())
        .agg(
            ((pl.col("status") == "active").sum() / (24 * total_equipment)).alias(
                "equipment_utilization"
            ),
            pl.col("fuel_consumption").sum().alias("total_fuel_consumption"),
        )
        .sort("timestamp")
    ).rename({"timestamp": "date"})

    return iot_data_daily


def transform_weather_data(weather_data: pl.DataFrame) -> pl.DataFrame:
    """
    Transform weather data by renaming columns and converting types.

    Args:
        weather_data (pl.DataFrame): DataFrame containing the weather data.

    Returns:
        pl.DataFrame: Transformed DataFrame.
    """
    if weather_data.is_empty():
        raise ValueError("Weather data is empty")

    return weather_data.sort("date")


def main():
    """
    Main function to execute the ETL pipeline.
    """
    # Load data
    sql_data = load_sql_data(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        query="""
                SELECT 
                    date, 
                    mine_id, 
                    shift, 
                    tons_extracted, 
                    quality_grade
                FROM 
                    production_logs
              """,
    )

    MIN_DATE = sql_data["date"].min().strftime("%Y-%m-%d")
    MAX_DATE = sql_data["date"].max().strftime("%Y-%m-%d")

    iot_data = load_iot_data(IOT_DATA_PATH)
    weather_data = load_weather_data(MIN_DATE, MAX_DATE)
    anomaly_data = get_anomaly_data(sql_data)

    # Transform data
    transformed_sql_data = transform_sql_data(sql_data)
    transformed_iot_data = transform_iot_data(iot_data)
    transformed_weather_data = transform_weather_data(weather_data)

    # Join data
    daily_production_metrics = transformed_sql_data.join(
        transformed_iot_data, on="date", how="left"
    ).join(transformed_weather_data, on="date", how="left")

    daily_production_metrics = daily_production_metrics.with_columns(
        (pl.col("total_fuel_consumption") / pl.col("total_production_daily")).alias(
            "fuel_efficiency"
        ),
    )

    # Print results
    print(transformed_sql_data.head())
    print(transformed_iot_data.head())
    print(transformed_weather_data.head())
    print(anomaly_data.head())
    print(daily_production_metrics.head())


if __name__ == "__main__":
    main()
