# src/tasks/weather_fetch.py

import time
import requests
import pyodbc
import redis
import pandas as pd
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.logger import logger
from config.settings import (
    DB_SERVER, DB_NAME, USER_AGENT,
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_KEY
)
from tasks.extract_mssql import fetch_table_data
from tasks.geocode_enrichment import get_connection
 
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def fetch_date_range():
    df = fetch_table_data("blinkit_orders")   # using orders table
    if df is None or "order_date" not in df.columns:
        logger.error("No orders data or 'order_date' column missing")
        return None, None
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    min_date, max_date = df["order_date"].min().date(), df["order_date"].max().date()
    logger.info(f"Fetched date range: {min_date} -> {max_date}")
    return min_date, max_date


def fetch_geocodes():
    df = fetch_table_data("blinkit_area_geocode")
    if df is None or not {"area", "latitude", "longitude"}.issubset(df.columns):
        logger.error("No geocode data or required columns missing")
        return []
    geocodes = df.dropna(subset=["latitude", "longitude"])
    logger.info(f"Fetched {len(geocodes)} geocodes from DB")
    return geocodes.to_dict(orient="records")

def create_weather_table():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='blinkit_weather_data' AND xtype='U')
        CREATE TABLE blinkit_weather_data (
            area NVARCHAR(255),
            date DATE,
            temperature FLOAT,
            precipitation FLOAT,
            PRIMARY KEY (area, date)
        )
    """)
    conn.commit()
    conn.close()
    logger.info("Weather table ensured in database")

def split_date_range(start_date, end_date, chunk_size=365):
    ranges = []
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_size-1), end_date)
        ranges.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return ranges

def get_weather_url(start_date, end_date):
    today = date.today()
    if end_date < today:
        return "https://archive-api.open-meteo.com/v1/archive"
    elif start_date >= today:
        return "https://api.open-meteo.com/v1/forecast"
    else:
        # split internally in fetch_weather
        return None


def is_record_stored(area, date_str):
    """Check if record exists in DB or Redis"""
    cache_key = f"{area}:{date_str}"
    if r.sismember(f"{REDIS_KEY}_weather", cache_key):
        # Check DB too
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM blinkit_weather_data WHERE area=? AND date=?", area, date_str)
            exists = cursor.fetchone() is not None
            conn.close()
            return exists
        except Exception:
            # DB missing table, rely on Redis
            return True
    return False

def fetch_weather(area, lat, lon, start_date, end_date):
    all_records = []
    today = date.today()

    # Handle past & future split
    if start_date < today < end_date:
        past_records = fetch_weather(area, lat, lon, start_date, today - timedelta(days=1))
        future_records = fetch_weather(area, lat, lon, today, end_date)
        return past_records + future_records

    url = get_weather_url(start_date, end_date)
    if not url:
        return []

    headers = {"User-Agent": USER_AGENT}

    for s, e in split_date_range(start_date, end_date, 365):
        missing_dates = []
        # Pre-check Redis & DB for missing dates
        current = s
        while current <= e:
            if not is_record_stored(area, current.strftime("%Y-%m-%d")):
                missing_dates.append(current)
            current += timedelta(days=1)

        if not missing_dates:
            logger.info(f"All data exists for '{area}' {s}->{e}, skipped API call")
            continue

        # Hit API for missing range
        api_start, api_end = missing_dates[0], missing_dates[-1]
        try:
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": api_start.strftime("%Y-%m-%d"),
                "end_date": api_end.strftime("%Y-%m-%d"),
                "daily": ["temperature_2m_max", "precipitation_sum"],
                "timezone": "auto",
            }
            response = requests.get(url, params=params, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()
            if "daily" not in data:
                logger.warning(f"No daily weather found for {area} {api_start}->{api_end}")
                continue

            for date_str, temp, rain in zip(
                data["daily"]["time"],
                data["daily"]["temperature_2m_max"],
                data["daily"]["precipitation_sum"]
            ):
                cache_key = f"{area}:{date_str}"
                if not is_record_stored(area, date_str):
                    all_records.append((area, date_str, temp, rain))

            logger.info(f"Fetched weather for '{area}' {api_start}->{api_end} ({len(data['daily']['time'])} days)")
        except Exception as e:
            logger.error(f"Error fetching weather for '{area}' {api_start}->{api_end}: {e}")
        finally:
            time.sleep(1.1)

    return all_records


def store_weather(records):
    if not records:
        return
    conn = get_connection()
    cursor = conn.cursor()
    try:
        for area, date_str, temp, rain in records:
            cursor.execute("""
                UPDATE blinkit_weather_data
                SET temperature = ?, precipitation = ?
                WHERE area = ? AND date = ?
            """, temp, rain, area, date_str)
            if cursor.rowcount == 0:
                cursor.execute("""
                    INSERT INTO blinkit_weather_data (area, date, temperature, precipitation)
                    VALUES (?, ?, ?, ?)
                """, area, date_str, temp, rain)
            r.sadd(f"{REDIS_KEY}_weather", f"{area}:{date_str}")
        conn.commit()
        logger.info(f"Stored {len(records)} weather records")
    except Exception as e:
        logger.error(f"Failed to store weather records: {e}")
    finally:
        conn.close()

# ----------------- Weather Pipeline -----------------
MAX_THREADS = 3

def run_weather_pipeline():
    logger.info("Starting weather enrichment pipeline")
    create_weather_table()

    min_date, max_date = fetch_date_range()
    if not min_date or not max_date:
        logger.info("No valid dates found. Exiting.")
        return

    geocodes = fetch_geocodes()
    if not geocodes:
        logger.info("No geocodes found. Exiting.")
        return

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {
            executor.submit(fetch_weather, g["area"], g["latitude"], g["longitude"], min_date, max_date): g["area"]
            for g in geocodes
        }
        for future in as_completed(futures):
            records = future.result()
            if records:
                store_weather(records)

    logger.info("Weather enrichment pipeline completed")
