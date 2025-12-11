# src/tasks/geocode_enrichment.py

import time
import requests
import pyodbc
import redis
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.logger import logger
from config.settings import DB_SERVER, DB_NAME, USER_AGENT, GEOCODE_URL, REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_KEY
from tasks.extract_mssql import fetch_table_data


def get_connection():
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_SERVER};DATABASE={DB_NAME};Trusted_Connection=yes"
    return pyodbc.connect(conn_str)


r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def get_new_areas(areas):
    """Return areas not yet cached in Redis"""
    return [area for area in areas if not r.sismember(REDIS_KEY, area)]

def update_area_cache(areas):
    """Add areas to Redis cache"""
    for area in areas:
        r.sadd(REDIS_KEY, area)


def fetch_distinct_areas():
    df = fetch_table_data("blinkit_customers")
    if df is None or "area" not in df.columns:
        logger.error("No customers data or 'area' column missing")
        return []
    areas = df["area"].dropna().map(lambda x: x.strip().title()).unique().tolist()
    return areas

def create_geocode_table():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='blinkit_area_geocode' AND xtype='U')
        CREATE TABLE blinkit_area_geocode (
            area NVARCHAR(255) PRIMARY KEY,
            latitude FLOAT,
            longitude FLOAT
        )
    """)
    conn.commit()
    conn.close()
    logger.info("Geocode table ensured in database")


def geocode_area(area):
    try:
        headers = {"User-Agent": USER_AGENT, "Accept-Language": "en"}
        response = requests.get(
            GEOCODE_URL,
            params={"q": area, "format": "json", "limit": 1},
            headers=headers,
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        if data:
            lat, lon = float(data[0]["lat"]), float(data[0]["lon"])
            logger.info(f"Geocoded '{area}' -> lat: {lat}, lon: {lon}")
            return area, lat, lon
        else:
            logger.warning(f"No geocode found for '{area}'")
            return area, None, None
    except Exception as e:
        logger.error(f"Error geocoding '{area}': {e}")
        return area, None, None
    finally:
        time.sleep(1.1)  # respect API rate limit

def store_geocode(area, lat, lon):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE blinkit_area_geocode
            SET latitude = ?, longitude = ?
            WHERE area = ?
        """, lat, lon, area)
        if cursor.rowcount == 0:
            cursor.execute("""
                INSERT INTO blinkit_area_geocode (area, latitude, longitude)
                VALUES (?, ?, ?)
            """, area, lat, lon)
        conn.commit()
        logger.info(f"Stored '{area}' -> lat: {lat}, lon: {lon}")
        # Add to Redis cache after successful store
        update_area_cache([area])
    except Exception as e:
        logger.error(f"Failed to store geocode for '{area}': {e}")
    finally:
        conn.close()


MAX_THREADS = 3

def run_geocode_pipeline():
    logger.info("Starting geocode enrichment pipeline")
    create_geocode_table()

    areas = fetch_distinct_areas()
    if not areas:
        logger.info("No areas to geocode. Exiting.")
        return

    # Filter areas using Redis cache
    new_areas = get_new_areas(areas)
    logger.info(f"{len(new_areas)} new areas to geocode (not in Redis cache)")

    if not new_areas:
        logger.info("All areas already geocoded. Exiting.")
        return

    # Parallel geocoding
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_area = {executor.submit(geocode_area, area): area for area in new_areas}
        for future in as_completed(future_to_area):
            area, lat, lon = future.result()
            if lat is not None and lon is not None:
                store_geocode(area, lat, lon)
            else:
                logger.warning(f"Skipped storing '{area}' due to missing geocode")

    logger.info("Geocode enrichment pipeline completed")
