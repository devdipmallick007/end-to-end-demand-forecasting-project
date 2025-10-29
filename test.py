import pyodbc
import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import redis

# ----------------- Database connection -----------------
DB_SERVER = os.getenv("DB_SERVER", "localhost\\Devdip")  # fix escape
DB_NAME = os.getenv("DB_NAME", "blinkit_database")

def get_connection():
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_SERVER};DATABASE={DB_NAME};Trusted_Connection=yes"
    return pyodbc.connect(conn_str)

# ----------------- Redis cache -----------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_KEY = "blinkit_areas"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def get_new_areas(cleaned_areas):
    """Return areas that are not yet in Redis cache"""
    new_areas = [area for area in cleaned_areas if not r.sismember(REDIS_KEY, area)]
    return new_areas

def update_area_cache(new_areas):
    """Add new areas to Redis"""
    for area in new_areas:
        r.sadd(REDIS_KEY, area)

# ----------------- Fetch distinct areas from DB -----------------
def fetch_distinct_areas():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT area FROM blinkit_customers WHERE area IS NOT NULL")
    areas = [row[0].strip().title() for row in cursor.fetchall()]
    conn.close()
    return areas

# ----------------- Create geocode table -----------------
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

# ----------------- Geocode function -----------------
GEOCODE_URL = "https://nominatim.openstreetmap.org/search"

def geocode_area(area):
    try:
        headers = {
            "User-Agent": "DevdipDemandForecasting/1.0 (devdipmallick22@gmail.com)",
            "Accept-Language": "en"
        }
        response = requests.get(
            GEOCODE_URL,
            params={"q": area, "format": "json", "limit": 1},
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        if data:
            lat, lon = float(data[0]["lat"]), float(data[0]["lon"])
            print(f"Geocoded '{area}' -> lat: {lat}, lon: {lon}")
            return area, lat, lon
        else:
            print(f"No geocode found for area '{area}'")
            return area, None, None
    except Exception as e:
        print(f"Error geocoding area '{area}': {e}")
        return area, None, None
    finally:
        time.sleep(1.1)  # respect Nominatim rate limit

# ----------------- Store geocode in database -----------------
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
        print(f"Stored '{area}' -> lat: {lat}, lon: {lon}")
    except Exception as e:
        print(f"Failed to store geocode for '{area}': {e}")
    finally:
        conn.close()

# ----------------- Parallel geocoding -----------------
MAX_THREADS = 3  # safe parallelism for Nominatim

def main():
    create_geocode_table()
    areas = fetch_distinct_areas()
    print(f"Found {len(areas)} distinct areas")

    # Check Redis cache
    new_areas = get_new_areas(areas)
    print(f"{len(new_areas)} new areas found (not in cache)")

    if not new_areas:
        print("No new areas to geocode. Exiting.")
        return

    # Update Redis cache
    update_area_cache(new_areas)

    # Parallel geocoding
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_area = {executor.submit(geocode_area, area): area for area in new_areas}
        for future in as_completed(future_to_area):
            area, lat, lon = future.result()
            if lat is not None and lon is not None:
                store_geocode(area, lat, lon)
            else:
                print(f"Skipped storing '{area}' due to missing geocode")

if __name__ == "__main__":
    main()
