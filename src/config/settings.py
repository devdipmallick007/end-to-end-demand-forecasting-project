# config/settings.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Logging
LOG_PATH = os.getenv("LOG_PATH", "logs")

# Database
DB_DRIVER = os.getenv("DB_DRIVER")
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")

# Redis
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_KEY = os.getenv("REDIS_KEY")

# Geocode API
GEOCODE_URL = os.getenv("GEOCODE_URL")
USER_AGENT = os.getenv("USER_AGENT")

# Weather API
WEATHER_URL = os.getenv("WEATHER_API_URL", "https://api.open-meteo.com/v1/forecast")
USER_AGENT = os.getenv("USER_AGENT", "DevdipDemandForecasting/1.0 (devdipmallick22@gmail.com)")

