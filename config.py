import os
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", 120))  # Increased default from 60 to 120 seconds
DATABASE_PATH = os.getenv("DATABASE_PATH", "polymarket.db")
API_TIMEOUT = int(os.getenv("API_TIMEOUT", 10))
GUILD_ID = os.getenv("GUILD_ID")  # Optional: for instant command sync
POLYGONSCAN_API_KEY = os.getenv("POLYGONSCAN_API_KEY")  # Optional: for higher rate limits
MAX_NOTIFICATIONS_PER_CYCLE = int(os.getenv("MAX_NOTIFICATIONS_PER_CYCLE", 5))  # Configurable limit

if not DISCORD_TOKEN:
    raise ValueError("DISCORD_TOKEN is required. Set it in your .env file.")
