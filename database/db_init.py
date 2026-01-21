"""
Database initialization script.
Run with: python -m database.db_init
"""

import sqlite3
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DATABASE_PATH

SCHEMA = """
-- Table des utilisateurs Discord
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    discord_id INTEGER UNIQUE NOT NULL,
    alert_thread_id INTEGER,
    is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table des wallets (chaque wallet unique une seule fois)
CREATE TABLE IF NOT EXISTS tracked_wallets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address TEXT UNIQUE NOT NULL,
    wallet_name TEXT NOT NULL,
    last_tx_hash TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table de liaison N:N (utilisateurs <-> wallets)
CREATE TABLE IF NOT EXISTS user_tracking (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    wallet_id INTEGER NOT NULL,
    tracked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (wallet_id) REFERENCES tracked_wallets(id) ON DELETE CASCADE,
    UNIQUE(user_id, wallet_id)
);

-- Cache des positions (pour détecter les changements)
CREATE TABLE IF NOT EXISTS wallet_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_address TEXT NOT NULL,
    market_id TEXT NOT NULL,
    market_title TEXT,
    outcome TEXT NOT NULL,
    quantity REAL NOT NULL,
    avg_price REAL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(wallet_address, market_id, outcome)
);

-- Table des wallets Polygon lies aux wallets Polymarket (pour tracking depots/retraits)
CREATE TABLE IF NOT EXISTS polygon_wallets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    polygon_address TEXT UNIQUE NOT NULL,
    polymarket_wallet_id INTEGER NOT NULL,
    wallet_name TEXT NOT NULL,
    last_tx_hash TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (polymarket_wallet_id) REFERENCES tracked_wallets(id) ON DELETE CASCADE
);

-- Table pour le tracking crypto temps reel (BTC, ETH, XRP, SOL 15min)
CREATE TABLE IF NOT EXISTS crypto_tracking (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    wallet_address TEXT NOT NULL,
    wallet_name TEXT NOT NULL,
    last_tx_hash TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE(user_id, wallet_address)
);

-- Index pour optimisation
CREATE INDEX IF NOT EXISTS idx_user_tracking_wallet ON user_tracking(wallet_id);
CREATE INDEX IF NOT EXISTS idx_user_tracking_user ON user_tracking(user_id);
CREATE INDEX IF NOT EXISTS idx_positions_wallet ON wallet_positions(wallet_address);
CREATE INDEX IF NOT EXISTS idx_users_discord_id ON users(discord_id);
CREATE INDEX IF NOT EXISTS idx_wallets_address ON tracked_wallets(wallet_address);
CREATE INDEX IF NOT EXISTS idx_polygon_wallets_address ON polygon_wallets(polygon_address);
CREATE INDEX IF NOT EXISTS idx_polygon_wallets_polymarket ON polygon_wallets(polymarket_wallet_id);
CREATE INDEX IF NOT EXISTS idx_crypto_tracking_user ON crypto_tracking(user_id);
CREATE INDEX IF NOT EXISTS idx_crypto_tracking_wallet ON crypto_tracking(wallet_address);
"""


def init_database(db_path: str = DATABASE_PATH) -> None:
    """Initialize the database with the schema."""
    print(f"Initializing database at: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()

    print("Database initialized successfully!")
    print("Tables created:")
    print("  - users")
    print("  - tracked_wallets")
    print("  - user_tracking")
    print("  - wallet_positions")
    print("  - polygon_wallets")
    print("  - crypto_tracking")


def reset_database(db_path: str = DATABASE_PATH) -> None:
    """Drop all tables and recreate them."""
    print(f"Resetting database at: {db_path}")

    if os.path.exists(db_path):
        os.remove(db_path)
        print("Old database removed.")

    init_database(db_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Initialize the Polymarket bot database")
    parser.add_argument("--reset", action="store_true", help="Reset the database (drops all data)")
    args = parser.parse_args()

    if args.reset:
        confirm = input("This will DELETE all data. Are you sure? (yes/no): ")
        if confirm.lower() == "yes":
            reset_database()
        else:
            print("Aborted.")
    else:
        init_database()
