"""
Async database manager for the Polymarket Discord bot.
Handles all CRUD operations with SQLite using aiosqlite.
"""

import aiosqlite
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from config import DATABASE_PATH
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class User:
    id: int
    discord_id: int
    alert_thread_id: Optional[int]
    is_active: bool
    created_at: datetime


@dataclass
class Wallet:
    id: int
    address: str
    name: str
    created_at: datetime


@dataclass
class Position:
    market_id: str
    market_title: Optional[str]
    outcome: str
    quantity: float
    avg_price: Optional[float]


@dataclass
class PolygonWallet:
    id: int
    polygon_address: str
    polymarket_wallet_id: int
    name: str
    created_at: datetime


class DatabaseManager:
    """Async database manager for wallet tracking."""

    def __init__(self, db_path: str = DATABASE_PATH):
        self.db_path = db_path
        self._connection: Optional[aiosqlite.Connection] = None

    async def init(self) -> None:
        """Initialize the database connection."""
        self._connection = await aiosqlite.connect(self.db_path)
        self._connection.row_factory = aiosqlite.Row
        await self._connection.execute("PRAGMA foreign_keys = ON")
        logger.info(f"Database connected: {self.db_path}")

    async def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            await self._connection.close()
            logger.info("Database connection closed")

    @property
    def conn(self) -> aiosqlite.Connection:
        if not self._connection:
            raise RuntimeError("Database not initialized. Call init() first.")
        return self._connection

    # ==================== USERS ====================

    async def get_or_create_user(self, discord_id: int) -> User:
        """Get existing user or create a new one."""
        async with self.conn.execute(
            "SELECT * FROM users WHERE discord_id = ?", (discord_id,)
        ) as cursor:
            row = await cursor.fetchone()

        if row:
            return User(
                id=row["id"],
                discord_id=row["discord_id"],
                alert_thread_id=row["alert_thread_id"],
                is_active=bool(row["is_active"]),
                created_at=row["created_at"],
            )

        await self.conn.execute(
            "INSERT INTO users (discord_id) VALUES (?)", (discord_id,)
        )
        await self.conn.commit()

        return await self.get_or_create_user(discord_id)

    async def set_user_thread(self, discord_id: int, thread_id: int) -> None:
        """Set the alert thread ID for a user."""
        await self.get_or_create_user(discord_id)
        await self.conn.execute(
            "UPDATE users SET alert_thread_id = ? WHERE discord_id = ?",
            (thread_id, discord_id),
        )
        await self.conn.commit()
        logger.info(f"Set thread {thread_id} for user {discord_id}")

    async def get_user_thread(self, discord_id: int) -> Optional[int]:
        """Get the alert thread ID for a user."""
        async with self.conn.execute(
            "SELECT alert_thread_id FROM users WHERE discord_id = ?", (discord_id,)
        ) as cursor:
            row = await cursor.fetchone()

        return row["alert_thread_id"] if row else None

    async def set_user_inactive(self, discord_id: int) -> None:
        """Mark a user as inactive (e.g., thread deleted)."""
        await self.conn.execute(
            "UPDATE users SET is_active = 0 WHERE discord_id = ?", (discord_id,)
        )
        await self.conn.commit()

    # ==================== WALLETS ====================

    async def get_or_create_wallet(self, address: str, name: str) -> Wallet:
        """Get existing wallet or create a new one."""
        address = address.lower()

        async with self.conn.execute(
            "SELECT * FROM tracked_wallets WHERE wallet_address = ?", (address,)
        ) as cursor:
            row = await cursor.fetchone()

        if row:
            return Wallet(
                id=row["id"],
                address=row["wallet_address"],
                name=row["wallet_name"],
                created_at=row["created_at"],
            )

        await self.conn.execute(
            "INSERT INTO tracked_wallets (wallet_address, wallet_name) VALUES (?, ?)",
            (address, name),
        )
        await self.conn.commit()

        return await self.get_or_create_wallet(address, name)

    async def get_unique_wallets(self, prioritize_recent: bool = False) -> list[Wallet]:
        """Get all unique wallets that are being tracked by at least one active user."""
        if prioritize_recent:
            # Prioritize wallets with recent activity (last_tx_hash updated recently)
            query = """
                SELECT DISTINCT tw.* FROM tracked_wallets tw
                INNER JOIN user_tracking ut ON tw.id = ut.wallet_id
                INNER JOIN users u ON ut.user_id = u.id
                WHERE u.is_active = 1 AND u.alert_thread_id IS NOT NULL
                ORDER BY tw.last_tx_hash IS NOT NULL DESC,
                         tw.created_at DESC
            """
        else:
            query = """
                SELECT DISTINCT tw.* FROM tracked_wallets tw
                INNER JOIN user_tracking ut ON tw.id = ut.wallet_id
                INNER JOIN users u ON ut.user_id = u.id
                WHERE u.is_active = 1 AND u.alert_thread_id IS NOT NULL
            """

        async with self.conn.execute(query) as cursor:
            rows = await cursor.fetchall()

        return [
            Wallet(
                id=row["id"],
                address=row["wallet_address"],
                name=row["wallet_name"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    # ==================== TX HASH TRACKING ====================

    async def get_last_tx_hash(self, wallet_address: str) -> Optional[str]:
        """Get the last known transaction hash for a wallet."""
        wallet_address = wallet_address.lower()
        async with self.conn.execute(
            "SELECT last_tx_hash FROM tracked_wallets WHERE wallet_address = ?",
            (wallet_address,),
        ) as cursor:
            row = await cursor.fetchone()
        return row["last_tx_hash"] if row else None

    async def set_last_tx_hash(self, wallet_address: str, tx_hash: str) -> None:
        """Set the last known transaction hash for a wallet."""
        wallet_address = wallet_address.lower()
        await self.conn.execute(
            "UPDATE tracked_wallets SET last_tx_hash = ? WHERE wallet_address = ?",
            (tx_hash, wallet_address),
        )
        await self.conn.commit()
        logger.debug(f"Set last_tx_hash for {wallet_address[:8]}... to {tx_hash[:10]}...")

    # ==================== TRACKING ====================

    async def add_tracking(
        self, discord_id: int, wallet_address: str, wallet_name: str
    ) -> bool:
        """Add a wallet to a user's tracking list. Returns True if added, False if already exists."""
        wallet_address = wallet_address.lower()

        user = await self.get_or_create_user(discord_id)
        wallet = await self.get_or_create_wallet(wallet_address, wallet_name)

        try:
            await self.conn.execute(
                "INSERT INTO user_tracking (user_id, wallet_id) VALUES (?, ?)",
                (user.id, wallet.id),
            )
            await self.conn.commit()
            logger.info(f"User {discord_id} now tracking wallet {wallet_address}")
            return True
        except aiosqlite.IntegrityError:
            logger.info(f"User {discord_id} already tracking wallet {wallet_address}")
            return False

    async def remove_tracking(self, discord_id: int, wallet_address: str) -> bool:
        """Remove a wallet from a user's tracking list. Returns True if removed."""
        wallet_address = wallet_address.lower()

        query = """
            DELETE FROM user_tracking
            WHERE user_id = (SELECT id FROM users WHERE discord_id = ?)
            AND wallet_id = (SELECT id FROM tracked_wallets WHERE wallet_address = ?)
        """
        cursor = await self.conn.execute(query, (discord_id, wallet_address))
        await self.conn.commit()

        removed = cursor.rowcount > 0
        if removed:
            logger.info(f"User {discord_id} stopped tracking wallet {wallet_address}")

            await self._cleanup_orphan_wallets()

        return removed

    async def _cleanup_orphan_wallets(self) -> None:
        """Remove wallets that are no longer tracked by anyone."""
        await self.conn.execute("""
            DELETE FROM tracked_wallets
            WHERE id NOT IN (SELECT DISTINCT wallet_id FROM user_tracking)
        """)
        await self.conn.commit()

    async def get_user_wallets(self, discord_id: int) -> list[Wallet]:
        """Get all wallets tracked by a specific user."""
        query = """
            SELECT tw.* FROM tracked_wallets tw
            INNER JOIN user_tracking ut ON tw.id = ut.wallet_id
            INNER JOIN users u ON ut.user_id = u.id
            WHERE u.discord_id = ?
        """
        async with self.conn.execute(query, (discord_id,)) as cursor:
            rows = await cursor.fetchall()

        return [
            Wallet(
                id=row["id"],
                address=row["wallet_address"],
                name=row["wallet_name"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    async def get_wallet_subscribers(self, wallet_address: str) -> list[User]:
        """Get all active users who are tracking a specific wallet."""
        wallet_address = wallet_address.lower()

        query = """
            SELECT u.* FROM users u
            INNER JOIN user_tracking ut ON u.id = ut.user_id
            INNER JOIN tracked_wallets tw ON ut.wallet_id = tw.id
            WHERE tw.wallet_address = ? AND u.is_active = 1 AND u.alert_thread_id IS NOT NULL
        """
        async with self.conn.execute(query, (wallet_address,)) as cursor:
            rows = await cursor.fetchall()

        return [
            User(
                id=row["id"],
                discord_id=row["discord_id"],
                alert_thread_id=row["alert_thread_id"],
                is_active=bool(row["is_active"]),
                created_at=row["created_at"],
            )
            for row in rows
        ]

    async def get_batch_wallet_subscribers(self, wallet_addresses: list[str]) -> dict[str, list[User]]:
        """Get subscribers for multiple wallets in a single query.

        Returns a dict mapping wallet_address -> list of subscribers.
        More efficient than calling get_wallet_subscribers multiple times.
        """
        if not wallet_addresses:
            return {}

        addresses = [addr.lower() for addr in wallet_addresses]
        placeholders = ",".join("?" * len(addresses))

        query = f"""
            SELECT u.*, tw.wallet_address FROM users u
            INNER JOIN user_tracking ut ON u.id = ut.user_id
            INNER JOIN tracked_wallets tw ON ut.wallet_id = tw.id
            WHERE tw.wallet_address IN ({placeholders})
            AND u.is_active = 1 AND u.alert_thread_id IS NOT NULL
        """
        async with self.conn.execute(query, addresses) as cursor:
            rows = await cursor.fetchall()

        result: dict[str, list[User]] = {addr: [] for addr in addresses}
        for row in rows:
            user = User(
                id=row["id"],
                discord_id=row["discord_id"],
                alert_thread_id=row["alert_thread_id"],
                is_active=bool(row["is_active"]),
                created_at=row["created_at"],
            )
            wallet_addr = row["wallet_address"]
            if wallet_addr in result:
                result[wallet_addr].append(user)

        return result

    # ==================== POSITIONS ====================

    async def get_wallet_positions(self, wallet_address: str, max_age_minutes: int = 5) -> list[Position]:
        """Get cached positions for a wallet if not older than max_age_minutes.

        Default TTL reduced to 5 minutes for more responsive data.
        Cache is also invalidated when new trades are detected (see invalidate_position_cache).
        """
        wallet_address = wallet_address.lower()

        cutoff_time = datetime.utcnow() - timedelta(minutes=max_age_minutes)

        async with self.conn.execute(
            "SELECT * FROM wallet_positions WHERE wallet_address = ? AND last_updated > ?",
            (wallet_address, cutoff_time),
        ) as cursor:
            rows = await cursor.fetchall()

        return [
            Position(
                market_id=row["market_id"],
                market_title=row["market_title"],
                outcome=row["outcome"],
                quantity=row["quantity"],
                avg_price=row["avg_price"],
            )
            for row in rows
        ]

    async def invalidate_position_cache(self, wallet_address: str) -> None:
        """Invalidate cached positions for a wallet (called when new trades detected)."""
        wallet_address = wallet_address.lower()
        await self.conn.execute(
            "DELETE FROM wallet_positions WHERE wallet_address = ?",
            (wallet_address,),
        )
        await self.conn.commit()
        logger.debug(f"Invalidated position cache for {wallet_address[:8]}...")

    async def update_wallet_positions(
        self, wallet_address: str, positions: list[Position]
    ) -> None:
        """Update cached positions for a wallet (replace all)."""
        wallet_address = wallet_address.lower()

        await self.conn.execute(
            "DELETE FROM wallet_positions WHERE wallet_address = ?",
            (wallet_address,),
        )

        for pos in positions:
            await self.conn.execute(
                """
                INSERT INTO wallet_positions
                (wallet_address, market_id, market_title, outcome, quantity, avg_price, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    wallet_address,
                    pos.market_id,
                    pos.market_title,
                    pos.outcome,
                    pos.quantity,
                    pos.avg_price,
                ),
            )

        await self.conn.commit()
        logger.debug(f"Updated {len(positions)} positions for wallet {wallet_address}")

    # ==================== POLYGON WALLETS ====================

    async def link_polygon_wallet(
        self, polygon_address: str, polymarket_address: str, name: str
    ) -> bool:
        """
        Link a Polygon wallet to a Polymarket wallet for deposit/withdrawal tracking.
        Returns True if linked, False if already exists.
        """
        polygon_address = polygon_address.lower()
        polymarket_address = polymarket_address.lower()

        # Get the polymarket wallet ID
        async with self.conn.execute(
            "SELECT id FROM tracked_wallets WHERE wallet_address = ?",
            (polymarket_address,),
        ) as cursor:
            row = await cursor.fetchone()

        if not row:
            logger.warning(f"Polymarket wallet {polymarket_address} not found")
            return False

        polymarket_wallet_id = row["id"]

        try:
            await self.conn.execute(
                """
                INSERT INTO polygon_wallets (polygon_address, polymarket_wallet_id, wallet_name)
                VALUES (?, ?, ?)
                """,
                (polygon_address, polymarket_wallet_id, name),
            )
            await self.conn.commit()
            logger.info(f"Linked polygon wallet {polygon_address} to polymarket {polymarket_address}")
            return True
        except aiosqlite.IntegrityError:
            logger.info(f"Polygon wallet {polygon_address} already linked")
            return False

    async def unlink_polygon_wallet(self, polygon_address: str) -> bool:
        """Remove a linked polygon wallet. Returns True if removed."""
        polygon_address = polygon_address.lower()

        cursor = await self.conn.execute(
            "DELETE FROM polygon_wallets WHERE polygon_address = ?",
            (polygon_address,),
        )
        await self.conn.commit()

        removed = cursor.rowcount > 0
        if removed:
            logger.info(f"Unlinked polygon wallet {polygon_address}")

        return removed

    async def get_unique_polygon_wallets(self) -> list[PolygonWallet]:
        """Get all unique polygon wallets that are linked to tracked polymarket wallets."""
        query = """
            SELECT DISTINCT pw.* FROM polygon_wallets pw
            INNER JOIN tracked_wallets tw ON pw.polymarket_wallet_id = tw.id
            INNER JOIN user_tracking ut ON tw.id = ut.wallet_id
            INNER JOIN users u ON ut.user_id = u.id
            WHERE u.is_active = 1 AND u.alert_thread_id IS NOT NULL
        """
        async with self.conn.execute(query) as cursor:
            rows = await cursor.fetchall()

        return [
            PolygonWallet(
                id=row["id"],
                polygon_address=row["polygon_address"],
                polymarket_wallet_id=row["polymarket_wallet_id"],
                name=row["wallet_name"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    async def get_polygon_wallet_subscribers(self, polygon_address: str) -> list[User]:
        """Get all active users who are tracking the polymarket wallet linked to this polygon wallet."""
        polygon_address = polygon_address.lower()

        query = """
            SELECT u.* FROM users u
            INNER JOIN user_tracking ut ON u.id = ut.user_id
            INNER JOIN tracked_wallets tw ON ut.wallet_id = tw.id
            INNER JOIN polygon_wallets pw ON pw.polymarket_wallet_id = tw.id
            WHERE pw.polygon_address = ? AND u.is_active = 1 AND u.alert_thread_id IS NOT NULL
        """
        async with self.conn.execute(query, (polygon_address,)) as cursor:
            rows = await cursor.fetchall()

        return [
            User(
                id=row["id"],
                discord_id=row["discord_id"],
                alert_thread_id=row["alert_thread_id"],
                is_active=bool(row["is_active"]),
                created_at=row["created_at"],
            )
            for row in rows
        ]

    async def get_last_polygon_tx_hash(self, polygon_address: str) -> Optional[str]:
        """Get the last known transaction hash for a polygon wallet."""
        polygon_address = polygon_address.lower()
        async with self.conn.execute(
            "SELECT last_tx_hash FROM polygon_wallets WHERE polygon_address = ?",
            (polygon_address,),
        ) as cursor:
            row = await cursor.fetchone()
        return row["last_tx_hash"] if row else None

    async def set_last_polygon_tx_hash(self, polygon_address: str, tx_hash: str) -> None:
        """Set the last known transaction hash for a polygon wallet."""
        polygon_address = polygon_address.lower()
        await self.conn.execute(
            "UPDATE polygon_wallets SET last_tx_hash = ? WHERE polygon_address = ?",
            (tx_hash, polygon_address),
        )
        await self.conn.commit()
        logger.debug(f"Set last_tx_hash for polygon {polygon_address[:8]}... to {tx_hash[:10]}...")

    async def get_linked_polygon_wallets(self, polymarket_address: str) -> list[PolygonWallet]:
        """Get all polygon wallets linked to a specific polymarket wallet."""
        polymarket_address = polymarket_address.lower()

        query = """
            SELECT pw.* FROM polygon_wallets pw
            INNER JOIN tracked_wallets tw ON pw.polymarket_wallet_id = tw.id
            WHERE tw.wallet_address = ?
        """
        async with self.conn.execute(query, (polymarket_address,)) as cursor:
            rows = await cursor.fetchall()

        return [
            PolygonWallet(
                id=row["id"],
                polygon_address=row["polygon_address"],
                polymarket_wallet_id=row["polymarket_wallet_id"],
                name=row["wallet_name"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    # ==================== CRYPTO TRACKING (WebSocket) ====================

    async def add_crypto_tracking(
        self, discord_id: int, wallet_address: str, wallet_name: str
    ) -> bool:
        """Add a wallet for crypto 15min market tracking. Returns True if added."""
        wallet_address = wallet_address.lower()
        user = await self.get_or_create_user(discord_id)

        try:
            await self.conn.execute(
                """
                INSERT INTO crypto_tracking (user_id, wallet_address, wallet_name)
                VALUES (?, ?, ?)
                """,
                (user.id, wallet_address, wallet_name),
            )
            await self.conn.commit()
            logger.info(f"User {discord_id} now crypto-tracking wallet {wallet_address}")
            return True
        except aiosqlite.IntegrityError:
            logger.info(f"User {discord_id} already crypto-tracking wallet {wallet_address}")
            return False

    async def remove_crypto_tracking(self, discord_id: int, wallet_address: str) -> bool:
        """Remove a wallet from crypto tracking. Returns True if removed."""
        wallet_address = wallet_address.lower()

        query = """
            DELETE FROM crypto_tracking
            WHERE user_id = (SELECT id FROM users WHERE discord_id = ?)
            AND wallet_address = ?
        """
        cursor = await self.conn.execute(query, (discord_id, wallet_address))
        await self.conn.commit()

        removed = cursor.rowcount > 0
        if removed:
            logger.info(f"User {discord_id} stopped crypto-tracking wallet {wallet_address}")

        return removed

    async def get_user_crypto_wallets(self, discord_id: int) -> list[tuple[str, str]]:
        """Get all wallets being crypto-tracked by a user. Returns list of (address, name)."""
        query = """
            SELECT ct.wallet_address, ct.wallet_name FROM crypto_tracking ct
            INNER JOIN users u ON ct.user_id = u.id
            WHERE u.discord_id = ?
        """
        async with self.conn.execute(query, (discord_id,)) as cursor:
            rows = await cursor.fetchall()

        return [(row["wallet_address"], row["wallet_name"]) for row in rows]

    async def get_crypto_tracked_wallets(self) -> list[str]:
        """Get all unique wallet addresses being crypto-tracked by active users."""
        query = """
            SELECT DISTINCT ct.wallet_address FROM crypto_tracking ct
            INNER JOIN users u ON ct.user_id = u.id
            WHERE u.is_active = 1 AND u.alert_thread_id IS NOT NULL
        """
        async with self.conn.execute(query) as cursor:
            rows = await cursor.fetchall()

        return [row["wallet_address"] for row in rows]

    async def get_crypto_subscribers(self, wallet_address: str) -> list[User]:
        """Get all active users who are crypto-tracking a specific wallet."""
        wallet_address = wallet_address.lower()

        query = """
            SELECT u.* FROM users u
            INNER JOIN crypto_tracking ct ON u.id = ct.user_id
            WHERE ct.wallet_address = ? AND u.is_active = 1 AND u.alert_thread_id IS NOT NULL
        """
        async with self.conn.execute(query, (wallet_address,)) as cursor:
            rows = await cursor.fetchall()

        return [
            User(
                id=row["id"],
                discord_id=row["discord_id"],
                alert_thread_id=row["alert_thread_id"],
                is_active=bool(row["is_active"]),
                created_at=row["created_at"],
            )
            for row in rows
        ]
