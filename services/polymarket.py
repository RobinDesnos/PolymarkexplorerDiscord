"""
Polymarket API service and monitoring engine.
Tracks transactions (trades) and provides position data.
"""

import aiohttp
import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, TYPE_CHECKING

import discord

from config import API_TIMEOUT, MAX_NOTIFICATIONS_PER_CYCLE
from database.manager import Position

if TYPE_CHECKING:
    from discord.ext.commands import Bot
    from database.manager import DatabaseManager, Wallet, User

logger = logging.getLogger(__name__)


@dataclass
class Trade:
    """Represents a trade/transaction."""
    tx_hash: str
    timestamp: int
    side: str  # "BUY" or "SELL"
    outcome: str  # "Yes", "No", "Up", "Down", etc.
    price: float
    size: float  # Number of shares
    usdc_size: float  # USDC amount
    market_title: str
    market_slug: str
    event_slug: str
    condition_id: str
    icon_url: Optional[str] = None


class PolymarketService:
    """Client for the Polymarket API."""

    BASE_URL = "https://data-api.polymarket.com"
    GAMMA_URL = "https://gamma-api.polymarket.com"  # For positions API

    def __init__(self, timeout: int = API_TIMEOUT):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None
        # Performance metrics
        self.api_calls = 0
        self.total_response_time = 0.0
        self.cache_hits = 0
        self.cache_misses = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _fetch_json(self, endpoint: str, params: dict, max_retries: int = 3, use_gamma: bool = False) -> list:
        """Generic fetch with retry logic and exponential backoff."""
        base = self.GAMMA_URL if use_gamma else self.BASE_URL
        url = f"{base}/{endpoint}"
        start_time = time.time()

        for attempt in range(max_retries):
            try:
                session = await self._get_session()
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        # Track metrics
                        self.api_calls += 1
                        self.total_response_time += (time.time() - start_time)
                        return data
                    elif response.status == 429:
                        # Rate limited - wait longer
                        wait_time = min(60 * (2 ** attempt), 300)  # Cap at 5 minutes
                        logger.warning(f"Rate limited. Waiting {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    elif response.status >= 500:
                        logger.error(f"Server error {response.status}")
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt
                            logger.info(f"Retrying in {wait_time}s...")
                            await asyncio.sleep(wait_time)
                            continue
                        return []
                    else:
                        logger.error(f"Unexpected status {response.status} for {url}")
                        try:
                            error_data = await response.json()
                            logger.error(f"Response body: {error_data}")
                        except:
                            logger.error("Could not parse error response")
                        return []

            except asyncio.TimeoutError:
                logger.warning(f"Timeout (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue

            except aiohttp.ClientError as e:
                logger.error(f"Network error: {e}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue

            except Exception as e:
                logger.exception(f"Unexpected error: {e}")
                return []

        return []

    async def get_recent_trades(self, wallet_address: str, limit: int = 50) -> list[Trade]:
        """Fetch recent trades/activity for a wallet."""
        wallet_address = wallet_address.lower()
        # Use data-api.polymarket.com/activity endpoint
        data = await self._fetch_json("activity", {"user": wallet_address, "limit": limit})
        return self._parse_trades(data)

    async def get_positions(self, wallet_address: str, db: Optional["DatabaseManager"] = None) -> list[Position]:
        """Fetch current positions for a wallet. Uses cache if available and db provided."""
        wallet_address = wallet_address.lower()

        # Check cache first if db is provided
        if db:
            cached_positions = await db.get_wallet_positions(wallet_address)
            if cached_positions:
                self.cache_hits += 1
                logger.debug(f"Using cached positions for {wallet_address[:8]}...")
                return cached_positions
            else:
                self.cache_misses += 1

        # Fetch from API (positions still uses gamma API)
        data = await self._fetch_json("positions", {"user": wallet_address}, use_gamma=True)
        positions = self._parse_positions(data)

        # Cache the results if db is provided
        if db and positions:
            await db.update_wallet_positions(wallet_address, positions)

        return positions

    def get_performance_stats(self) -> dict:
        """Get performance statistics."""
        avg_response_time = self.total_response_time / self.api_calls if self.api_calls > 0 else 0
        cache_hit_rate = self.cache_hits / (self.cache_hits + self.cache_misses) if (self.cache_hits + self.cache_misses) > 0 else 0

        return {
            "api_calls": self.api_calls,
            "avg_response_time": round(avg_response_time, 2),
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_rate": round(cache_hit_rate * 100, 1),
        }

    async def get_market_price(self, condition_id: str, outcome: str) -> Optional[float]:
        """Get current market price for an outcome."""
        try:
            data = await self._fetch_json("market", {"condition_id": condition_id}, use_gamma=True)
            if data and isinstance(data, list) and len(data) > 0:
                market = data[0]
                tokens = market.get("tokens", [])
                for token in tokens:
                    if token.get("outcome", "").lower() == outcome.lower():
                        return float(token.get("price", 0))
        except Exception as e:
            logger.warning(f"Could not fetch market price: {e}")
        return None

    def _parse_trades(self, data: list) -> list[Trade]:
        """Parse API response into Trade objects."""
        trades = []
        for item in data:
            try:
                if item.get("type") not in (None, "TRADE"):
                    continue

                trades.append(Trade(
                    tx_hash=item.get("transactionHash", ""),
                    timestamp=int(item.get("timestamp", 0)),
                    side=item.get("side", ""),
                    outcome=item.get("outcome", ""),
                    price=float(item.get("price", 0)),
                    size=float(item.get("size", 0)),
                    usdc_size=float(item.get("usdcSize", 0)),
                    market_title=item.get("title", ""),
                    market_slug=item.get("slug", ""),
                    event_slug=item.get("eventSlug", ""),
                    condition_id=item.get("conditionId", ""),
                    icon_url=item.get("icon"),
                ))
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Error parsing trade: {e}")
                continue
        return trades

    def _parse_positions(self, data: list) -> list[Position]:
        """Parse API response into Position objects. Filters out expired/closed markets."""
        positions = []
        for item in data:
            try:
                size = float(item.get("size", 0) or 0)
                cur_price = float(item.get("curPrice", 0) or 0)
                redeemable = item.get("redeemable", False)

                # Skip if no position or market is closed/expired
                if size <= 0:
                    continue
                if redeemable:  # Market ended, can redeem winnings
                    continue
                if cur_price == 0:  # Market closed (price is 0)
                    continue

                positions.append(Position(
                    market_id=item.get("conditionId", ""),
                    market_title=item.get("title", ""),
                    outcome=item.get("outcome", ""),
                    quantity=size,
                    avg_price=float(item.get("avgPrice", 0) or 0),
                ))
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Error parsing position: {e}")
                continue
        return positions


class MonitoringEngine:
    """
    Monitoring engine that detects NEW transactions.
    Tracks last seen tx_hash and timestamp per wallet to avoid duplicate notifications.
    """

    def __init__(
        self,
        db: "DatabaseManager",
        api: PolymarketService,
        bot: "Bot",
    ):
        self.db = db
        self.api = api
        self.bot = bot
        # Cache last seen timestamps to avoid old notifications
        self._last_timestamps: dict[str, int] = {}
        self.max_notifications = MAX_NOTIFICATIONS_PER_CYCLE
        # Cache subscribers per cycle to reduce DB queries
        self._subscribers_cache: dict[str, list["User"]] = {}

    async def run_monitoring_cycle(self) -> None:
        """Run a single monitoring cycle for all tracked wallets."""
        try:
            # Get wallets prioritized by recent activity
            unique_wallets = await self.db.get_unique_wallets(prioritize_recent=True)

            if not unique_wallets:
                logger.debug("No wallets to monitor")
                return

            logger.info(f"Monitoring {len(unique_wallets)} unique wallet(s)...")

            # Pre-fetch all subscribers in batch (reduces N+1 queries)
            wallet_addresses = [w.address for w in unique_wallets]
            self._subscribers_cache = await self.db.get_batch_wallet_subscribers(wallet_addresses)

            # Process wallets in parallel batches to respect API rate limits
            batch_size = 5  # Process 5 wallets concurrently
            for i in range(0, len(unique_wallets), batch_size):
                batch = unique_wallets[i:i + batch_size]
                # Process batch in parallel
                await asyncio.gather(
                    *[self._process_wallet(wallet) for wallet in batch],
                    return_exceptions=True
                )
                # Small delay between batches to respect rate limits
                if i + batch_size < len(unique_wallets):
                    await asyncio.sleep(1)

            # Clear cache after cycle
            self._subscribers_cache.clear()

        except Exception as e:
            logger.exception(f"Error in monitoring cycle: {e}")

    async def _process_wallet(self, wallet: "Wallet") -> None:
        """Process a single wallet: fetch trades, detect new ones, notify."""
        try:
            recent_trades = await self.api.get_recent_trades(wallet.address, limit=10)

            if not recent_trades:
                logger.debug(f"No trades found for {wallet.name}")
                return

            last_tx_hash = await self.db.get_last_tx_hash(wallet.address)

            # If no last_tx_hash, initialize and store timestamp
            if not last_tx_hash:
                logger.info(f"Initializing tracking for {wallet.name}")
                await self.db.set_last_tx_hash(wallet.address, recent_trades[0].tx_hash)
                self._last_timestamps[wallet.address] = recent_trades[0].timestamp
                return

            # Get cached timestamp or use a recent default (2 minutes ago)
            import time
            last_timestamp = self._last_timestamps.get(wallet.address, int(time.time()) - 120)

            new_trades = self._get_new_trades(recent_trades, last_tx_hash, last_timestamp)

            if new_trades:
                # Limit notifications per cycle
                trades_to_notify = new_trades[:self.max_notifications]

                if len(new_trades) > self.max_notifications:
                    logger.warning(f"Limiting {len(new_trades)} trades to {self.max_notifications} for {wallet.name}")

                logger.info(f"{len(trades_to_notify)} new trade(s) for {wallet.name}")

                # Invalidate position cache since trades detected (positions likely changed)
                await self.db.invalidate_position_cache(wallet.address)

                # Use cached subscribers (pre-fetched in batch at cycle start)
                subscribers = self._subscribers_cache.get(wallet.address.lower(), [])

                for trade in reversed(trades_to_notify):
                    await self._notify_subscribers(wallet, trade, subscribers)

                # Update last seen
                await self.db.set_last_tx_hash(wallet.address, recent_trades[0].tx_hash)
                self._last_timestamps[wallet.address] = recent_trades[0].timestamp

        except Exception as e:
            logger.error(f"Error processing wallet {wallet.address}: {e}")

    def _get_new_trades(self, trades: list[Trade], last_tx_hash: str, last_timestamp: int) -> list[Trade]:
        """Get trades that are newer than last_tx_hash or last_timestamp."""
        new_trades = []

        for trade in trades:
            # Stop if we found the last known transaction
            if trade.tx_hash == last_tx_hash:
                break
            # Also stop if transaction is older than last timestamp (safety check)
            if trade.timestamp <= last_timestamp:
                break
            new_trades.append(trade)

        return new_trades

    async def _notify_subscribers(
        self, wallet: "Wallet", trade: Trade, subscribers: list["User"]
    ) -> None:
        """Send notification embed to all subscribers' private threads."""
        embed = self._build_trade_embed(wallet, trade)

        for user in subscribers:
            if not user.alert_thread_id:
                continue

            try:
                thread = self.bot.get_channel(user.alert_thread_id)
                if thread is None:
                    thread = await self.bot.fetch_channel(user.alert_thread_id)

                if thread:
                    await thread.send(embed=embed)
                    logger.debug(f"Notified user {user.discord_id}")
                else:
                    await self.db.set_user_inactive(user.discord_id)

            except discord.NotFound:
                logger.warning(f"Thread deleted for user {user.discord_id}")
                await self.db.set_user_inactive(user.discord_id)
            except discord.Forbidden:
                logger.warning(f"Permission denied for thread {user.alert_thread_id}")
            except Exception as e:
                logger.error(f"Error notifying user {user.discord_id}: {e}")

    def _build_trade_embed(
        self, wallet: "Wallet", trade: Trade
    ) -> discord.Embed:
        """Build a Discord embed for a trade notification."""
        is_buy = trade.side == "BUY"
        color = discord.Color.green() if is_buy else discord.Color.red()
        side_emoji = "BUY" if is_buy else "SELL"
        side_icon = "🟢" if is_buy else "🔴"

        short_addr = f"{wallet.address[:6]}...{wallet.address[-4:]}"
        short_tx = f"{trade.tx_hash[:6]}...{trade.tx_hash[-4:]}"

        polygonscan_wallet = f"https://polygonscan.com/address/{wallet.address}"
        polygonscan_tx = f"https://polygonscan.com/tx/{trade.tx_hash}"
        polymarket_event = f"https://polymarket.com/event/{trade.event_slug}"

        trade_time = datetime.fromtimestamp(trade.timestamp).strftime("%H:%M:%S")

        price_percent = int(trade.price * 100)
        progress_bar = self._build_progress_bar(price_percent)

        embed = discord.Embed(color=color)

        embed.description = (
            f"👤 [`{short_addr}`]({polygonscan_wallet}) ({wallet.name})\n\n"
            f"{side_icon} **{side_emoji}** - {trade.outcome} @ ${trade.price:.2f}\n\n"
            f"📌 [{trade.market_title}]({polymarket_event})\n\n"
            f"📊 {price_percent}% {progress_bar}\n\n"
            f"💰 {trade.usdc_size:.2f} USDC • 🕒 {trade_time} • 🔗 [`{short_tx}`]({polygonscan_tx})"
        )

        if trade.icon_url:
            embed.set_thumbnail(url=trade.icon_url)

        return embed

    def _build_progress_bar(self, percent: int) -> str:
        """Build a visual progress bar."""
        filled = min(10, max(0, percent // 10))
        empty = 10 - filled
        return "🟩" * filled + "⬜" * empty
