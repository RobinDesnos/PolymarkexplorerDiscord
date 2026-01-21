"""
WebSocket service for real-time crypto market monitoring on Polymarket.
Monitors BTC, ETH, XRP, SOL 15-minute Up/Down markets.
"""

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Callable, TYPE_CHECKING

import aiohttp
import discord

if TYPE_CHECKING:
    from discord.ext.commands import Bot
    from database.manager import DatabaseManager

logger = logging.getLogger(__name__)

# WebSocket URL for live data
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Crypto markets to monitor (15min Up/Down)
CRYPTO_MARKETS = {
    "BTC": "btc-updown-15m",
    "ETH": "eth-updown-15m",
    "XRP": "xrp-updown-15m",
    "SOL": "sol-updown-15m",
}


@dataclass
class CryptoTrade:
    """Represents a trade on a crypto market."""
    tx_hash: str
    timestamp: int
    wallet_address: str
    side: str  # BUY or SELL
    outcome: str  # Up or Down
    price: float
    size: float
    usdc_size: float
    market_slug: str
    crypto: str  # BTC, ETH, etc.


class CryptoWebSocketService:
    """
    WebSocket client for real-time crypto market monitoring.
    Connects to Polymarket's live data stream and filters trades by tracked wallets.
    """

    def __init__(
        self,
        db: "DatabaseManager",
        bot: "Bot",
        on_trade: Optional[Callable] = None,
    ):
        self.db = db
        self.bot = bot
        self.on_trade = on_trade
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._running = False
        self._tracked_wallets: set[str] = set()
        self._processed_trades: set[str] = set()  # Track processed tx hashes
        self._reconnect_delay = 5

    async def start(self) -> None:
        """Start the crypto monitoring service."""
        self._running = True
        # Use polling-based monitoring instead of WebSocket
        # Polymarket WebSocket doesn't support market-level subscriptions well
        asyncio.create_task(self._run_polling())
        logger.info("CryptoWebSocketService started (polling mode)")

    async def stop(self) -> None:
        """Stop the WebSocket connection."""
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()
        logger.info("CryptoWebSocketService stopped")

    async def refresh_tracked_wallets(self) -> None:
        """Refresh the list of wallets to track from the database."""
        wallets = await self.db.get_crypto_tracked_wallets()
        self._tracked_wallets = {w.lower() for w in wallets}
        logger.info(f"Tracking {len(self._tracked_wallets)} wallet(s) for crypto markets")

    async def _run_polling(self) -> None:
        """Main polling loop for crypto market monitoring."""
        # Wait a bit before starting
        await asyncio.sleep(5)

        while self._running:
            try:
                await self._poll_crypto_markets()
            except Exception as e:
                logger.error(f"Crypto polling error: {e}")

            # Poll every 3 seconds for near real-time updates
            await asyncio.sleep(3)

    async def _poll_crypto_markets(self) -> None:
        """Poll Polymarket API for crypto trades from tracked wallets."""
        if not self._tracked_wallets:
            return

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()

        try:
            # Get recent activity
            url = "https://data-api.polymarket.com/activity"
            async with self._session.get(url, params={"limit": 100}) as response:
                if response.status != 200:
                    return
                data = await response.json()

            for item in data:
                if item.get("type") != "TRADE":
                    continue

                wallet = item.get("proxyWallet", "").lower()
                if wallet not in self._tracked_wallets:
                    continue

                # Check if this is a crypto 15min market
                event_slug = item.get("eventSlug", "").lower()
                crypto = self._get_crypto_from_event(event_slug)

                if not crypto:
                    continue

                # Check if we've already processed this trade
                tx_hash = item.get("transactionHash", "")
                if tx_hash in self._processed_trades:
                    continue

                self._processed_trades.add(tx_hash)
                # Keep only last 1000 trades in memory
                if len(self._processed_trades) > 1000:
                    self._processed_trades = set(list(self._processed_trades)[-500:])

                # Process and notify
                await self._process_api_trade(crypto, item)

        except Exception as e:
            logger.error(f"Error polling crypto markets: {e}")

    def _get_crypto_from_event(self, event_slug: str) -> Optional[str]:
        """Check if event is a crypto 15min market and return crypto symbol."""
        event_slug = event_slug.lower()

        # Match patterns like "btc-15-min", "eth-15-minute", etc.
        for crypto in CRYPTO_MARKETS.keys():
            crypto_lower = crypto.lower()
            if crypto_lower in event_slug and ("15" in event_slug or "15min" in event_slug or "15-min" in event_slug):
                return crypto

        return None

    async def _process_api_trade(self, crypto: str, item: dict) -> None:
        """Process a trade from the API and notify subscribers."""
        try:
            wallet = item.get("proxyWallet", "").lower()

            # Determine outcome from market title
            title = item.get("title", "").lower()
            outcome = "Up" if "up" in title else "Down" if "down" in title else item.get("outcome", "")

            trade = CryptoTrade(
                tx_hash=item.get("transactionHash", ""),
                timestamp=int(item.get("timestamp", 0)) if item.get("timestamp") else 0,
                wallet_address=wallet,
                side=item.get("side", ""),
                outcome=outcome,
                price=float(item.get("price", 0)),
                size=float(item.get("size", 0)),
                usdc_size=float(item.get("usdcSize", 0)),
                market_slug=item.get("eventSlug", ""),
                crypto=crypto,
            )

            logger.info(f"Crypto trade detected: {crypto} {trade.side} {trade.outcome} by {wallet[:8]}...")

            await self._notify_trade(trade)

        except Exception as e:
            logger.error(f"Error processing API trade: {e}")

    async def _run(self) -> None:
        """Main WebSocket loop with reconnection logic (not used currently)."""
        while self._running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                if self._running:
                    logger.info(f"Reconnecting in {self._reconnect_delay}s...")
                    await asyncio.sleep(self._reconnect_delay)

    async def _connect_and_listen(self) -> None:
        """Connect to WebSocket and listen for messages."""
        self._session = aiohttp.ClientSession()

        # Refresh tracked wallets before connecting
        await self.refresh_tracked_wallets()

        try:
            async with self._session.ws_connect(WS_URL) as ws:
                self._ws = ws
                logger.info(f"Connected to Polymarket WebSocket: {WS_URL}")

                # Subscribe to all crypto markets
                await self._subscribe_to_markets()

                # Listen for messages
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._handle_message(msg.data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error(f"WebSocket error: {ws.exception()}")
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        logger.warning("WebSocket closed")
                        break

        finally:
            if self._session:
                await self._session.close()

    async def _subscribe_to_markets(self) -> None:
        """Subscribe to all crypto 15min markets."""
        # We need to get current market asset IDs
        # For now, subscribe using market type
        subscription = {
            "type": "market",
            "markets": list(CRYPTO_MARKETS.values()),
        }

        if self._ws:
            await self._ws.send_json(subscription)
            logger.info(f"Subscribed to markets: {list(CRYPTO_MARKETS.keys())}")

    async def _handle_message(self, data: str) -> None:
        """Handle incoming WebSocket message."""
        try:
            message = json.loads(data)

            # Check if it's a trade message
            if message.get("event_type") == "last_trade_price":
                await self._process_trade(message)

        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON: {data[:100]}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    async def _process_trade(self, message: dict) -> None:
        """Process a trade message and notify if it's from a tracked wallet."""
        try:
            wallet = message.get("maker", "") or message.get("taker", "")
            wallet = wallet.lower() if wallet else ""

            # Check if this wallet is being tracked
            if wallet not in self._tracked_wallets:
                return

            # Parse trade data
            market_slug = message.get("market", "")
            crypto = self._get_crypto_from_slug(market_slug)

            if not crypto:
                return

            trade = CryptoTrade(
                tx_hash=message.get("transaction_hash", ""),
                timestamp=int(message.get("timestamp", 0)),
                wallet_address=wallet,
                side=message.get("side", ""),
                outcome=message.get("outcome", ""),
                price=float(message.get("price", 0)),
                size=float(message.get("size", 0)),
                usdc_size=float(message.get("size", 0)) * float(message.get("price", 0)),
                market_slug=market_slug,
                crypto=crypto,
            )

            logger.info(f"Crypto trade detected: {crypto} {trade.side} {trade.outcome} by {wallet[:8]}...")

            # Notify subscribers
            await self._notify_trade(trade)

        except Exception as e:
            logger.error(f"Error processing trade: {e}")

    def _get_crypto_from_slug(self, slug: str) -> Optional[str]:
        """Get crypto symbol from market slug."""
        slug_lower = slug.lower()
        for crypto, market_slug in CRYPTO_MARKETS.items():
            if market_slug in slug_lower or crypto.lower() in slug_lower:
                return crypto
        return None

    async def _notify_trade(self, trade: CryptoTrade) -> None:
        """Notify all subscribers of a crypto trade."""
        try:
            subscribers = await self.db.get_crypto_subscribers(trade.wallet_address)

            if not subscribers:
                return

            embed = self._build_trade_embed(trade)

            for user in subscribers:
                if not user.alert_thread_id:
                    continue

                try:
                    thread = self.bot.get_channel(user.alert_thread_id)
                    if thread is None:
                        thread = await self.bot.fetch_channel(user.alert_thread_id)

                    if thread:
                        await thread.send(embed=embed)

                except discord.NotFound:
                    logger.warning(f"Thread not found for user {user.discord_id}")
                except Exception as e:
                    logger.error(f"Error notifying user {user.discord_id}: {e}")

        except Exception as e:
            logger.error(f"Error in _notify_trade: {e}")

    def _build_trade_embed(self, trade: CryptoTrade) -> discord.Embed:
        """Build Discord embed for crypto trade notification."""
        is_buy = trade.side.upper() == "BUY"
        is_up = trade.outcome.lower() == "up"

        # Color based on position
        if is_up:
            color = discord.Color.green()
            direction = "📈 UP"
        else:
            color = discord.Color.red()
            direction = "📉 DOWN"

        side_icon = "🟢" if is_buy else "🔴"
        side_text = "BUY" if is_buy else "SELL"

        short_addr = f"{trade.wallet_address[:6]}...{trade.wallet_address[-4:]}"

        # Crypto icon
        crypto_icons = {
            "BTC": "₿",
            "ETH": "Ξ",
            "XRP": "✕",
            "SOL": "◎",
        }
        crypto_icon = crypto_icons.get(trade.crypto, "🪙")

        trade_time = datetime.fromtimestamp(trade.timestamp).strftime("%H:%M:%S") if trade.timestamp else "now"

        price_percent = int(trade.price * 100)

        embed = discord.Embed(
            title=f"{crypto_icon} {trade.crypto} 15min {direction}",
            color=color,
        )

        embed.description = (
            f"👤 `{short_addr}`\n\n"
            f"{side_icon} **{side_text}** @ ${trade.price:.2f} ({price_percent}%)\n\n"
            f"📊 Size: {trade.size:.1f} shares\n"
            f"💰 ~{trade.usdc_size:.2f} USDC\n\n"
            f"🕒 {trade_time}"
        )

        return embed


class CryptoMonitoringEngine:
    """
    Fallback polling-based monitoring for crypto markets.
    Used when WebSocket is not available or as backup.
    """

    CRYPTO_SLUGS = list(CRYPTO_MARKETS.values())

    def __init__(
        self,
        db: "DatabaseManager",
        bot: "Bot",
    ):
        self.db = db
        self.bot = bot
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def run_monitoring_cycle(self) -> None:
        """Poll crypto markets for trades from tracked wallets."""
        try:
            tracked_wallets = await self.db.get_crypto_tracked_wallets()

            if not tracked_wallets:
                return

            logger.info(f"Crypto monitoring: {len(tracked_wallets)} wallet(s)")

            # Get recent trades for each market
            for crypto, slug in CRYPTO_MARKETS.items():
                await self._check_market(crypto, slug, tracked_wallets)
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.exception(f"Error in crypto monitoring: {e}")

    async def _check_market(self, crypto: str, slug: str, tracked_wallets: list[str]) -> None:
        """Check a specific crypto market for trades from tracked wallets."""
        try:
            session = await self._get_session()
            url = f"https://data-api.polymarket.com/activity"

            # Get recent trades for this market (by event slug pattern)
            async with session.get(url, params={"limit": 50}) as response:
                if response.status != 200:
                    return

                data = await response.json()

            # Filter for our tracked wallets and this crypto market
            tracked_set = {w.lower() for w in tracked_wallets}

            for item in data:
                if item.get("type") != "TRADE":
                    continue

                wallet = item.get("proxyWallet", "").lower()
                event_slug = item.get("eventSlug", "").lower()

                # Check if this is our crypto market and tracked wallet
                if wallet in tracked_set and slug in event_slug:
                    # Process this trade
                    await self._process_trade(crypto, item)

        except Exception as e:
            logger.error(f"Error checking market {crypto}: {e}")

    async def _process_trade(self, crypto: str, item: dict) -> None:
        """Process and notify about a crypto trade."""
        # This would be similar to the WebSocket notification
        # Implementation depends on your needs
        pass
