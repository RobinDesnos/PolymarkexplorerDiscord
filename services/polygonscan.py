"""
PolygonScan API service for tracking on-chain USDC deposits and withdrawals.
Monitors token transfers to/from Polymarket proxy wallets.
"""

import aiohttp
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, TYPE_CHECKING

import discord

from config import API_TIMEOUT, POLYGONSCAN_API_KEY

if TYPE_CHECKING:
    from discord.ext.commands import Bot
    from database.manager import DatabaseManager, User

logger = logging.getLogger(__name__)

# USDC contract on Polygon
USDC_CONTRACT = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# Polymarket USDC exchange contract (deposits go here)
POLYMARKET_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"


@dataclass
class Transfer:
    """Represents a USDC transfer (deposit or withdrawal)."""
    tx_hash: str
    timestamp: int
    from_address: str
    to_address: str
    value: float  # USDC amount (already converted from wei)
    transfer_type: str  # "DEPOSIT" or "WITHDRAWAL"


class PolygonScanService:
    """Client for PolygonScan API to track USDC transfers."""

    BASE_URL = "https://api.polygonscan.com/api"

    def __init__(self, api_key: str = POLYGONSCAN_API_KEY, timeout: int = API_TIMEOUT):
        self.api_key = api_key or ""  # Empty = free tier (5 calls/sec, 10k/day)
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_usdc_transfers(
        self, wallet_address: str, limit: int = 50
    ) -> list[Transfer]:
        """
        Fetch recent USDC transfers for a wallet.
        Returns both incoming (deposits) and outgoing (withdrawals) transfers.
        """
        wallet_address = wallet_address.lower()
        transfers = []

        try:
            session = await self._get_session()

            params = {
                "module": "account",
                "action": "tokentx",
                "contractaddress": USDC_CONTRACT,
                "address": wallet_address,
                "page": 1,
                "offset": limit,
                "sort": "desc",
            }

            if self.api_key:
                params["apikey"] = self.api_key

            async with session.get(self.BASE_URL, params=params) as response:
                if response.status != 200:
                    logger.error(f"PolygonScan API error: {response.status}")
                    return []

                data = await response.json()

                if data.get("status") != "1":
                    message = data.get("message", "Unknown error")
                    if message != "No transactions found":
                        logger.warning(f"PolygonScan API: {message}")
                        logger.warning(f"Full response: {data}")
                    return []

                for tx in data.get("result", []):
                    try:
                        from_addr = tx.get("from", "").lower()
                        to_addr = tx.get("to", "").lower()

                        # Determine if deposit or withdrawal
                        if to_addr == wallet_address:
                            transfer_type = "DEPOSIT"
                        elif from_addr == wallet_address:
                            transfer_type = "WITHDRAWAL"
                        else:
                            continue

                        # USDC has 6 decimals
                        value_wei = int(tx.get("value", 0))
                        value = value_wei / 1_000_000

                        transfers.append(Transfer(
                            tx_hash=tx.get("hash", ""),
                            timestamp=int(tx.get("timeStamp", 0)),
                            from_address=from_addr,
                            to_address=to_addr,
                            value=value,
                            transfer_type=transfer_type,
                        ))

                    except (KeyError, ValueError, TypeError) as e:
                        logger.warning(f"Error parsing transfer: {e}")
                        continue

        except asyncio.TimeoutError:
            logger.warning("PolygonScan API timeout")
        except aiohttp.ClientError as e:
            logger.error(f"PolygonScan network error: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error fetching transfers: {e}")

        return transfers


class OnChainMonitoringEngine:
    """
    Monitoring engine for on-chain USDC deposits/withdrawals.
    Tracks last seen tx_hash per linked wallet to avoid duplicate notifications.
    """

    def __init__(
        self,
        db: "DatabaseManager",
        api: PolygonScanService,
        bot: "Bot",
    ):
        self.db = db
        self.api = api
        self.bot = bot

    async def run_monitoring_cycle(self) -> None:
        """Run a single monitoring cycle for all linked polygon wallets."""
        try:
            linked_wallets = await self.db.get_unique_polygon_wallets()

            if not linked_wallets:
                return

            logger.info(f"Monitoring {len(linked_wallets)} linked polygon wallet(s)...")

            for wallet in linked_wallets:
                await self._process_wallet(wallet)
                await asyncio.sleep(0.5)  # Rate limiting

        except Exception as e:
            logger.exception(f"Error in on-chain monitoring cycle: {e}")

    async def _process_wallet(self, wallet) -> None:
        """Process a single polygon wallet: fetch transfers, detect new ones, notify."""
        try:
            recent_transfers = await self.api.get_usdc_transfers(
                wallet.polygon_address, limit=20
            )

            if not recent_transfers:
                return

            last_tx_hash = await self.db.get_last_polygon_tx_hash(wallet.polygon_address)
            new_transfers = self._get_new_transfers(recent_transfers, last_tx_hash)

            if new_transfers:
                logger.info(
                    f"{len(new_transfers)} new transfer(s) for {wallet.name} ({wallet.polygon_address[:8]}...)"
                )

                # Get subscribers for the linked Polymarket wallet
                subscribers = await self.db.get_polygon_wallet_subscribers(wallet.polygon_address)

                for transfer in reversed(new_transfers):
                    await self._notify_subscribers(wallet, transfer, subscribers)

                await self.db.set_last_polygon_tx_hash(
                    wallet.polygon_address, recent_transfers[0].tx_hash
                )

        except Exception as e:
            logger.error(f"Error processing polygon wallet {wallet.polygon_address}: {e}")

    def _get_new_transfers(
        self, transfers: list[Transfer], last_tx_hash: Optional[str]
    ) -> list[Transfer]:
        """Get transfers that are newer than last_tx_hash."""
        if not last_tx_hash:
            return []

        new_transfers = []
        for transfer in transfers:
            if transfer.tx_hash == last_tx_hash:
                break
            new_transfers.append(transfer)

        return new_transfers

    async def _notify_subscribers(
        self, wallet, transfer: Transfer, subscribers: list["User"]
    ) -> None:
        """Send notification embed to all subscribers' private threads."""
        embed = self._build_transfer_embed(wallet, transfer)

        for user in subscribers:
            if not user.alert_thread_id:
                continue

            try:
                thread = self.bot.get_channel(user.alert_thread_id)
                if thread is None:
                    thread = await self.bot.fetch_channel(user.alert_thread_id)

                if thread:
                    await thread.send(embed=embed)
                    logger.debug(f"Notified user {user.discord_id} of transfer")
                else:
                    await self.db.set_user_inactive(user.discord_id)

            except discord.NotFound:
                logger.warning(f"Thread deleted for user {user.discord_id}")
                await self.db.set_user_inactive(user.discord_id)
            except discord.Forbidden:
                logger.warning(f"Permission denied for thread {user.alert_thread_id}")
            except Exception as e:
                logger.error(f"Error notifying user {user.discord_id}: {e}")

    def _build_transfer_embed(self, wallet, transfer: Transfer) -> discord.Embed:
        """Build a Discord embed for a transfer notification."""
        is_deposit = transfer.transfer_type == "DEPOSIT"
        color = discord.Color.green() if is_deposit else discord.Color.orange()
        type_emoji = "DEPOSIT" if is_deposit else "WITHDRAWAL"
        type_icon = "📥" if is_deposit else "📤"

        short_addr = f"{wallet.polygon_address[:6]}...{wallet.polygon_address[-4:]}"
        short_tx = f"{transfer.tx_hash[:6]}...{transfer.tx_hash[-4:]}"

        polygonscan_wallet = f"https://polygonscan.com/address/{wallet.polygon_address}"
        polygonscan_tx = f"https://polygonscan.com/tx/{transfer.tx_hash}"

        transfer_time = datetime.fromtimestamp(transfer.timestamp).strftime("%H:%M:%S")

        # Build source/destination info with clickable links
        if is_deposit:
            short_from = f"{transfer.from_address[:6]}...{transfer.from_address[-4:]}"
            from_url = f"https://polygonscan.com/address/{transfer.from_address}"
            direction_info = f"De: [`{short_from}`]({from_url})"
        else:
            short_to = f"{transfer.to_address[:6]}...{transfer.to_address[-4:]}"
            to_url = f"https://polygonscan.com/address/{transfer.to_address}"
            direction_info = f"Vers: [`{short_to}`]({to_url})"

        embed = discord.Embed(color=color)

        embed.description = (
            f"👤 [`{short_addr}`]({polygonscan_wallet}) ({wallet.name})\n\n"
            f"{type_icon} **{type_emoji}** - {transfer.value:,.2f} USDC\n\n"
            f"{direction_info}\n\n"
            f"🕒 {transfer_time} • 🔗 [`{short_tx}`]({polygonscan_tx})"
        )

        return embed
