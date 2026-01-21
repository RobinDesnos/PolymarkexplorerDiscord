"""
Polymarket Discord Bot - Main Entry Point
Monitors wallets on Polymarket and sends alerts to private Discord threads.
"""

import asyncio
import logging
import sys

import discord
from discord.ext import commands

from config import DISCORD_TOKEN, GUILD_ID
from database.manager import DatabaseManager
from database.db_init import init_database
from cogs.tracking import TrackingCog

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)

logging.getLogger("discord").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("aiosqlite").setLevel(logging.WARNING)


class PolymarketBot(commands.Bot):
    """Custom bot class with database integration."""

    def __init__(self, db: DatabaseManager):
        intents = discord.Intents.default()

        super().__init__(
            command_prefix="!",
            intents=intents,
            help_command=None,
        )
        self.db = db

    async def setup_hook(self) -> None:
        """Called when the bot is starting up."""
        await self.add_cog(TrackingCog(self, self.db))
        logger.info("TrackingCog added")

        # Sync commands to guild for instant update
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
            logger.info(f"Slash commands synchronized to guild {GUILD_ID}")
        else:
            await self.tree.sync()
            logger.info("Slash commands synchronized globally (may take up to 1h)")

    async def on_ready(self) -> None:
        """Called when the bot is fully connected."""
        logger.info(f"Bot connected as {self.user} (ID: {self.user.id})")
        logger.info(f"Connected to {len(self.guilds)} guild(s)")

        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="Polymarket wallets",
            )
        )

    async def on_error(self, event_method: str, *args, **kwargs) -> None:
        """Global error handler."""
        logger.exception(f"Error in {event_method}")


async def main() -> None:
    """Main entry point."""
    logger.info("Starting Polymarket Bot...")

    init_database()

    db = DatabaseManager()
    await db.init()

    bot = PolymarketBot(db)

    try:
        await bot.start(DISCORD_TOKEN)
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
    finally:
        logger.info("Shutting down...")
        await db.close()
        await bot.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
