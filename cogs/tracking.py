"""
Discord Cog for wallet tracking commands and monitoring background task.
"""

import re
import logging
from datetime import datetime
from typing import Optional

import discord
from discord import app_commands, Interaction
from discord.ext import commands, tasks
from discord.ui import View, Button, Select

from config import POLLING_INTERVAL
from database.manager import DatabaseManager
from services.polymarket import PolymarketService, MonitoringEngine
from services.polygonscan import PolygonScanService, OnChainMonitoringEngine
from services.crypto_websocket import CryptoWebSocketService, CRYPTO_MARKETS

logger = logging.getLogger(__name__)

ADDRESS_PATTERN = re.compile(r"^0x[a-fA-F0-9]{40}$")


def is_valid_address(address: str) -> bool:
    """Validate Ethereum address format."""
    return bool(ADDRESS_PATTERN.match(address))


class WalletSelectView(View):
    """View with a dropdown to select a wallet and view its positions."""

    def __init__(self, wallets: list, api: PolymarketService, timeout: float = 120):
        super().__init__(timeout=timeout)
        self.api = api

        options = []
        for w in wallets[:25]:
            short_addr = f"{w.address[:6]}...{w.address[-4:]}"
            options.append(discord.SelectOption(
                label=w.name,
                value=w.address,
                description=short_addr,
            ))

        select = Select(
            placeholder="Selectionne un wallet pour voir ses positions...",
            options=options,
            custom_id="wallet_select",
        )
        select.callback = self.select_callback
        self.add_item(select)

    async def select_callback(self, interaction: Interaction):
        select = interaction.data["values"][0]
        wallet_address = select

        await interaction.response.defer(ephemeral=True)

        positions = await self.api.get_positions(wallet_address, self.db)

        if not positions:
            await interaction.followup.send(
                f"Aucune position ouverte pour ce wallet.",
                ephemeral=True,
            )
            return

        embed = build_positions_embed(wallet_address, positions)
        await interaction.followup.send(embed=embed, ephemeral=True)


def build_positions_embed(wallet_address: str, positions: list) -> discord.Embed:
    """Build an embed showing positions for a wallet."""
    short_addr = f"{wallet_address[:6]}...{wallet_address[-4:]}"
    polygonscan_url = f"https://polygonscan.com/address/{wallet_address}"

    embed = discord.Embed(
        title=f"Positions ouvertes",
        description=f"👤 [`{short_addr}`]({polygonscan_url})",
        color=discord.Color.blue(),
        timestamp=datetime.utcnow(),
    )

    total_value = 0
    for pos in positions[:10]:
        title = pos.market_title[:40] + "..." if len(pos.market_title or "") > 40 else pos.market_title
        value = pos.quantity * (pos.avg_price or 0)
        total_value += value

        price_percent = int((pos.avg_price or 0) * 100)
        progress = "🟩" * (price_percent // 10) + "⬜" * (10 - price_percent // 10)

        embed.add_field(
            name=f"{pos.outcome} - {title}",
            value=f"Qte: {pos.quantity:.1f} | Prix moy: ${pos.avg_price:.2f}\n{progress} {price_percent}%",
            inline=False,
        )

    if len(positions) > 10:
        embed.add_field(
            name="...",
            value=f"*Et {len(positions) - 10} autre(s) position(s)*",
            inline=False,
        )

    embed.set_footer(text=f"Total positions: {len(positions)}")

    return embed


class TrackingCog(commands.Cog):
    """Cog handling wallet tracking commands and monitoring."""

    def __init__(self, bot: commands.Bot, db: DatabaseManager):
        self.bot = bot
        self.db = db
        self.api = PolymarketService()
        self.polygon_api = PolygonScanService()
        self.monitoring_engine = MonitoringEngine(db, self.api, bot)
        self.onchain_monitoring_engine = OnChainMonitoringEngine(db, self.polygon_api, bot)
        self.crypto_ws_service = CryptoWebSocketService(db, bot)

    async def cog_load(self) -> None:
        """Called when the cog is loaded."""
        self.monitoring_task.start()
        self.onchain_monitoring_task.start()
        # Start the crypto WebSocket service for real-time monitoring
        await self.crypto_ws_service.start()
        logger.info("TrackingCog loaded, monitoring tasks started (including crypto WebSocket)")

    async def cog_unload(self) -> None:
        """Called when the cog is unloaded."""
        self.monitoring_task.cancel()
        self.onchain_monitoring_task.cancel()
        await self.crypto_ws_service.stop()
        await self.api.close()
        await self.polygon_api.close()
        logger.info("TrackingCog unloaded")

    # ==================== BACKGROUND TASK ====================

    @tasks.loop(seconds=POLLING_INTERVAL)
    async def monitoring_task(self) -> None:
        """Background task that monitors wallets for new transactions."""
        try:
            await self.monitoring_engine.run_monitoring_cycle()
        except Exception as e:
            logger.exception(f"Error in monitoring task: {e}")

    @monitoring_task.before_loop
    async def before_monitoring(self) -> None:
        """Wait for bot to be ready before starting monitoring."""
        await self.bot.wait_until_ready()
        logger.info("Monitoring task ready to start")

    @tasks.loop(seconds=POLLING_INTERVAL)
    async def onchain_monitoring_task(self) -> None:
        """Background task that monitors polygon wallets for deposits/withdrawals."""
        try:
            await self.onchain_monitoring_engine.run_monitoring_cycle()
        except Exception as e:
            logger.exception(f"Error in on-chain monitoring task: {e}")

    @onchain_monitoring_task.before_loop
    async def before_onchain_monitoring(self) -> None:
        """Wait for bot to be ready before starting on-chain monitoring."""
        await self.bot.wait_until_ready()
        logger.info("On-chain monitoring task ready to start")

    # ==================== COMMANDS ====================

    @app_commands.command(name="setup", description="Cree ton dashboard prive pour recevoir tes alertes")
    async def setup(self, interaction: Interaction) -> None:
        """Create a private thread for the user's alerts."""
        existing_thread_id = await self.db.get_user_thread(interaction.user.id)

        if existing_thread_id:
            try:
                existing_thread = self.bot.get_channel(existing_thread_id)
                if existing_thread is None:
                    existing_thread = await self.bot.fetch_channel(existing_thread_id)

                if existing_thread:
                    await interaction.response.send_message(
                        f"Tu as deja un dashboard: {existing_thread.mention}\n"
                        f"Utilise `/resetdashboard` si tu veux en creer un nouveau.",
                        ephemeral=True,
                    )
                    return
            except discord.NotFound:
                pass
            except Exception:
                pass

        # Defer response first to avoid timeout
        await interaction.response.defer(ephemeral=True)

        try:
            thread = await interaction.channel.create_thread(
                name=f"Dashboard-{interaction.user.display_name}",
                type=discord.ChannelType.private_thread,
                invitable=False,
            )

            await self.db.set_user_thread(interaction.user.id, thread.id)

            welcome_embed = discord.Embed(
                title="Bienvenue dans ton Dashboard Polymarket!",
                description=(
                    "Ce thread est ton espace prive pour recevoir "
                    "les alertes de tes wallets suivis.\n\n"
                    "**Commandes disponibles:**\n"
                    "`/track <address> <name>` - Suivre un wallet\n"
                    "`/mywallets` - Voir ta liste (avec positions)\n"
                    "`/positions <address>` - Voir les positions d'un wallet\n"
                    "`/untrack <address>` - Arreter de suivre\n\n"
                    "Bonne chasse aux baleines!"
                ),
                color=discord.Color.blue(),
                timestamp=datetime.utcnow(),
            )
            await thread.send(embed=welcome_embed)

            await interaction.followup.send(
                f"Dashboard cree! Tes alertes seront envoyees dans {thread.mention}",
                ephemeral=True,
            )
            logger.info(f"Created dashboard thread {thread.id} for user {interaction.user.id}")

        except discord.Forbidden:
            await interaction.followup.send(
                "Je n'ai pas la permission de creer des threads dans ce salon.",
                ephemeral=True,
            )
        except Exception as e:
            logger.error(f"Error creating dashboard: {e}")
            await interaction.followup.send(
                "Une erreur est survenue lors de la creation du dashboard.",
                ephemeral=True,
            )

    @app_commands.command(name="resetdashboard", description="Supprime ton dashboard actuel et en cree un nouveau")
    async def resetdashboard(self, interaction: Interaction) -> None:
        """Reset user's dashboard by creating a new thread."""
        await interaction.response.defer(ephemeral=True)

        old_thread_id = await self.db.get_user_thread(interaction.user.id)

        if old_thread_id:
            try:
                old_thread = self.bot.get_channel(old_thread_id)
                if old_thread is None:
                    old_thread = await self.bot.fetch_channel(old_thread_id)
                if old_thread:
                    await old_thread.delete()
            except Exception:
                pass

        try:
            thread = await interaction.channel.create_thread(
                name=f"Dashboard-{interaction.user.display_name}",
                type=discord.ChannelType.private_thread,
                invitable=False,
            )

            await self.db.set_user_thread(interaction.user.id, thread.id)

            welcome_embed = discord.Embed(
                title="Dashboard reinitialise!",
                description=(
                    "Ton nouveau dashboard est pret.\n\n"
                    "**Commandes disponibles:**\n"
                    "`/track <address> <name>` - Suivre un wallet\n"
                    "`/mywallets` - Voir ta liste (avec positions)\n"
                    "`/positions <address>` - Voir les positions d'un wallet\n"
                    "`/untrack <address>` - Arreter de suivre"
                ),
                color=discord.Color.green(),
                timestamp=datetime.utcnow(),
            )
            await thread.send(embed=welcome_embed)

            await interaction.followup.send(
                f"Nouveau dashboard cree: {thread.mention}",
                ephemeral=True,
            )

        except discord.Forbidden:
            await interaction.followup.send(
                "Je n'ai pas la permission de creer des threads dans ce salon.",
                ephemeral=True,
            )
        except Exception as e:
            logger.error(f"Error resetting dashboard: {e}")
            await interaction.followup.send(
                "Une erreur est survenue.",
                ephemeral=True,
            )

    @app_commands.command(name="track", description="Ajoute un wallet a suivre")
    @app_commands.describe(
        address="L'adresse du wallet (format: 0x...)",
        name="Un nom pour identifier ce wallet",
    )
    async def track(self, interaction: Interaction, address: str, name: str) -> None:
        """Add a wallet to the user's tracking list."""
        thread_id = await self.db.get_user_thread(interaction.user.id)
        if not thread_id:
            await interaction.response.send_message(
                "Utilise `/setup` d'abord pour creer ton dashboard.",
                ephemeral=True,
            )
            return

        if not is_valid_address(address):
            await interaction.response.send_message(
                "Adresse invalide. Format attendu: `0x` suivi de 40 caracteres hexadecimaux.\n"
                "Exemple: `0x1234567890abcdef1234567890abcdef12345678`",
                ephemeral=True,
            )
            return

        if len(name) > 32:
            await interaction.response.send_message(
                "Le nom du wallet ne peut pas depasser 32 caracteres.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            success = await self.db.add_tracking(interaction.user.id, address, name)

            if not success:
                await interaction.followup.send(
                    f"Tu suis deja ce wallet!",
                    ephemeral=True,
                )
                return

            trades = await self.api.get_recent_trades(address, limit=1)
            if trades:
                await self.db.set_last_tx_hash(address, trades[0].tx_hash)

            positions = await self.api.get_positions(address, self.db)

            short_addr = f"{address[:8]}...{address[-6:]}"
            await interaction.followup.send(
                f"Wallet **{name}** (`{short_addr}`) ajoute!\n"
                f"Positions actuelles: {len(positions)}\n"
                f"Tu recevras les alertes des **nouvelles transactions** dans ton dashboard.",
                ephemeral=True,
            )
            logger.info(f"User {interaction.user.id} now tracking {address} as '{name}'")

        except Exception as e:
            logger.error(f"Error adding tracking: {e}")
            await interaction.followup.send(
                "Une erreur est survenue lors de l'ajout du wallet.",
                ephemeral=True,
            )

    @app_commands.command(name="mywallets", description="Affiche tes wallets suivis")
    async def mywallets(self, interaction: Interaction) -> None:
        """List all wallets tracked by the user with interactive buttons."""
        wallets = await self.db.get_user_wallets(interaction.user.id)

        if not wallets:
            await interaction.response.send_message(
                "Tu ne suis aucun wallet.\n"
                "Utilise `/track <address> <name>` pour commencer.",
                ephemeral=True,
            )
            return

        embed = discord.Embed(
            title="Mes Wallets Suivis",
            description="Selectionne un wallet dans le menu pour voir ses positions.",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow(),
        )

        for wallet in wallets:
            short_addr = f"{wallet.address[:6]}...{wallet.address[-4:]}"
            polygonscan_url = f"https://polygonscan.com/address/{wallet.address}"
            embed.add_field(
                name=wallet.name,
                value=f"[`{short_addr}`]({polygonscan_url})",
                inline=True,
            )

        embed.set_footer(text=f"Total: {len(wallets)} wallet(s)")

        view = WalletSelectView(wallets, self.api)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @app_commands.command(name="positions", description="Affiche les positions d'un wallet")
    @app_commands.describe(address="L'adresse du wallet")
    async def positions(self, interaction: Interaction, address: str) -> None:
        """Show positions for a specific wallet."""
        if not is_valid_address(address):
            await interaction.response.send_message(
                "Adresse invalide. Format attendu: `0x` suivi de 40 caracteres hexadecimaux.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            positions = await self.api.get_positions(address, self.db)

            if not positions:
                await interaction.followup.send(
                    f"Aucune position ouverte pour ce wallet.",
                    ephemeral=True,
                )
                return

            embed = build_positions_embed(address, positions)
            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            await interaction.followup.send(
                "Une erreur est survenue lors de la recuperation des positions.",
                ephemeral=True,
            )

    @app_commands.command(name="untrack", description="Retire un wallet de ta liste")
    @app_commands.describe(address="L'adresse du wallet a retirer")
    async def untrack(self, interaction: Interaction, address: str) -> None:
        """Remove a wallet from the user's tracking list."""
        if not is_valid_address(address):
            await interaction.response.send_message(
                "Adresse invalide. Format attendu: `0x` suivi de 40 caracteres hexadecimaux.",
                ephemeral=True,
            )
            return

        success = await self.db.remove_tracking(interaction.user.id, address)

        if success:
            short_addr = f"{address[:8]}...{address[-6:]}"
            await interaction.response.send_message(
                f"Wallet `{short_addr}` retire de ta liste.",
                ephemeral=True,
            )
            logger.info(f"User {interaction.user.id} stopped tracking {address}")
        else:
            await interaction.response.send_message(
                "Ce wallet n'est pas dans ta liste.",
                ephemeral=True,
            )

    @app_commands.command(name="linkwallet", description="Lie un wallet Polygon pour suivre les depots/retraits")
    @app_commands.describe(
        polygon_address="L'adresse du wallet Polygon d'origine (0x...)",
        polymarket_address="L'adresse du wallet Polymarket proxy associe (0x...)",
        name="Un nom pour identifier ce wallet",
    )
    async def linkwallet(
        self, interaction: Interaction, polygon_address: str, polymarket_address: str, name: str
    ) -> None:
        """Link a Polygon wallet to track deposits/withdrawals."""
        thread_id = await self.db.get_user_thread(interaction.user.id)
        if not thread_id:
            await interaction.response.send_message(
                "Utilise `/setup` d'abord pour creer ton dashboard.",
                ephemeral=True,
            )
            return

        if not is_valid_address(polygon_address):
            await interaction.response.send_message(
                "Adresse Polygon invalide. Format attendu: `0x` suivi de 40 caracteres hexadecimaux.",
                ephemeral=True,
            )
            return

        if not is_valid_address(polymarket_address):
            await interaction.response.send_message(
                "Adresse Polymarket invalide. Format attendu: `0x` suivi de 40 caracteres hexadecimaux.",
                ephemeral=True,
            )
            return

        if len(name) > 32:
            await interaction.response.send_message(
                "Le nom du wallet ne peut pas depasser 32 caracteres.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            # Check if the polymarket wallet is being tracked
            user_wallets = await self.db.get_user_wallets(interaction.user.id)
            tracked_addresses = [w.address.lower() for w in user_wallets]

            if polymarket_address.lower() not in tracked_addresses:
                await interaction.followup.send(
                    f"Tu dois d'abord suivre le wallet Polymarket avec `/track` avant de lier un wallet Polygon.",
                    ephemeral=True,
                )
                return

            success = await self.db.link_polygon_wallet(polygon_address, polymarket_address, name)

            if not success:
                await interaction.followup.send(
                    f"Ce wallet Polygon est deja lie!",
                    ephemeral=True,
                )
                return

            # Get initial transfers to set baseline
            transfers = await self.polygon_api.get_usdc_transfers(polygon_address, limit=1)
            if transfers:
                await self.db.set_last_polygon_tx_hash(polygon_address, transfers[0].tx_hash)

            short_polygon = f"{polygon_address[:8]}...{polygon_address[-6:]}"
            short_polymarket = f"{polymarket_address[:8]}...{polymarket_address[-6:]}"

            await interaction.followup.send(
                f"Wallet Polygon **{name}** (`{short_polygon}`) lie!\n"
                f"Associe au wallet Polymarket: `{short_polymarket}`\n"
                f"Tu recevras les alertes des **depots et retraits USDC** dans ton dashboard.",
                ephemeral=True,
            )
            logger.info(f"User {interaction.user.id} linked polygon {polygon_address} to {polymarket_address}")

        except Exception as e:
            logger.error(f"Error linking polygon wallet: {e}")
            await interaction.followup.send(
                "Une erreur est survenue lors du lien du wallet.",
                ephemeral=True,
            )

    @app_commands.command(name="unlinkwallet", description="Retire un wallet Polygon lie")
    @app_commands.describe(polygon_address="L'adresse du wallet Polygon a retirer")
    async def unlinkwallet(self, interaction: Interaction, polygon_address: str) -> None:
        """Remove a linked polygon wallet."""
        if not is_valid_address(polygon_address):
            await interaction.response.send_message(
                "Adresse invalide. Format attendu: `0x` suivi de 40 caracteres hexadecimaux.",
                ephemeral=True,
            )
            return

        success = await self.db.unlink_polygon_wallet(polygon_address)

        if success:
            short_addr = f"{polygon_address[:8]}...{polygon_address[-6:]}"
            await interaction.response.send_message(
                f"Wallet Polygon `{short_addr}` retire.",
                ephemeral=True,
            )
            logger.info(f"User {interaction.user.id} unlinked polygon wallet {polygon_address}")
        else:
            await interaction.response.send_message(
                "Ce wallet Polygon n'est pas lie.",
                ephemeral=True,
            )

    @app_commands.command(name="mylinks", description="Affiche tes wallets Polygon lies")
    async def mylinks(self, interaction: Interaction) -> None:
        """Show all linked polygon wallets for the user's tracked polymarket wallets."""
        user_wallets = await self.db.get_user_wallets(interaction.user.id)

        if not user_wallets:
            await interaction.response.send_message(
                "Tu ne suis aucun wallet Polymarket.\n"
                "Utilise `/track` d'abord, puis `/linkwallet` pour lier un wallet Polygon.",
                ephemeral=True,
            )
            return

        embed = discord.Embed(
            title="Mes Wallets Polygon Lies",
            description="Wallets surveilles pour depots/retraits USDC",
            color=discord.Color.orange(),
            timestamp=datetime.utcnow(),
        )

        total_links = 0
        for wallet in user_wallets:
            linked = await self.db.get_linked_polygon_wallets(wallet.address)
            if linked:
                for pw in linked:
                    short_polygon = f"{pw.polygon_address[:6]}...{pw.polygon_address[-4:]}"
                    polygonscan_url = f"https://polygonscan.com/address/{pw.polygon_address}"
                    embed.add_field(
                        name=f"{pw.name}",
                        value=f"[`{short_polygon}`]({polygonscan_url})\n→ Lie a {wallet.name}",
                        inline=True,
                    )
                    total_links += 1

        if total_links == 0:
            await interaction.response.send_message(
                "Tu n'as aucun wallet Polygon lie.\n"
                "Utilise `/linkwallet <polygon> <polymarket> <nom>` pour en ajouter.",
                ephemeral=True,
            )
            return

        embed.set_footer(text=f"Total: {total_links} wallet(s) lie(s)")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="status", description="Affiche le statut du bot et du monitoring")
    async def status(self, interaction: Interaction) -> None:
        """Show bot status and monitoring info."""
        unique_wallets = await self.db.get_unique_wallets()
        polygon_wallets = await self.db.get_unique_polygon_wallets()
        crypto_wallets = await self.db.get_crypto_tracked_wallets()
        perf_stats = self.api.get_performance_stats()

        embed = discord.Embed(
            title="Statut du Bot",
            color=discord.Color.green() if self.monitoring_task.is_running() else discord.Color.red(),
            timestamp=datetime.utcnow(),
        )

        embed.add_field(
            name="Monitoring Polymarket",
            value="Actif" if self.monitoring_task.is_running() else "Inactif",
            inline=True,
        )
        embed.add_field(
            name="Monitoring On-Chain",
            value="Actif" if self.onchain_monitoring_task.is_running() else "Inactif",
            inline=True,
        )
        embed.add_field(
            name="Crypto WebSocket",
            value="Actif" if self.crypto_ws_service._running else "Inactif",
            inline=True,
        )
        embed.add_field(
            name="Intervalle (polling)",
            value=f"{POLLING_INTERVAL}s",
            inline=True,
        )
        embed.add_field(
            name="Wallets Polymarket",
            value=str(len(unique_wallets)),
            inline=True,
        )
        embed.add_field(
            name="Wallets Polygon",
            value=str(len(polygon_wallets)),
            inline=True,
        )
        embed.add_field(
            name="Wallets Crypto",
            value=str(len(crypto_wallets)),
            inline=True,
        )
        embed.add_field(
            name="API Calls",
            value=str(perf_stats["api_calls"]),
            inline=True,
        )
        embed.add_field(
            name="Cache Hit Rate",
            value=f"{perf_stats['cache_hit_rate']}%",
            inline=True,
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ==================== CRYPTO TRACKING (Real-time) ====================

    @app_commands.command(name="cryptotrack", description="Suivre un wallet sur les marches crypto 15min (BTC, ETH, XRP, SOL)")
    @app_commands.describe(
        address="L'adresse du wallet a suivre (format: 0x...)",
        name="Un nom pour identifier ce wallet",
    )
    async def cryptotrack(self, interaction: Interaction, address: str, name: str) -> None:
        """Add a wallet for real-time crypto 15min market tracking."""
        thread_id = await self.db.get_user_thread(interaction.user.id)
        if not thread_id:
            await interaction.response.send_message(
                "Utilise `/setup` d'abord pour creer ton dashboard.",
                ephemeral=True,
            )
            return

        if not is_valid_address(address):
            await interaction.response.send_message(
                "Adresse invalide. Format attendu: `0x` suivi de 40 caracteres hexadecimaux.",
                ephemeral=True,
            )
            return

        if len(name) > 32:
            await interaction.response.send_message(
                "Le nom du wallet ne peut pas depasser 32 caracteres.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            success = await self.db.add_crypto_tracking(interaction.user.id, address, name)

            if not success:
                await interaction.followup.send(
                    f"Tu suis deja ce wallet en crypto tracking!",
                    ephemeral=True,
                )
                return

            # Refresh WebSocket tracked wallets
            await self.crypto_ws_service.refresh_tracked_wallets()

            short_addr = f"{address[:8]}...{address[-6:]}"
            markets = ", ".join(CRYPTO_MARKETS.keys())

            await interaction.followup.send(
                f"**Crypto Tracking active!**\n\n"
                f"Wallet **{name}** (`{short_addr}`)\n\n"
                f"Marches surveilles: **{markets}** (15min Up/Down)\n"
                f"Tu recevras les alertes **en temps reel** via WebSocket!",
                ephemeral=True,
            )
            logger.info(f"User {interaction.user.id} now crypto-tracking {address} as '{name}'")

        except Exception as e:
            logger.error(f"Error adding crypto tracking: {e}")
            await interaction.followup.send(
                "Une erreur est survenue lors de l'ajout du wallet.",
                ephemeral=True,
            )

    @app_commands.command(name="cryptountrack", description="Retire un wallet du suivi crypto temps reel")
    @app_commands.describe(address="L'adresse du wallet a retirer")
    async def cryptountrack(self, interaction: Interaction, address: str) -> None:
        """Remove a wallet from crypto tracking."""
        if not is_valid_address(address):
            await interaction.response.send_message(
                "Adresse invalide. Format attendu: `0x` suivi de 40 caracteres hexadecimaux.",
                ephemeral=True,
            )
            return

        success = await self.db.remove_crypto_tracking(interaction.user.id, address)

        if success:
            # Refresh WebSocket tracked wallets
            await self.crypto_ws_service.refresh_tracked_wallets()

            short_addr = f"{address[:8]}...{address[-6:]}"
            await interaction.response.send_message(
                f"Wallet `{short_addr}` retire du crypto tracking.",
                ephemeral=True,
            )
            logger.info(f"User {interaction.user.id} stopped crypto-tracking {address}")
        else:
            await interaction.response.send_message(
                "Ce wallet n'est pas dans ta liste de crypto tracking.",
                ephemeral=True,
            )

    @app_commands.command(name="mycrypto", description="Affiche tes wallets suivis en crypto temps reel")
    async def mycrypto(self, interaction: Interaction) -> None:
        """List all wallets being crypto-tracked by the user."""
        wallets = await self.db.get_user_crypto_wallets(interaction.user.id)

        if not wallets:
            markets = ", ".join(CRYPTO_MARKETS.keys())
            await interaction.response.send_message(
                f"Tu ne suis aucun wallet en crypto tracking.\n"
                f"Utilise `/cryptotrack <address> <name>` pour suivre un wallet sur les marches {markets} 15min.",
                ephemeral=True,
            )
            return

        embed = discord.Embed(
            title="Mes Wallets Crypto Tracking",
            description="Wallets surveilles en temps reel (WebSocket)\nMarches: BTC, ETH, XRP, SOL (15min Up/Down)",
            color=discord.Color.gold(),
            timestamp=datetime.utcnow(),
        )

        for address, name in wallets:
            short_addr = f"{address[:6]}...{address[-4:]}"
            polygonscan_url = f"https://polygonscan.com/address/{address}"
            embed.add_field(
                name=name,
                value=f"[`{short_addr}`]({polygonscan_url})",
                inline=True,
            )

        embed.set_footer(text=f"Total: {len(wallets)} wallet(s) | WebSocket actif")
        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    """Setup function for loading the cog."""
    pass
