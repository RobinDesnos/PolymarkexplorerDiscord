"""Script to get the Guild ID of the bot's first connected guild."""
import asyncio
import discord
from config import DISCORD_TOKEN

intents = discord.Intents.default()
client = discord.Client(intents=intents)

@client.event
async def on_ready():
    print(f"\nBot connected to {len(client.guilds)} guild(s):\n")
    for guild in client.guilds:
        print(f"  Guild Name: {guild.name}")
        print(f"  Guild ID: {guild.id}")
        print()
    print("Add this to your .env file:")
    if client.guilds:
        print(f"GUILD_ID={client.guilds[0].id}")
    await client.close()

asyncio.run(client.start(DISCORD_TOKEN))
