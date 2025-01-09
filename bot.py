import os
import csv
import time
import urllib.request
import asyncio
import re
import argparse
import shlex

import discord
from discord.ext import commands

import chara_db

TOKEN = os.getenv('BOT_TOKEN')

MAX_ID_PER_MSG = 2000 // (4 + 18)
MAX_CACHE_SIZE = 300
CSV_CACHE_SECOND = 10 * 60

HOUSAMO_SHEET = "https://docs.google.com/spreadsheets/d/1nr4k_-5DKjgyCX49gokg5iPpQ9a5l3d0lGrGnrnDR2U/gviz/tq?tqx=out:csv&sheet=Sheet1&headers=0"
LAH_SHEET = "https://docs.google.com/spreadsheets/d/1xKhpSCMeyATJMr6Ur8q_OcvYEMPXYRJoX5hfrNOwSKU/gviz/tq?tqx=out:csv&sheet=Sheet1&headers=0"
EIDOS_SHEET = "https://docs.google.com/spreadsheets/d/1ycqCALRsh2f6aoIeI4EjlFL5bZjVNlQerQfFLhsRems/gviz/tq?tqx=out:csv&sheet=Sheet1&headers=0"
YAM_SHEET = "https://docs.google.com/spreadsheets/d/1njU9KzbJyiGeqzB8T04_A0kwx-oMvnXAGlIBg3BT5CE/gviz/tq?tqx=out:csv&sheet=Sheet1&headers=0"


CONFIG = {
  # test
  487520756289503253: {
    #"disable": True,
    "channels": [844546489133170729, 844546594430779412],
    "sheet": HOUSAMO_SHEET,
    "filename": "housamo.csv",
    "db_filename": "test.db",
  },
  # samocord
  #280061796390404096: {
  #  "disable": True,
  #  "channels": [420080593217257487, 285588282522206208, 698643025236066426, 698701417971712030, 844714468390731777],
  #  "sheet": HOUSAMO_SHEET,
  #  "filename": "housamo.csv",
  #  "db_filename": "housamo.db",
  #},
  ## herocord
  #758267927362797568: {
  #  "disable": True,
  #  "channels": [758267927878828046, 758267927878828047, 758267927878828044, 758267927878828045, 844972742692700240],
  #  "sheet": LAH_SHEET,
  #  "filename": "lah.csv",
  #  "db_filename": "lah.db",
  #},
  ## eidos
  #904187573696618506: {
  #  "disable": True,
  #  "channels": [904207190561267794, 904519401699700746, 904224270945775648],
  #  "sheet": EIDOS_SHEET,
  #  "filename": "eidos.csv",
  #  "db_filename": "eidos.db",
  #},
  # yamcord
  #796603865318555659: {
  #  #"disable": True,
  #  # creative-nsfw, creative, other art, other nsfw art, housamo, lah
  #  "channels": [796607723034378271, 796843532932743218, 796843651438346280, 796607601026269184, 841201670129188875, 796607557614436352],
  #  "sheet": YAM_SHEET,
  #  "filename": "yam.csv",
  #}
}
for v in CONFIG.values():
  v["charaManager"] = chara_db.CharaManager(v["db_filename"])
  v["charaManager"].create_db()

DUPLICATE_PING_MSG = """Posted recently. Please search the Twitter/BlueSky URL in Discord without '?s=', '?t=' and 'mobile.' before posting.

Please also delete the post so that Discord search will show less duplicated search results.
"""

BLACKLISTED_HOST = [
  "https://e621.net/",
  "https://danbooru.donmai.us/",
]

BLACKLIST_MSG = """No pings as these sites are usually just repost from original author's social media. Please provide the actual source."""

def read_csv(filename):
  data = {}
  with open(filename, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    for row in reader:
      names = row[0].split("/")
      ids = row[1].split()
      for name in names:
        data[name.lower()] = ids

  return data

def read_raw_csv(filename):
  data = {}
  names = []
  first = True
  with open(filename, "r", encoding="utf-8", newline='') as f:
    reader = csv.reader(f)
    for row in reader:
      if first:
        first = False
        continue
      names.append(row[0])
      data[row[0]] = set(row[1].split())

  return data, names

def write_new_csv(filename, data, names):
  with open(filename, "w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f)
    for name in names:
      writer.writerow([name, " ".join(data[name])])

def merge():
  data1, names1 = read_raw_csv("backup.csv")
  data2, names2 = read_raw_csv("housamo.csv")
  assert names1 == names2
  for k in names1:
    data1[k].update(data2[k])
  write_new_csv("final.csv", data1, names1)
  import sys
  sys.exit(0)

class Cache:
  def __init__(self, maxsize):
    self.maxsize = maxsize
    self.queue = [] # circular queue
    self.qS = 0
    self.qE = 0
    self.bucket = set()
    self.lock = asyncio.Lock()

  def push(self, obj):
    if len(self.queue) < self.maxsize:
      self.queue.append(obj)
    else:
      self.queue[self.qE] = obj
    self.bucket.add(obj)
    self.qE = (self.qE + 1) % self.maxsize

  def pop(self):
    obj = self.queue[self.qS]
    self.qS = (self.qS + 1) % self.maxsize
    self.bucket.remove(obj)

  def has(self, obj):
    return obj in self.bucket

  async def hit(self, obj):
    async with self.lock:
      if self.has(obj):
        return True
      if len(self.bucket) == self.maxsize:
        self.pop()
      self.push(obj)
      return False


TWITTER_PATTERN = re.compile(r"^https\:\/\/(mobile\.|fx|vx)?(twitter|x|fixvx|fixupx)\.com\/\w+\/status\/(\d+)")
BSKY_PATTERN = re.compile(r"^https\:\/\/(bsky|bsyy|bskx|vbsky|cbsky|bskye|bskyx)\.app\/profile/((\w|\.)+\/post\/\w+)")

BOT_COMMAND_NAME = "$art-ping"

def get_twitter(tokens):
  ids = []

  for token in tokens:
    match = TWITTER_PATTERN.match(token)
    if match:
      ids.append(match.group(3))
      continue

    match = BSKY_PATTERN.match(token)
    if match:
      ids.append(match.group(2))

  return ids

def check_blacklisted_host(tokens):
  for token in tokens:
    for blacklist in BLACKLISTED_HOST:
      if token.startswith(blacklist):
        return True

  return False

class ErrorCatchingArgumentParser(argparse.ArgumentParser):

  def print_help(self, file=None):
    raise Exception(self.format_help())
  
  def print_usage(self, file=None):
    raise Exception(self.format_usage())

  def exit(self, status=0, message=None):
    if message:
      raise Exception(message)

  def error(self, message):
    if message:
      raise Exception(message)

async def handle_bot_message(msg: discord.Message):
  if not msg.content.startswith(BOT_COMMAND_NAME):
    return

  guild = CONFIG.get(msg.guild.id)
  if not guild:
    return

  charaManager: chara_db.CharaManager = guild["charaManager"]

  async def add(args):
    charaManager.add_user_to_chara(args.chara_name, str(msg.author.id))
    await msg.reply("Done")

  async def remove(args):
    charaManager.remove_user_to_chara(args.chara_name, str(msg.author.id))
    await msg.reply("Done")

  async def list_(args):
    names = charaManager.get_charas_for_user(str(msg.author.id))
    if len(names) == 0:
      await msg.reply("You are not in any ping list yet")
    else:
      names.sort()
      await msg.reply(", ".join(names))

  async def listall(args):
    names = charaManager.get_charas()
    names.sort()
    await msg.reply(", ".join(names))

  async def purge(args):
    await msg.reply("TODO")

  parser = ErrorCatchingArgumentParser(BOT_COMMAND_NAME, exit_on_error=False)
  subparsers = parser.add_subparsers(description="Manage art pings", required=True)

  parser_add = subparsers.add_parser('add', add_help=True, description="Add yourself to a character's ping list")
  parser_add.add_argument("chara_name")
  parser_add.set_defaults(func=add)

  parser_remove = subparsers.add_parser("remove", add_help=True, description="Remove yourself to a character's ping list")
  parser_remove.add_argument("chara_name")
  parser_remove.set_defaults(func=remove)

  parser_list = subparsers.add_parser("list", add_help=True, description="List all characters that you are in their ping list")
  parser_list.set_defaults(func=list_)

  parser_listall = subparsers.add_parser("list-all", add_help=True, description="List all characters available")
  parser_listall.set_defaults(func=listall)

  role = discord.utils.get(msg.guild.roles, name="art-ping-manager")
  if role in msg.author.roles:
    parser_purge = subparsers.add_parser("purge", add_help=True, description="Purge user ids that are not in server")
    parser_purge.set_defaults(func=purge)

  tokens = shlex.split(msg.content)

  try:
    args = parser.parse_args(tokens[1:])
    await args.func(args)
  except Exception as e:
    await msg.reply(f"```\n{str(e)}\n```")
    raise e
    return

class PingClient(discord.Client):
  #class PingClient(commands.Bot):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

  async def on_ready(self):
    await client.change_presence(activity=discord.Activity(
      type=discord.ActivityType.listening,
      name="art tags, see pinned msg in sfw art channel for details"))

    print(f"{client.user} {client.guilds}")
    self.userId = str(client.user.id)

  async def handle_find_userid(self, msg: discord.Message):
    content = msg.content

    # if bot is pinged in #bot channel
    if self.userId in content:
      await msg.reply(f"Your id is {msg.author.id}")

  def handle_convert(self, msg):
    print(repr(msg.content))

    rows = msg.content.split("\n")
    for row in rows:
      row = row.split("    ")
      names = row[1].split()
      ids = []
      self.names.append(row[0])
      for name in names:
        if name.startswith("<@!"):
          ids.append(name[3:-1])

      self.data[row[0]] = set(ids)
      write_new_csv("new.csv", self.data, self.names)

  def download_csv(self, sheet, filename, guild):
    now = time.time()
    if now - guild.get("last_update", 0) < CSV_CACHE_SECOND:
      return guild["data"]

    req = urllib.request.Request(sheet)
    with urllib.request.urlopen(req) as res:
      with open(filename, "wb") as f:
        f.write(res.read())

    guild["data"] = read_csv(filename)
    guild["last_update"] = now
 
  async def on_message(self, msg):
    #if msg.channel.name == "art-bot-management":
    #  self.handle_convert(msg)
    #  return
    #return

    if msg.author == client.user:
      return

    if msg.channel.name == "bot":
      await handle_bot_message(msg)
      return

    guild = CONFIG.get(msg.guild.id)
    if not guild or guild.get("disable"):
      return

    if msg.channel.id not in guild["channels"]:
      return

    self.download_csv(guild["sheet"], guild["filename"], guild)
    data = guild["data"]

    content = msg.content
    if '#' not in content:
      return

    tokens = content.split()

    ids = set()
    for t in tokens:
      if len(t) > 1 and t[0] == "#":
        t = t[1:].lower()
        ids.update(data.get(t, []))

    n = len(ids)
    if not n:
      return

    cache = guild.get("cache")
    if not cache:
      cache = Cache(MAX_CACHE_SIZE)
      guild["cache"] = cache

    twitter = get_twitter(tokens)
    for t in twitter:
      if await cache.hit(t):
        await msg.reply(DUPLICATE_PING_MSG, mention_author=False)
        return

    if check_blacklisted_host(tokens):
      await msg.reply(BLACKLIST_MSG, mention_author=False)
      return

    ids = list(ids)
    for i in range(n // MAX_ID_PER_MSG + 1):
      parts = ids[i*MAX_ID_PER_MSG : min((i+1)*MAX_ID_PER_MSG, n)]
      await msg.reply("".join(f"<@!{x}>" for x in parts), mention_author=False)


def register_commands(bot):
  @bot.command()
  async def listallchara(ctx):
    guild = CONFIG.get(ctx.guild.id)
    if not guild:
      pass
    charaManager = guild["charaManager"]
    await ctx.send(charaManager.getall())
    await ctx.send("hello")

  @bot.event
  async def on_ready():
    print(f'Logged in as {bot.user} (ID: {bot.user.id})')
    print('------')

  bot.run(TOKEN)

if __name__ == "__main__":
  import logging

  logging.basicConfig(level=logging.INFO)

  logger = logging.getLogger('discord')
  logger.setLevel(logging.INFO)
  handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='w')
  handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
  logger.addHandler(handler)

  intents = discord.Intents(messages=True, guilds=True)
  #bot = commands.Bot(command_prefix='$art-ping-', intents=intents)
  #register_commands(bot)

  client = PingClient(command_prefix='$art-ping-', intents=intents)
  #register_commands(client)
  #client.add_listener(client.on_message_handler, 'on_message')
  client.run(TOKEN)

