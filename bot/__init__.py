from os import path as ospath, mkdir, system, getenv
from logging import INFO, ERROR, FileHandler, StreamHandler, basicConfig, getLogger
from traceback import format_exc
from asyncio import Queue, Lock

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrogram import Client
from pyrogram.enums import ParseMode
from dotenv import load_dotenv
from uvloop import install

install()
basicConfig(format="[%(asctime)s] [%(name)s | %(levelname)s] - %(message)s [%(filename)s:%(lineno)d]",
            datefmt="%m/%d/%Y, %H:%M:%S %p",
            handlers=[FileHandler('log.txt'), StreamHandler()],
            level=INFO)

getLogger("pyrogram").setLevel(ERROR)
LOGS = getLogger(__name__)

load_dotenv('config.env')

ani_cache = {
    'fetch_animes': True,
    'ongoing': set(),
    'completed': set()
}
ffpids_cache = list()

ffLock = Lock()
ffQueue = Queue()
ff_queued = dict()

class Var:
    API_ID, API_HASH, BOT_TOKEN = getenv("API_ID"), getenv("API_HASH"), getenv("BOT_TOKEN")
    MONGO_URI = getenv("MONGO_URI")
    
    if not BOT_TOKEN or not API_HASH or not API_ID or not MONGO_URI:
        LOGS.critical('Important Variables Missing. Fill Up and Retry..!! Exiting Now...')
        exit(1)

    RSS_ITEMS = getenv("RSS_ITEMS", "https://subsplease.org/rss/?r=1080").split()
    FSUB_CHATS = list(map(int, getenv('FSUB_CHATS').split()))
    BACKUP_CHANNEL = getenv("BACKUP_CHANNEL") or ""
    MAIN_CHANNEL = int(getenv("MAIN_CHANNEL"))
    LOG_CHANNEL = int(getenv("LOG_CHANNEL") or 0)
    FILE_STORE = int(getenv("FILE_STORE"))
    ADMINS = list(map(int, getenv("ADMINS", "1047253913").split()))
    
    SEND_SCHEDULE = getenv("SEND_SCHEDULE", "False").lower() == "true"
    BRAND_UNAME = getenv("BRAND_UNAME", "@username")
  #  FFCODE_1080 = getenv("FFCODE_1080") or """ffmpeg -i '{}' -progress '{}' -preset veryfast -c:v libx264 -s 1920x1080 -pix_fmt yuv420p -crf 30 -c:a libopus -b:a 32k -c:s copy -map 0 -ac 2 -ab 32k -vbr 2 -level 3.1 '{}' -y"""
    FFCODE_1080 = getenv("FFCODE_1080") or """ffmpeg -i '{}' -progress '{}' -filter_complex "drawtext=text='Cultured Telugu Weeb':x=30:y=30:fontcolor=white@0.5:fontsize=40" -map 0:v -map 0:a -map 0:s -c:v libx265 -crf 26 -s 1920x1080 -b:v 500k -c:a libopus -b:a 64k -preset veryfast -x265-params "log-level=error" -metadata author='@Culturedteluguweeb' -metadata:s:s title='@Culturedteluguweeb' -metadata:s:a title='@Culturedteluguweeb' -metadata:s:v title='@Culturedteluguweeb' -y '{}'"""

    FFCODE_720 = getenv("FFCODE_720") or """ffmpeg -i '{}' -progress '{}' -filter_complex "scale=1280:720:force_original_aspect_ratio=decrease,drawtext=text='Cultured Telugu Weeb':x=30:y=30:fontcolor=white@0.5:fontsize=30:borderw=2:bordercolor=black@0.5" -map 0:v -map 0:a -map 0:s -c:v libx265 -crf 26 -preset veryfast -x265-params "log-level=error:ref=3:bframes=5:no-sao=1" -pix_fmt yuv420p -c:a libopus -b:a 48k -vbr on -c:s copy -metadata author='@Culturedteluguweeb' -metadata:s:s title='@Culturedteluguweeb' -metadata:s:a title='@Culturedteluguweeb' -metadata:s:v title='@Culturedteluguweeb' -y '{}'"""

    FFCODE_480 = getenv("FFCODE_480") or """ffmpeg -i '{}' -progress '{}' -filter_complex "scale=854:480:force_original_aspect_ratio=decrease,drawtext=text='Cultured Telugu Weeb':x=20:y=20:fontsize=24:fontcolor=white@0.7:box=1:boxcolor=black@0.4:shadowx=2:shadowy=2" -map 0:v -map 0:a -map 0:s -c:v libx265 -crf 26 -preset veryfast -x265-params "log-level=error:ref=3:bframes=3:no-sao=1:rc-lookahead=20" -pix_fmt yuv420p -c:a libopus -b:a 32k -vbr on -c:s copy -metadata author='@Culturedteluguweeb' -metadata:s:s title='@Culturedteluguweeb' -metadata:s:a title='@Culturedteluguweeb' -metadata:s:v title='@Culturedteluguweeb' -y '{}'"""

    QUALS = getenv("QUALS", "480 720 1080").split()

    
    AS_DOC = getenv("AS_DOC", "True").lower() == "true"
    THUMB = getenv("THUMB", "https://i.ibb.co/LzKKB5nL/x.jpg")
    AUTO_DEL = getenv("AUTO_DEL", "True").lower() == "true"
    DEL_TIMER = int(getenv("DEL_TIMER", "600"))
    START_PHOTO = getenv("START_PHOTO", "https://te.legra.ph/file/120de4dbad87fb20ab862.jpg")
    START_MSG = getenv("START_MSG", "<b>Hey {first_name}</b>,\n\n    <i>I am Auto Animes Store & Automater Encoder Build with ❤️ !!</i>")
    START_BUTTONS = getenv("START_BUTTONS", "UPDATES|https://telegram.me/Matiz_Tech SUPPORT|https://t.me/+p78fp4UzfNwzYzQ5")

if Var.THUMB and not ospath.exists("thumb.jpg"):
    system(f"wget -q {Var.THUMB} -O thumb.jpg")
    LOGS.info("Thumbnail has been Saved!!")
if not ospath.isdir("encode/"):
    mkdir("encode/")
if not ospath.isdir("thumbs/"):
    mkdir("thumbs/")
if not ospath.isdir("downloads/"):
    mkdir("downloads/")

try:
    bot = Client(name="AutoAniAdvance", api_id=Var.API_ID, api_hash=Var.API_HASH, bot_token=Var.BOT_TOKEN, plugins=dict(root="bot/modules"), parse_mode=ParseMode.HTML)
    bot_loop = bot.loop
    sch = AsyncIOScheduler(timezone="Asia/Kolkata", event_loop=bot_loop)
except Exception as ee:
    LOGS.error(str(ee))
    exit(1)
