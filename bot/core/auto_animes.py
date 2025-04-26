from asyncio import Semaphore, Lock, gather, create_task, sleep as asleep, Event
from asyncio.subprocess import PIPE, create_subprocess_shell
from os import path as ospath, makedirs
from aiofiles import open as aiopen
from aiofiles.os import remove as aioremove
from traceback import format_exc
from time import time
from datetime import datetime
from psutil import cpu_percent, virtual_memory
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto

from bot import bot, bot_loop, Var, ani_cache, ffQueue, ffLock, ff_queued
from .tordownload import TorDownloader
from .database import db
from .func_utils import getfeed, encode, editMessage, sendMessage
from .text_utils import TextEditor
from .ffencoder import FFEncoder
from .tguploader import TgUploader
from .reporter import rep

# Configuration
btn_formatter = {
    '1080': '🎬 𝟭𝟬𝟴𝟬𝗽',
    '720': '🎥 𝟳𝟮𝟬𝗽',
    '480': '📺 𝟰𝟴𝟬𝗽'
}

# Safety Limits
SAFETY = {
    'MAX_CPU': 80,          # Stop if CPU > 80%
    'MAX_RAM': 90,          # Stop if RAM > 90%
    'FFMPEG_TIMEOUT': 21600, # 6 hours max per encode
    'COOLDOWN': 300         # 5 minute wait if overloaded
}

# Global Control
PROCESS_LOCK = Semaphore(1)  # One anime at a time
SCREENSHOT_LOCK = Lock()     # Prevent screenshot collisions
processed_torrents = set()   # Track processed torrents

# Initialize cache
for key in ['processing', 'ongoing', 'completed', 'fetch_animes']:
    if not hasattr(ani_cache, key):
        setattr(ani_cache, key, set())

async def safe_encode(encoder, input_path, output_path, qual):
    """Protected FFmpeg encoding with resource monitoring"""
    start_time = datetime.now()
    attempts = 0
    
    while attempts < 3:
        # System check
        if (cpu_percent() > SAFETY['MAX_CPU'] or 
            virtual_memory().percent > SAFETY['MAX_RAM']):
            await asleep(SAFETY['COOLDOWN'])
            continue
            
        # Timeout check
        if (datetime.now() - start_time).seconds > SAFETY['FFMPEG_TIMEOUT']:
            raise TimeoutError(f"Encoding exceeded {SAFETY['FFMPEG_TIMEOUT']}s timeout")

        try:
            return await encoder.encode(input_path, output_path, qual)
        except Exception as e:
            attempts += 1
            if "ffmpeg" in str(e).lower():
                await asleep(60)
                continue
            raise

async def generate_screenshots(video_path, anime_name, msg_id):
    """Thread-safe screenshot generation"""
    async with SCREENSHOT_LOCK:
        screenshot_dir = f"./screenshots/{msg_id}"
        makedirs(screenshot_dir, exist_ok=True)
        media_group = []
        
        try:
            # Generate 5 screenshots from key moments
            timestamps = ["00:10:00", "00:20:00", "00:30:00", "00:40:00", "00:50:00"]
            
            for i, timestamp in enumerate(timestamps, 1):
                ss_path = f"{screenshot_dir}/ss_{i}.jpg"
                cmd = f"ffmpeg -ss {timestamp} -i '{video_path}' -vframes 1 -q:v 2 '{ss_path}' -y"
                
                proc = await create_subprocess_shell(
                    cmd, stdout=PIPE, stderr=PIPE
                )
                await proc.communicate()
                
                if ospath.exists(ss_path):
                    async with aiopen(ss_path, 'rb') as f:
                        media = InputMediaPhoto(
                            await f.read(),
                            caption=f"📸 {anime_name}" if i == 1 else ""
                        )
                        media_group.append(media)
                    await aioremove(ss_path)
            
            if media_group and hasattr(Var, 'LOG_CHANNEL'):
                await bot.send_media_group(Var.LOG_CHANNEL, media=media_group)
                return True
                
        except Exception as e:
            await rep.report(f"📸 Screenshot Error: {str(e)}", "error")
        finally:
            if ospath.exists(screenshot_dir):
                await aioremove(screenshot_dir)
        return False

async def fetch_animes():
    """Fetch new anime from RSS feeds with duplicate prevention"""
    await rep.report("✨ Fetch Animes Started!", "info")
    while True:
        await asleep(60)
        if ani_cache.fetch_animes:
            for link in Var.RSS_ITEMS:
                try:
                    if not (info := await getfeed(link, 0)):
                        continue
                        
                    torrent_id = f"{info.title}_{info.link.split('/')[-1]}"
                    if torrent_id in processed_torrents:
                        continue
                        
                    processed_torrents.add(torrent_id)
                    bot_loop.create_task(get_animes(info.title, info.link))
                    
                except Exception as e:
                    await rep.report(f"RSS feed error ({link}): {str(e)}", "error")

async def get_animes(name, torrent, force=False):
    """Main processing pipeline with safety features"""
    async with PROCESS_LOCK:
        episode_id = None
        dl_path = None
        encoded_files = {}
        
        try:
            # System health check
            if (cpu_percent() > SAFETY['MAX_CPU'] or 
                virtual_memory().percent > SAFETY['MAX_RAM']):
                await asleep(SAFETY['COOLDOWN'])
                return await get_animes(name, torrent, force)

            # Initialize anime info
            aniInfo = TextEditor(name)
            await aniInfo.load_anilist()
            ani_id = aniInfo.adata.get('id')
            ep_no = aniInfo.pdata.get("episode_number", 1)
            episode_id = f"{ani_id}_{ep_no}"

            if episode_id in ani_cache.processing and not force:
                return

            ani_cache.processing.add(episode_id)
            await rep.report(f"🆕 New Anime: {name}", "info")

            # Create initial post
            post_msg = await bot.send_photo(
                Var.MAIN_CHANNEL,
                photo=await aniInfo.get_poster(),
                caption=await aniInfo.get_caption()
            )
            
            # Download with progress
            stat_msg = await sendMessage(
                Var.MAIN_CHANNEL,
                f"🌀 <b>Anime:</b> <i>{name}</i>\n\n⬇️ <i>Downloading...</i>\n▱▱▱▱▱▱▱▱▱▱ 0%"
            )

            dl = TorDownloader("./downloads")
            dl_path = await dl.download(
                torrent,
                name,
                progress_callback=lambda p: bot_loop.create_task(
                    editMessage(
                        stat_msg,
                        f"🌀 <b>Anime:</b> <i>{name}</i>\n\n⬇️ <i>Downloading...</i>\n"
                        f"{'▰' * (p // 10)}{'▱' * (10 - (p // 10))} {p}%"
                    )
                )
            )

            if not dl_path or not ospath.exists(dl_path):
                await editMessage(stat_msg, "❌ Download failed!")
                return

            # Encoding queue
            post_id = post_msg.id
            ffEvent = Event()
            ff_queued[post_id] = ffEvent
            
            if ffLock.locked():
                await editMessage(stat_msg, f"🌀 <b>Anime:</b> <i>{name}</i>\n\n⏳ <i>Queued for encoding...</i>")
                await rep.report("📥 Added to encode queue...", "info")
                
            await ffQueue.put(post_id)
            await ffEvent.wait()
            
            # Process each quality
            async with ffLock:
                btns = []
                for qual in Var.QUALS:
                    filename = await aniInfo.get_upname(qual)
                    await editMessage(stat_msg, f"🌀 <b>Anime:</b> <i>{name}</i>\n\n⚙️ <i>Encoding {qual}p...</i>")
                    
                    try:
                        # Safe encoding
                        encoder = FFEncoder(stat_msg, dl_path, filename, qual)
                        out_path = await safe_encode(encoder, dl_path, f"encoded/{filename}", qual)
                        encoded_files[qual] = out_path
                        
                        # Upload
                        await editMessage(stat_msg, f"🌀 <b>Anime:</b> <i>{filename}</i>\n\n📤 <i>Uploading {qual}p...</i>")
                        msg = await TgUploader(stat_msg).upload(out_path, qual)
                        
                        # Create button
                        msg_id = msg.id
                        link = f"https://telegram.me/{Var.BRAND_UNAME.replace('@', '')}?start={await encode('get-'+str(msg_id * abs(Var.FILE_STORE)))}"
                        btn_text = f"{btn_formatter[qual]} ({round(msg.document.file_size/(1024*1024), 1)}MB)"
                        
                        if btns and len(btns[-1]) < 2:
                            btns[-1].append(InlineKeyboardButton(btn_text, url=link))
                        else:
                            btns.append([InlineKeyboardButton(btn_text, url=link)])

                        await db.saveAnime(ani_id, ep_no, qual, post_id)

                        # Generate screenshots for 1080p
                        if qual == '1080' and hasattr(Var, 'LOG_CHANNEL'):
                            await generate_screenshots(out_path, name, msg_id)
                            
                    except Exception as e:
                        await rep.report(f"❌ {qual}p failed: {str(e)}", "error")
                        if ospath.exists(out_path):
                            await aioremove(out_path)
                        continue

            # Final update
            await editMessage(
                stat_msg,
                f"✅ <b>Completed:</b> <i>{name}</i>\n"
                f"⚡ Quality: {'/'.join(encoded_files.keys())}p",
                reply_markup=InlineKeyboardMarkup(btns) if btns else None
            )

        except Exception as error:
            await rep.report(f"💥 Pipeline crashed: {format_exc()}", "critical")
            
            # Emergency cleanup
            if dl_path and ospath.exists(dl_path):
                await aioremove(dl_path)
            for path in encoded_files.values():
                if ospath.exists(path):
                    await aioremove(path)
                    
            # System recovery
            if cpu_percent() > 70:
                await asleep(SAFETY['COOLDOWN'])
                
        finally:
            if episode_id:
                ani_cache.processing.discard(episode_id)
            if dl_path and ospath.exists(dl_path):
                await aioremove(dl_path)
            for path in encoded_files.values():
                if ospath.exists(path):
                    await aioremove(path)

async def extra_utils(msg_id, out_path, anime_name):
    """Handle backups and cleanup"""
    try:
        msg = await bot.get_messages(Var.FILE_STORE, msg_id)

        # Backup to channels
        if getattr(Var, 'BACKUP_CHANNEL', 0) != 0:
            for chat_id in str(Var.BACKUP_CHANNEL).split():
                try:
                    await msg.copy(int(chat_id))
                except Exception as e:
                    await rep.report(f"❌ Backup failed for {chat_id}: {e}", "warning")
        
    except Exception as e:
        await rep.report(f"❌ Extra utils error: {e}", "error")
