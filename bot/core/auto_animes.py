from asyncio import gather, create_task, sleep as asleep, Event
from asyncio.subprocess import PIPE
from os import path as ospath, makedirs
from aiofiles import open as aiopen
from aiofiles.os import remove as aioremove
from traceback import format_exc
from base64 import urlsafe_b64encode
from time import time
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from bot import bot, bot_loop, Var, ani_cache, ffQueue, ffLock, ff_queued
from .tordownload import TorDownloader
from .database import db
from .func_utils import getfeed, encode, editMessage, sendMessage
from .text_utils import TextEditor
from .ffencoder import FFEncoder
from .tguploader import TgUploader
from .reporter import rep

btn_formatter = {
    '1080':'🎬 𝟭𝟬𝟴𝟬𝗽', 
    '720':'🎥 𝟳𝟮𝟬𝗽',
    '480':'📺 𝟰𝟴𝟬𝗽'
}

async def fetch_animes():
    await rep.report("✨ Fetch Animes Started!", "info")
    while True:
        await asleep(60)
        if ani_cache['fetch_animes']:
            for link in Var.RSS_ITEMS:
                if (info := await getfeed(link, 0)):
                    bot_loop.create_task(get_animes(info.title, info.link))

async def get_animes(name, torrent, force=False):
    try:
        aniInfo = TextEditor(name)
        await aniInfo.load_anilist()
        ani_id, ep_no = aniInfo.adata.get('id'), aniInfo.pdata.get("episode_number")
        
        if ani_id not in ani_cache['ongoing']:
            ani_cache['ongoing'].add(ani_id)
        elif not force:
            return
            
        if not force and ani_id in ani_cache['completed']:
            return
            
        if force or (not (ani_data := await db.getAnime(ani_id)) \
            or (ani_data and not (qual_data := ani_data.get(ep_no))) \
            or (ani_data and qual_data and not all(qual for qual in qual_data.values())):

            if "[Batch]" in name:
                await rep.report(f"⏭️ Torrent Skipped!\n\n📛 {name}", "warning")
                return
            
            await rep.report(f"🆕 New Anime Found!\n\n🎬 {name}", "info")
            post_msg = await bot.send_photo(
                Var.MAIN_CHANNEL,
                photo=await aniInfo.get_poster(),
                caption=await aniInfo.get_caption()
            )
            
            await asleep(1.5)
            stat_msg = await sendMessage(Var.MAIN_CHANNEL, f"🌀 <b>Anime:</b> <i>{name}</i>\n\n⬇️ <i>Downloading...</i>")
            dl = await TorDownloader("./downloads").download(torrent, name)
            if not dl or not ospath.exists(dl):
                await rep.report(f"❌ Download Failed!\n\n📛 {name}", "error")
                await stat_msg.delete()
                return

            post_id = post_msg.id
            ffEvent = Event()
            ff_queued[post_id] = ffEvent
            if ffLock.locked():
                await editMessage(stat_msg, f"🌀 <b>Anime:</b> <i>{name}</i>\n\n⏳ <i>Queued for encoding...</i>")
                await rep.report("📥 Added to encode queue...", "info")
            await ffQueue.put(post_id)
            await ffEvent.wait()
            
            await ffLock.acquire()
            btns = []
            for qual in Var.QUALS:
                filename = await aniInfo.get_upname(qual)
                await editMessage(stat_msg, f"🌀 <b>Anime:</b> <i>{name}</i>\n\n⚙️ <i>Encoding {qual}p...</i>")
                
                await asleep(1.5)
                await rep.report(f"🔧 Starting {qual}p encode...", "info")
                try:
                    out_path = await FFEncoder(stat_msg, dl, filename, qual).start_encode()
                except Exception as e:
                    await rep.report(f"❌ Encode failed!\nError: {e}", "error")
                    await stat_msg.delete()
                    ffLock.release()
                    return
                await rep.report(f"✅ {qual}p encode complete!\n📤 Uploading...", "info")
                
                await editMessage(stat_msg, f"🌀 <b>Anime:</b> <i>{filename}</i>\n\n📤 <i>Uploading {qual}p...</i>")
                await asleep(1.5)
                try:
                    msg = await TgUploader(stat_msg).upload(out_path, qual)
                except Exception as e:
                    await rep.report(f"❌ Upload failed!\nError: {e}", "error")
                    await stat_msg.delete()
                    ffLock.release()
                    return
                await rep.report(f"✅ {qual}p upload complete!", "info")
                
                msg_id = msg.id
                link = f"https://telegram.me/{(await bot.get_me()).username}?start={await encode('get-'+str(msg_id * abs(Var.FILE_STORE))}"
                
                if post_msg:
                    btn_text = f"{btn_formatter[qual]} ({round(msg.document.file_size/(1024*1024), 1)}MB)"
                    if len(btns) != 0 and len(btns[-1]) == 1:
                        btns[-1].insert(1, InlineKeyboardButton(btn_text, url=link))
                    else:
                        btns.append([InlineKeyboardButton(btn_text, url=link)])
                    await editMessage(post_msg, post_msg.caption.html if post_msg.caption else "", InlineKeyboardMarkup(btns))
                    
                await db.saveAnime(ani_id, ep_no, qual, post_id)
                bot_loop.create_task(extra_utils(msg_id, out_path, name))
            ffLock.release()
            
            await stat_msg.delete()
            await aioremove(dl)
        ani_cache['completed'].add(ani_id)
    except Exception as error:
        await rep.report(f"💥 Critical Error!\n{format_exc()}", "error")

async def extra_utils(msg_id, out_path, anime_name):
    try:
        msg = await bot.get_messages(Var.FILE_STORE, message_ids=msg_id)

        # Backup to channels
        if Var.BACKUP_CHANNEL != 0:
            for chat_id in Var.BACKUP_CHANNEL.split():
                await msg.copy(int(chat_id))
        
        # Generate and send screenshots to log channel
        if hasattr(Var, 'LOG_CHANNEL') and Var.LOG_CHANNEL:
            ss_success = await generate_and_send_screenshots(msg_id, out_path, anime_name)
            if not ss_success:
                await rep.report("⚠️ Screenshot generation failed", "warning")
    except Exception as e:
        await rep.report(f"❌ Extra utils error: {e}", "error")

async def generate_and_send_screenshots(msg_id, video_path, anime_name):
    try:
        screenshot_dir = f"./screenshots/{msg_id}"
        makedirs(screenshot_dir, exist_ok=True)
        
        # Generate 5 high-quality screenshots
        cmd = (
            f"ffmpeg -i '{video_path}' -vf 'select=gt(scene\,0.4),scale=640:-1' -frames:v 5 "
            f"'{screenshot_dir}/ss_%02d.jpg' -y"
        )
        
        process = await asyncio.create_subprocess_shell(
            cmd, stdout=PIPE, stderr=PIPE
        )
        await process.communicate()
        
        if process.returncode != 0:
            return False
            
        # Send screenshots to log channel
        media_group = []
        for i in range(1, 6):
            ss_path = f"{screenshot_dir}/ss_{i:02d}.jpg"
            if ospath.exists(ss_path):
                media_group.append(InputMediaPhoto(ss_path))
        
        if media_group:
            await bot.send_media_group(
                Var.LOG_CHANNEL,
                media=media_group,
                caption=f"📸 Screenshots for: {anime_name}"
            )
            
        return True
        
    except Exception as error:
        await rep.report(f"❌ Screenshot error: {error}", "error")
        return False
