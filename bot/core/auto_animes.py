from asyncio import gather, create_task, sleep as asleep, Event
from asyncio.subprocess import PIPE
from os import path as ospath
from aiofiles import open as aiopen
from aiofiles.os import remove as aioremove
from traceback import format_exc
from time import time
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from bot import bot, bot_loop, Var, ani_cache, ffQueue, ffLock, ff_queued
from .tordownload import TorDownloader
from .database import db
from .func_utils import getfeed, encode, editMessage, sendMessage, convertBytes
from .text_utils import TextEditor
from .ffencoder import FFEncoder
from .tguploader import TgUploader
from .reporter import rep

btn_formatter = {
    '1080':'𝟭𝟬𝟴𝟬𝗽', 
    '720':'𝟳𝟮𝟬𝗽',
    '480':'𝟰𝟴𝟬𝗽'
}

# Track processed torrents
processed_torrents = set()

async def fetch_animes():
    """Fetch new anime from RSS feeds with duplicate checking"""
    await rep.report("Fetch Animes Started!", "info")
    while True:
        await asleep(60)
        if not ani_cache.get('fetch_animes', True):
            continue
            
        for link in Var.RSS_ITEMS:
            try:
                if not (info := await getfeed(link, 0)):
                    continue
                    
                # Create unique identifier for this torrent
                torrent_id = f"{info.title}_{info.link.split('/')[-1]}"
                
                # Skip if already processed
                if torrent_id in processed_torrents:
                    continue
                    
                processed_torrents.add(torrent_id)
                bot_loop.create_task(get_animes(info.title, info.link))
                
            except Exception as e:
                await rep.report(f"RSS feed error ({link}): {str(e)}", "error")

async def get_animes(name, torrent, force=False):
    """Process anime from torrent download to upload"""
    try:
        # Create unique episode identifier
        episode_id = f"{name}_{torrent.split('/')[-1]}"
        
        # Skip if already processing or completed
        if episode_id in ani_cache['processing'] and not force:
            return
        ani_cache['processing'].add(episode_id)
        
        aniInfo = TextEditor(name)
        await aniInfo.load_anilist()
        ani_id, ep_no = aniInfo.adata.get('id'), aniInfo.pdata.get("episode_number")
        
        # Skip if already in ongoing or completed
        if ani_id in ani_cache['ongoing'] and not force:
            return
        ani_cache['ongoing'].add(ani_id)
        
        if ani_id in ani_cache['completed'] and not force:
            return

        # Check if needs processing
        if force or (not (ani_data := await db.getAnime(ani_id)) \
            or (ani_data and not (qual_data := ani_data.get(ep_no))) \
            or (ani_data and qual_data and not all(qual for qual in qual_data.values()))):
            
            if "[Batch]" in name:
                await rep.report(f"Batch torrent skipped: {name}", "warning")
                return
            
            await rep.report(f"New anime found: {name}", "info")
            
            # Create post
            post_msg = await bot.send_photo(
                Var.MAIN_CHANNEL,
                photo=await aniInfo.get_poster(),
                caption=await aniInfo.get_caption()
            )
            
            await asleep(1.5)
            stat_msg = await sendMessage(
                Var.MAIN_CHANNEL, 
                f"‣ <b>Anime Name:</b> <b><i>{name}</i></b>\n\n<i>Downloading...</i>"
            )
            
            # Download torrent
            dl = await TorDownloader("./downloads").download(torrent, name)
            if not dl or not ospath.exists(dl):
                await rep.report(f"Download failed: {name}", "error")
                await stat_msg.delete()
                return

            # Add to encode queue
            post_id = post_msg.id
            ffEvent = Event()
            ff_queued[post_id] = ffEvent
            
            if ffLock.locked():
                await editMessage(
                    stat_msg, 
                    f"‣ <b>Anime Name:</b> <b><i>{name}</i></b>\n\n<i>Queued for encoding...</i>"
                )
                await rep.report("Added to encode queue", "info")
                
            await ffQueue.put(post_id)
            await ffEvent.wait()
            
            # Process each quality
            async with ffLock:
                btns = []
                for qual in Var.QUALS:
                    filename = await aniInfo.get_upname(qual)
                    await editMessage(
                        stat_msg,
                        f"‣ <b>Anime Name:</b> <b><i>{name}</i></b>\n\n<i>Encoding {qual}p...</i>"
                    )
                    
                    await asleep(1.5)
                    await rep.report(f"Starting {qual}p encode", "info")
                    
                    try:
                        out_path = await FFEncoder(stat_msg, dl, filename, qual).start_encode()
                    except Exception as e:
                        await rep.report(f"{qual}p encode failed: {str(e)}", "error")
                        continue
                        
                    await rep.report(f"{qual}p encode complete, uploading...", "info")
                    
                    await editMessage(
                        stat_msg,
                        f"‣ <b>Anime Name:</b> <b><i>{filename}</i></b>\n\n<i>Uploading {qual}p...</i>"
                    )
                    
                    await asleep(1.5)
                    try:
                        msg = await TgUploader(stat_msg).upload(out_path, qual)
                    except Exception as e:
                        await rep.report(f"{qual}p upload failed: {str(e)}", "error")
                        continue
                        
                    await rep.report(f"{qual}p upload complete", "info")
                    
                    # Generate download link
                    msg_id = msg.id
                    link = f"https://telegram.me/{(await bot.get_me()).username}?start={await encode('get-'+str(msg_id * abs(Var.FILE_STORE)))}"
                    
                    # Update buttons
                    if post_msg:
                        btn_text = f"{btn_formatter[qual]} - {convertBytes(msg.document.file_size)}"
                        if btns and len(btns[-1]) < 2:
                            btns[-1].append(InlineKeyboardButton(btn_text, url=link))
                        else:
                            btns.append([InlineKeyboardButton(btn_text, url=link)])
                            
                        await editMessage(
                            post_msg,
                            post_msg.caption.html if post_msg.caption else "",
                            InlineKeyboardMarkup(btns)
                        )
                    
                    await db.saveAnime(ani_id, ep_no, qual, post_id)
                    bot_loop.create_task(extra_utils(msg_id, out_path))
            
            # Cleanup
            await stat_msg.delete()
            await aioremove(dl)
            ani_cache['completed'].add(ani_id)
            
    except Exception as error:
        await rep.report(f"Error in get_animes: {str(error)}", "error")
    finally:
        ani_cache['processing'].discard(episode_id)

async def extra_utils(msg_id, out_path):
    """Handle additional utilities like backups"""
    try:
        msg = await bot.get_messages(Var.FILE_STORE, message_ids=msg_id)

        # Backup to channels
        if getattr(Var, 'BACKUP_CHANNEL', 0) != 0:
            for chat_id in str(Var.BACKUP_CHANNEL).split():
                try:
                    await msg.copy(int(chat_id))
                except Exception as e:
                    await rep.report(f"Backup failed for {chat_id}: {str(e)}", "warning")
                    
    except Exception as e:
        await rep.report(f"Extra utils error: {str(e)}", "error")
