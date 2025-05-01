import asyncio
from os import path as ospath
from traceback import format_exc
from aiofiles.os import remove as aioremove
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from typing import Dict, Any, Tuple, Optional, Set

# Local imports
from bot import bot, bot_loop, Var, ani_cache
from bot.core.task_queue import (
    ffQueue,
    ffLock,
    ff_queued,
    add_task_to_queue,
    TaskPriority
)
from bot.core.tordownload import TorDownloader
from bot.core.database import db
from bot.core.func_utils import getfeed, encode, sendMessage, convertBytes
from bot.core.text_utils import TextEditor
from bot.core.ffencoder import FFEncoder
from bot.core.tguploader import TgUploader
from bot.core.reporter import rep

# Button formatting
btn_formatter = {
    '1080': '𝟭𝟬𝟴𝟬𝗽',
    '720': '𝟳𝟮𝟬𝗽',
    '480': '𝟰𝟴𝟬𝗽'
    
}

class AnimePostManager:
    """Handles anime post creation and updates"""
    def __init__(self):
        self.status_messages: Dict[int, Dict[str, Any]] = {}
        self.current_texts: Dict[int, str] = {}

    async def create_initial_post(self, title: str) -> Tuple[Message, Message]:
        """Create initial anime post and status message"""
        try:
            ani_info = TextEditor(title)
            await ani_info.load_anilist()
            
            post_msg = await bot.send_photo(
                Var.MAIN_CHANNEL,
                photo=await ani_info.get_poster(),
                caption=await ani_info.get_caption()
            )
            
            status_text = f"‣ <b>Anime Name :</b> <b><i>{title}</i></b>\n\n<i>Downloading...</i>"
            status_msg = await sendMessage(Var.MAIN_CHANNEL, status_text)
            
            self.status_messages[status_msg.id] = {
                'message': status_msg,
                'post': post_msg,
                'title': title,
                'buttons': []
            }
            self.current_texts[status_msg.id] = status_text
            
            await asyncio.sleep(1.5)
            return post_msg, status_msg
            
        except Exception as e:
            await rep.report(f"Failed to create initial post: {str(e)}", "error")
            raise

    async def update_status(self, post_id: int, status: str) -> None:
        """Update processing status"""
        if post_id not in self.status_messages:
            return
            
        post_data = self.status_messages[post_id]
        new_text = f"‣ <b>Anime Name :</b> <b><i>{post_data['title']}</i></b>\n\n{status}"
        
        if self.current_texts.get(post_id) == new_text:
            return
            
        try:
            await post_data['message'].edit_text(new_text)
            self.current_texts[post_id] = new_text
        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" not in str(e):
                await rep.report(f"Failed to update status: {str(e)}", "error")

    async def add_download_button(self, post_id: int, quality: str, file_link: str, file_size: int) -> None:
        """Add download button to post"""
        if post_id not in self.status_messages:
            return
            
        post_data = self.status_messages[post_id]
        btn_text = f"{btn_formatter[quality]} - {convertBytes(file_size)}"
        
        if post_data['buttons'] and len(post_data['buttons'][-1]) < 2:
            post_data['buttons'][-1].append(InlineKeyboardButton(btn_text, url=file_link))
        else:
            post_data['buttons'].append([InlineKeyboardButton(btn_text, url=file_link)])
        
        try:
            await post_data['post'].edit_reply_markup(
                reply_markup=InlineKeyboardMarkup(post_data['buttons'])
            )
        except Exception as e:
            await rep.report(f"Failed to add button: {str(e)}", "error")

    async def cleanup_post(self, post_id: int) -> None:
        """Cleanup after processing"""
        if post_id in self.status_messages:
            try:
                await self.status_messages[post_id]['message'].delete()
                del self.status_messages[post_id]
                if post_id in self.current_texts:
                    del self.current_texts[post_id]
            except Exception as e:
                await rep.report(f"Failed to cleanup post: {str(e)}", "error")

class AnimeProcessor:
    def __init__(self):
        self.active_tasks: Set[int] = set()
        self.post_manager = AnimePostManager()
        self._ready = False
        self.processed_torrents: Set[str] = set()
        self.ff_events: Dict[int, asyncio.Event] = {}

        # Initialize cache if not exists
        ani_cache.setdefault('processing', set())
        ani_cache.setdefault('ongoing', set())
        ani_cache.setdefault('completed', set())
        ani_cache.setdefault('fetch_animes', True)

    async def start(self):
        """Initialize processor"""
        self._ready = True
        await rep.report("Processor initialized", "info")

    async def fetch_animes(self):
        """Fetch new animes from RSS"""
        while True:
            try:
                if ani_cache.get('fetch_animes', True):
                    await rep.report("✨ Fetch Animes Started!", "info")
                    for link in Var.RSS_ITEMS:
                        try:
                            info = await getfeed(link, 0)
                            if not info:
                                continue

                            torrent_id = f"{info.title}_{info.link.split('/')[-1]}"
                            if torrent_id in self.processed_torrents:
                                continue

                            self.processed_torrents.add(torrent_id)
                            await self.process_anime(info.title, info.link)

                        except Exception as e:
                            await rep.report(f"RSS feed error ({link}): {str(e)}", "error")

                await asyncio.sleep(60)

            except Exception as e:
                await rep.report(f"Fetch animes error: {str(e)}", "error")
                await asyncio.sleep(60)

    async def process_anime(self, title: str, link: str, force: bool = False):
        """Process single anime"""
        task_id = hash((title, link))
        if task_id in self.active_tasks and not force:
            return

        self.active_tasks.add(task_id)
        post_id = None

        try:
            if "[Batch]" in title:
                await rep.report(f"⏭️ Skipped batch: {title}", "warning")
                return

            ani_info = TextEditor(title)
            await ani_info.load_anilist()
            ani_id, ep_no = ani_info.adata.get('id'), ani_info.pdata.get("episode_number")

            if ani_id not in ani_cache['ongoing']:
                ani_cache['ongoing'].add(ani_id)
            elif not force:
                return

            if not force and ani_id in ani_cache['completed']:
                return

            if force or not (await db.getAnime(ani_id)):
                # 1. Create post
                post_msg, status_msg = await self.post_manager.create_initial_post(title)
                post_id = status_msg.id

                # 2. Download
                await self.post_manager.update_status(post_id, "<i>Downloading...</i>")
                dl_path = await TorDownloader("./downloads").download(link, title)

                if not dl_path or not ospath.exists(dl_path):
                    await rep.report(f"❌ Download failed: {title}", "error")
                    await self.post_manager.cleanup_post(post_id)
                    return

                ff_event = asyncio.Event()
                self.ff_events[post_id] = ff_event

                # 3. Queue encoding
                await self.post_manager.update_status(post_id, "<i>Queued to Encode...</i>")
                await rep.report(f"📥 Added {title} to encode queue", "info")

                await add_task_to_queue(post_id, TaskPriority.HIGH)
                await ff_event.wait()

                async with ffLock:
                    for qual in Var.QUALS:
                        filename = await ani_info.get_upname(qual)
                        await self.post_manager.update_status(post_id, f"<i>Encoding {qual}p...</i>")

                        try:
                            out_path = await FFEncoder(status_msg, dl_path, filename, qual).start_encode()
                            if not out_path or not ospath.exists(out_path):
                                raise Exception("Encoding failed")

                            await rep.report(f"✅ {qual}p encoded: {title}", "info")
                            await self.post_manager.update_status(post_id, f"<i>Uploading {qual}p...</i>")

                            msg = await TgUploader(status_msg).upload(out_path, qual)

                            file_link = f"https://t.me/{(await bot.get_me()).username}?start={await encode('get-'+str(msg.id * abs(Var.FILE_STORE)))}"
                            await self.post_manager.add_download_button(
                                post_id,
                                qual,
                                file_link,
                                msg.document.file_size
                            )

                            await db.saveAnime(ani_id, ep_no, qual, post_id)
                            asyncio.create_task(self.handle_upload_cleanup(msg.id, out_path))

                        except Exception as e:
                            await rep.report(f"❌ {qual}p failed: {title}\n{str(e)}", "error")
                            continue

                await self.post_manager.cleanup_post(post_id)
                await aioremove(dl_path)
                ani_cache['completed'].add(ani_id)

        except Exception as e:
            await rep.report(f"💥 Error in {title}\n{format_exc()}", "error")
            if post_id:
                await self.post_manager.cleanup_post(post_id)
        finally:
            self.active_tasks.discard(task_id)
            if post_id in self.ff_events:
                del self.ff_events[post_id]

    async def handle_upload_cleanup(self, msg_id: int, out_path: str):
        """Handle post-upload tasks"""
        try:
            msg = await bot.get_messages(Var.FILE_STORE, message_ids=msg_id)
            if Var.BACKUP_CHANNEL:
                for chat_id in Var.BACKUP_CHANNEL.split():
                    await msg.copy(int(chat_id))
            await aioremove(out_path)
        except Exception as e:
            await rep.report(f"Upload cleanup error: {str(e)}", "error")

    async def process_queue(self):
        """Process the encoding queue"""
        while True:
            try:
                priority, task_coro = await ffQueue.get()
                try:
                    await task_coro()  # 🚀 Run the coroutine
                except Exception as e:
                    await rep.report(f"Queue task error: {e}", "error")
                ffQueue.task_done()
            except Exception as e:
                await rep.report(f"Queue processing error: {str(e)}", "error")
                await asyncio.sleep(5)

    async def monitor_queue(self):
        """Monitor queue health"""
        while True:
            await asyncio.sleep(300)
            qsize = ffQueue.qsize()
            if qsize > 5:
                await rep.report(f"Queue backlog: {qsize} items", "warning")

# Initialize processor
processor = AnimeProcessor()

async def process_anime(title: str, link: str, force: bool = False):
    """Public interface to process anime"""
    if not processor._ready:
        raise RuntimeError("Processor not ready")
    return await processor.process_anime(title, link, force)

async def initialize_processor():
    """Initialize processor"""
    await processor.start()
    asyncio.create_task(processor.fetch_animes())
    asyncio.create_task(processor.process_queue())
    asyncio.create_task(processor.monitor_queue())

__all__ = [
    'processor', 
    'process_anime', 
    'initialize_processor',
    'AnimeProcessor',
    'AnimePostManager'
]
