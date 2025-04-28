import asyncio
from os import path as ospath
from traceback import format_exc
from collections import deque
from aiofiles.os import remove as aioremove
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from asyncio import sleep as asleep

from bot import bot, bot_loop, Var, ani_cache, ffQueue, ffLock, ff_queued
from .tordownload import TorDownloader
from .database import db
from .func_utils import getfeed, encode, sendMessage, handle_logs
from .text_utils import TextEditor
from .ffencoder import FFEncoder
from .tguploader import TgUploader
from .reporter import rep
from .task_queue import task_queue, TaskPriority

# Format for buttons
btn_formatter = {
    '1080': '🎬 𝟭𝟬𝟴𝟬𝗽',
    '720': '🎥 𝟳𝟮𝟬𝗽',
    '480': '📺 𝟰𝟴𝟬𝗽'
}

class AnimePostManager:
    """Handles all anime post creation and updates"""
    def __init__(self):
        self.status_messages = {}

    async def create_initial_post(self, title: str) -> Message:
        """Create the initial processing post"""
        try:
            ani_info = TextEditor(title)
            await ani_info.load_anilist()
            
            post_msg = await bot.send_message(
                Var.MAIN_CHANNEL,
                f"🌀 <b>Anime:</b> <i>{title}</i>\n\n⬇️ <i>Downloading...</i>"
            )
            
            self.status_messages[post_msg.id] = {
                'message': post_msg,
                'title': title,
                'buttons': []
            }
            return post_msg
            
        except Exception as e:
            await rep.report(f"Failed to create initial post: {str(e)}", "error")
            raise

    async def update_status(self, post_id: int, status: str) -> None:
        """Update the processing status"""
        if post_id not in self.status_messages:
            return
            
        post_data = self.status_messages[post_id]
        try:
            await post_data['message'].edit_text(
                f"🌀 <b>Anime:</b> <i>{post_data['title']}</i>\n\n{status}"
            )
        except Exception as e:
            await rep.report(f"Failed to update status: {str(e)}", "error")

    async def add_download_button(self, post_id: int, quality: str, file_link: str, file_size: float) -> None:
        """Add download button to the post"""
        if post_id not in self.status_messages:
            return
            
        post_data = self.status_messages[post_id]
        btn_text = f"{btn_formatter[quality]} ({round(file_size / (1024 * 1024), 1)}MB)"
        
        # Add new button or append to existing row
        buttons = post_data['buttons']
        if buttons and len(buttons[-1]) < 2:
            buttons[-1].append(InlineKeyboardButton(btn_text, url=file_link))
        else:
            buttons.append([InlineKeyboardButton(btn_text, url=file_link)])
        
        try:
            await post_data['message'].edit_reply_markup(
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        except Exception as e:
            await rep.report(f"Failed to add button: {str(e)}", "error")

    async def cleanup_post(self, post_id: int) -> None:
        """Clean up the post after processing"""
        if post_id in self.status_messages:
            try:
                await self.status_messages[post_id]['message'].delete()
                del self.status_messages[post_id]
            except Exception as e:
                await rep.report(f"Failed to cleanup post: {str(e)}", "error")

class AnimeProcessor:
    def __init__(self):
        self.active_tasks = set()
        self.post_manager = AnimePostManager()
        
        # Initialize ani_cache with default values
        ani_cache.setdefault('processing', set())
        ani_cache.setdefault('ongoing', set())
        ani_cache.setdefault('completed', set())
        ani_cache.setdefault('fetch_animes', True)

        # Task management
        self.task_lock = asyncio.Lock()
        self.processed_torrents = set()
        self.ffEvent = asyncio.Event()

    async def fetch_animes(self):
        """Fetch new animes from RSS feeds"""
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
                            await rep.report(f"📥 Adding task for {info.title} to queue", "info")
                            
                            priority = (
                                TaskPriority.HIGH 
                                if "special" in info.title.lower() 
                                else TaskPriority.MEDIUM
                            )
                            
                            await self.add_task(
                                lambda: self.process_anime(info.title, info.link), 
                                priority
                            )
                        except Exception as e:
                            await rep.report(f"RSS feed error ({link}): {str(e)}", "error")
                
                await asleep(60)  # Check every minute
                
            except Exception as e:
                await rep.report(f"Fetch animes error: {str(e)}", "error")
                await asleep(60)

    async def process_anime(self, title, link):
        """Process a single anime from download to upload"""
        task_id = hash((title, link))
        if task_id in self.active_tasks:
            return
            
        self.active_tasks.add(task_id)
        
        try:
            if "[Batch]" in title:
                await rep.report(f"⏭️ Skipped batch: {title}", "warning")
                return
                
            # Create initial post
            post_msg = await self.post_manager.create_initial_post(title)
            post_id = post_msg.id

            # Download torrent
            await self.post_manager.update_status(post_id, "⬇️ <i>Downloading...</i>")
            dl_path = await TorDownloader("./downloads").download(link, title)
            
            if not dl_path or not ospath.exists(dl_path):
                await rep.report(f"❌ Download failed: {title}", "error")
                await self.post_manager.cleanup_post(post_id)
                return

            # Wait for encoder availability
            if ffLock.locked():
                await self.post_manager.update_status(post_id, "⏳ <i>Queued for encoding...</i>")
                await rep.report("📥 Added to encode queue", "info")

            await ffQueue.put(post_id)
            self.ffEvent.clear()
            await self.ffEvent.wait()

            # Process encoding
            async with ffLock:
                for qual in Var.QUALS:
                    filename = f"{title.replace(' ', '_')}_{qual}p.mkv"
                    
                    await self.post_manager.update_status(post_id, f"⚙️ <i>Encoding {qual}p...</i>")

                    try:
                        # Encode video
                        encoder = FFEncoder(post_msg, dl_path, filename, qual)
                        out_path = await encoder.start_encode()
                        
                        if not out_path or not ospath.exists(out_path):
                            raise Exception("Encoding failed - no output file")
                            
                        await rep.report(f"✅ {qual}p encoded: {title}", "info")

                        # Upload
                        await self.post_manager.update_status(post_id, f"📤 <i>Uploading {qual}p...</i>")
                        uploader = TgUploader(post_msg)
                        msg = await uploader.upload(out_path, qual)
                        
                        # Add download button
                        file_link = f"https://telegram.me/{(await bot.get_me()).username}?start={await encode('get-' + str(msg.id * abs(Var.FILE_STORE)))}"
                        await self.post_manager.add_download_button(
                            post_id, 
                            qual, 
                            file_link, 
                            msg.document.file_size
                        )

                        # Save to database
                        anime_id = abs(hash(title + link)) % 100000
                        await db.saveAnime(anime_id, 1, qual, post_id)
                        
                        # Handle logs
                        bot_loop.create_task(handle_logs(msg.id, out_path, title))

                    except Exception as e:
                        await rep.report(f"❌ {qual}p failed: {title}\nError: {str(e)}", "error")
                        continue

                # Cleanup
                await self.post_manager.cleanup_post(post_id)
                await aioremove(dl_path)
                ani_cache['completed'].add(anime_id)
                
        except Exception as e:
            await rep.report(f"💥 Critical error: {title}\n{format_exc()}", "error")
        finally:
            self.ffEvent.set()
            self.active_tasks.discard(task_id)

    # ... (rest of the class remains the same)
    
    async def run_workers(self, num_workers=3):
        """Run multiple anime processing workers"""
        workers = [self.worker() for _ in range(num_workers)]
        await asyncio.gather(*workers)

    async def worker(self):
        """Process tasks from the queue"""
        while True:
            task = await task_queue.get_next_task()
            if task:
                try:
                    await task()
                except Exception as e:
                    await rep.report(f"Worker error: {str(e)}", "error")
            else:
                await asleep(1)

# Initialize and start the processor
processor = AnimeProcessor()

async def main():
    # Start the RSS fetcher and workers
    await processor.add_task(processor.fetch_animes, TaskPriority.HIGH)
    await processor.run_workers()

# Start the main loop
bot_loop.create_task(main())
