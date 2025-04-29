import asyncio
import os
import time
import tracemalloc
import ffmpeg
from os import path as ospath, remove as osremove, makedirs
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import RPCError, FloodWait, MessageNotModified
from torrentp import TorrentDownloader
from bot import bot

# Initialize tracemalloc
tracemalloc.start()

# Ensure directories exist
makedirs("downloads", exist_ok=True)
makedirs("thumbnails", exist_ok=True)
makedirs("watermarks", exist_ok=True)
makedirs("encoded", exist_ok=True)

# Global trackers
user_data = {}
progress_messages = {}
current_stage = {}

class LOGS:
    @staticmethod
    def info(msg): print(f"[INFO] {msg}")
    @staticmethod
    def warning(msg): print(f"[WARNING] {msg}")
    @staticmethod
    def error(msg): print(f"[ERROR] {msg}")

def convert_bytes(size):
    """Convert bytes to human-readable format"""
    power = 2**10
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"

def convert_seconds(seconds):
    """Convert seconds to human-readable time"""
    seconds = int(seconds)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days > 0: return f"{days}d {hours}h {minutes}m"
    elif hours > 0: return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0: return f"{minutes}m {seconds}s"
    else: return f"{seconds}s"

async def send_message(chat_id, text, reply_markup=None, **kwargs):
    """Send message with error handling"""
    try:
        if not text.strip(): raise ValueError("Empty message text")
        return await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
            **kwargs
        )
    except FloodWait as e:
        await asyncio.sleep(e.value + 2)
        return await send_message(chat_id, text, reply_markup, **kwargs)
    except Exception as e:
        LOGS.error(f"Send message error: {e}")
        raise

async def edit_message(chat_id, message_id, text, reply_markup=None):
    """Edit message with error handling"""
    try:
        if not text.strip(): raise ValueError("Empty message text")
        return await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(e.value + 2)
        return await edit_message(chat_id, message_id, text, reply_markup)
    except Exception as e:
        LOGS.error(f"Edit message error: {e}")
        raise

class EnhancedTorrentDownloader(TorrentDownloader):
    """Improved torrent downloader with progress tracking"""
    def __init__(self, magnet_link, download_path):
        super().__init__(magnet_link, download_path)
        self._is_complete = False
        self._progress = 0
        self._download_rate = 0
        self._eta = 0

    async def start(self):
        """Start download with progress tracking"""
        await self.start_download()
        timeout = 3600 * 3  # 3 hour timeout
        start_time = time.time()

        while True:
            await asyncio.sleep(2)
            try:
                self._progress = self.progress
                self._download_rate = self.download_rate
                self._eta = self.eta
                self._is_complete = self._progress >= 100
                
                if self._is_complete:
                    LOGS.info(f"Download completed: {self._progress}%")
                    break
                
                if time.time() - start_time > timeout:
                    raise TimeoutError("Download timeout")
                    
            except Exception as e:
                LOGS.error(f"Progress error: {e}")
                continue

        return ospath.join(self.download_path, self._torrent_info._info.name())

    @property
    def is_complete(self): return self._is_complete
    @property
    def progress(self): return self._progress
    @property
    def download_rate(self): return self._download_rate
    @property
    def eta(self): return self._eta

class ProgressTracker:
    """Handles progress message updates"""
    @staticmethod
    async def update(chat_id, stage, percent, speed="", eta=""):
        bars = "▓" * int(percent/10) + "░" * (10 - int(percent/10))
        text = f"🚀 **{stage.upper()}**\n{bars} {percent}%\n⚡ {speed} | ⏱️ {eta}"
        
        if chat_id in progress_messages:
            try:
                await edit_message(chat_id, progress_messages[chat_id], text)
            except Exception as e:
                LOGS.error(f"Failed to update progress: {e}")
                try:
                    msg = await send_message(chat_id, text)
                    progress_messages[chat_id] = msg.id
                except Exception as e:
                    LOGS.error(f"Failed to send new progress message: {e}")
        else:
            try:
                msg = await send_message(chat_id, text)
                progress_messages[chat_id] = msg.id
            except Exception as e:
                LOGS.error(f"Failed to send initial progress message: {e}")

async def encode_video(chat_id, file_path):
    """Encode video with metadata and watermark"""
    data = user_data[chat_id]
    output_path = ospath.join("encoded", f"encoded_{ospath.basename(file_path)}")
    
    try:
        probe = ffmpeg.probe(file_path)
        duration = float(probe['format']['duration'])
        
        quality = data.get("quality", "720p")
        if quality == "480p":
            vcodec, crf, preset = "libx264", 23, "fast"
            resolution, bitrate = "854x480", "1000k"
        elif quality == "720p":
            vcodec, crf, preset = "libx264", 21, "medium"
            resolution, bitrate = "1280x720", "2500k"
        elif quality == "1080p":
            vcodec, crf, preset = "libx264", 20, "slow"
            resolution, bitrate = "1920x1080", "5000k"
        else:
            vcodec, crf, preset = "copy", None, None
            resolution, bitrate = None, None

        input_stream = ffmpeg.input(file_path)
        kwargs = {
            'c:v': vcodec,
            'preset': preset,
            'crf': crf,
            'b:v': bitrate,
            's': resolution,
            'c:a': 'aac',
            'b:a': '192k',
            'metadata': f"title={data.get('video_title', '')}",
            'metadata:s:v': f"title={data.get('video_title', '')}",
            'metadata:s:a': f"title={data.get('audio_title', '')}",
            'movflags': '+faststart',
            'y': None
        }
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        if 'watermark' in data and ospath.exists(data['watermark']):
            watermark = ffmpeg.input(data['watermark'])
            input_stream = ffmpeg.overlay(
                input_stream,
                watermark,
                x='main_w-overlay_w-10',
                y='main_h-overlay_h-10'
            )

        process = (
            input_stream
            .output(output_path, **kwargs)
            .global_args('-progress', 'pipe:1')
            .run_async(pipe_stdout=True, pipe_stderr=True)
        )

        start_time = time.time()
        while True:
            line = process.stdout.readline().decode('utf-8').strip()
            if not line and process.poll() is not None:
                break
                
            if line.startswith('out_time='):
                current_time = float(line.split('=')[1])
                percent = (current_time / duration) * 100
                elapsed = time.time() - start_time
                speed = current_time / elapsed if elapsed > 0 else 0
                eta = (duration - current_time) / speed if speed > 0 else 0
                
                await ProgressTracker.update(
                    chat_id,
                    "ENCODING",
                    min(100, int(percent)),
                    f"{speed:.2f}x",
                    convert_seconds(eta)
                )

        if process.returncode != 0:
            error = process.stderr.read().decode('utf-8')
            raise Exception(f"FFmpeg error: {error}")
        
        return output_path
        
    except Exception as e:
        LOGS.error(f"Encoding error: {e}")
        if ospath.exists(output_path):
            try:
                osremove(output_path)
            except:
                pass
        raise

async def handle_upload(chat_id, file_path):
    """Upload file with progress tracking"""
    if not ospath.exists(file_path):
        raise Exception("File not found for upload")
    
    file_size = ospath.getsize(file_path)
    start_time = time.time()
    last_update = 0
    
    async def progress(current, total):
        nonlocal last_update
        current_time = time.time()
        if current_time - last_update < 5 and current < total:
            return
            
        percent = (current / total) * 100
        elapsed = current_time - start_time
        speed = convert_bytes(current / elapsed) + "/s" if elapsed > 0 else "0 B/s"
        eta = convert_seconds((total - current) / (current / elapsed)) if current > 0 else "Calculating..."
        
        await ProgressTracker.update(
            chat_id,
            "UPLOADING",
            int(percent),
            speed,
            eta
        )
        last_update = current_time
    
    try:
        thumb = None
        if 'thumbnail' in user_data[chat_id] and ospath.exists(user_data[chat_id]['thumbnail']):
            thumb = user_data[chat_id]['thumbnail']
        
        await bot.send_document(
            chat_id=chat_id,
            document=file_path,
            thumb=thumb,
            progress=progress,
            caption=f"📁 {ospath.basename(file_path)}"
        )
    except Exception as e:
        LOGS.error(f"Upload failed: {e}")
        raise

@bot.on_message(filters.command("magnet") & filters.private)
async def magnet_handler(_, message: Message):
    """Handle magnet link command"""
    chat_id = message.chat.id
    args = message.text.split(maxsplit=1)
    
    if len(args) < 2:
        return await message.reply("❗ Usage: /magnet <magnet-link>")
    
    # Store user data and start download immediately
    user_data[chat_id] = {
        "link": args[1],
        "state": "downloading",
        "video_title": "Video Stream",
        "audio_title": "Audio Stream",
        "subtitle_title": "Subtitles"
    }
    
    await message.reply("⚡ Starting download...")
    try:
        await ProgressTracker.update(chat_id, "DOWNLOADING", 0)
        downloader = EnhancedTorrentDownloader(
            user_data[chat_id]["link"],
            "downloads"
        )
        file_path = await downloader.start()
        
        # Store downloaded file path
        user_data[chat_id]["downloaded_path"] = file_path
        user_data[chat_id]["state"] = "ready_for_processing"
        
        await send_message(
            chat_id,
            "🎬 Download complete! Select Quality and click Start Processing when ready",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("480p", callback_data="qual_480"),
                 InlineKeyboardButton("720p", callback_data="qual_720")],
                [InlineKeyboardButton("1080p", callback_data="qual_1080"),
                 InlineKeyboardButton("Original", callback_data="qual_orig")],
                [InlineKeyboardButton("⚡ Start Processing", callback_data="start_now")]
            ])
        )
    except Exception as e:
        await message.reply(f"❌ Download failed: {str(e)}")
        user_data.pop(chat_id, None)

@bot.on_callback_query(filters.regex("^qual_"))
async def quality_handler(_, query):
    """Handle quality selection"""
    chat_id = query.message.chat.id
    quality = query.data.split("_")[1]
    
    if chat_id not in user_data:
        await query.answer("Session expired!")
        return
    
    user_data[chat_id]["quality"] = quality
    await query.answer(f"Quality set to {quality}")
    await edit_message(
        chat_id,
        query.message.id,
        "🎬 Ready to process!\n"
        f"Quality: {quality}\n"
        "Click 'Start Processing' when ready",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⚡ Start Processing", callback_data="start_now")]
        ])
    )

@bot.on_callback_query(filters.regex("^start_now"))
async def start_processing(_, query):
    """Start encoding and upload process"""
    chat_id = query.message.chat.id
    await query.answer("Starting processing...")
    
    if chat_id not in user_data or user_data[chat_id].get("state") != "ready_for_processing":
        await query.message.edit_text("Session expired or not ready for processing!")
        return
    
    try:
        file_path = user_data[chat_id]["downloaded_path"]
        
        # 1. ENCODING PHASE
        if user_data[chat_id].get("quality", "original") != "original":
            await ProgressTracker.update(chat_id, "ENCODING", 0)
            output_path = await encode_video(chat_id, file_path)
            try:
                osremove(file_path)
            except Exception as e:
                LOGS.error(f"Error deleting original file: {e}")
        else:
            output_path = file_path
        
        # 2. UPLOAD PHASE
        await ProgressTracker.update(chat_id, "UPLOADING", 0)
        await handle_upload(chat_id, output_path)
        
        # 3. CLEANUP
        files_to_clean = [
            output_path,
            user_data[chat_id].get("thumbnail"),
            user_data[chat_id].get("watermark")
        ]
        
        for file_path in files_to_clean:
            if file_path and ospath.exists(file_path):
                try:
                    osremove(file_path)
                except Exception as e:
                    LOGS.error(f"Error deleting {file_path}: {e}")
        
        await send_message(chat_id, "✅ Processing completed successfully!")
        
    except Exception as e:
        LOGS.error(f"Processing error: {e}")
        await send_message(chat_id, f"❌ Processing failed: {str(e)}")
        
    finally:
        # Clear session data
        user_data.pop(chat_id, None)
        progress_messages.pop(chat_id, None)

# Other handlers (thumbnail, watermark, metadata settings) remain the same as in previous versions
