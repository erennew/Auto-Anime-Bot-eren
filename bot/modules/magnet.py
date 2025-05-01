import asyncio
import os
import time
import logging
import zipfile
from pathlib import Path
from io import BytesIO
from os import path as ospath, remove as osremove, makedirs
from typing import Optional, Dict, List, Callable
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, RPCError
from torrentp import TorrentDownloader
from bot import bot

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Ensure directories exist
makedirs("downloads", exist_ok=True)
makedirs("encoded", exist_ok=True)
makedirs("thumbnails", exist_ok=True)
makedirs("watermarks", exist_ok=True)

# Global session tracker
user_sessions: Dict[int, Dict] = {}
progress_messages: Dict[int, int] = {}

class VideoEncoder:
    """Handles video encoding with progress reporting"""
    @staticmethod
    async def encode_with_progress(
        input_path: str,
        output_path: str,
        quality: str,
        metadata: Dict[str, str],
        watermark_path: Optional[str] = None,
        thumbnail_path: Optional[str] = None,
        progress_callback: Optional[Callable[[float], None]] = None
    ) -> str:
        """Encode video with progress reporting"""
        try:
            if not ospath.exists(input_path):
                raise FileNotFoundError(f"Input file not found: {input_path}")
            
            # Normalize quality parameter
            quality = quality.lower()
            if quality.isdigit():  # Handle "480" -> "480p"
                quality = f"{quality}p"
            
            presets = {
                '480p': {'vf': 'scale=854:480', 'crf': 23, 'preset': 'fast'},
                '720p': {'vf': 'scale=1280:720', 'crf': 21, 'preset': 'medium'},
                '1080p': {'vf': 'scale=1920:1080', 'crf': 20, 'preset': 'slow'},
                'original': {}
            }
            
            if quality not in presets:
                raise ValueError(f"Invalid quality: {quality}. Must be one of {list(presets.keys())}")
            
            # Get video duration for progress calculation
            duration = await VideoEncoder.get_duration(input_path)
            
            # Build FFmpeg command
            cmd = [
                'ffmpeg',
                '-i', input_path,
                '-c:v', 'libx264',
                '-c:a', 'aac',
                '-b:a', '192k',
                '-movflags', '+faststart',
                '-metadata', f'title={metadata.get("title", "")}',
                '-metadata:s:v', f'title={metadata.get("video_title", "")}',
                '-metadata:s:a', f'title={metadata.get("audio_title", "")}',
                '-y',  # Overwrite output
                output_path
            ]
            
            # Add quality parameters if not original
            if quality != 'original':
                cmd[4:4] = [
                    '-vf', presets[quality]['vf'],
                    '-crf', str(presets[quality]['crf']),
                    '-preset', presets[quality]['preset']
                ]
            
            # Add watermark if exists
            if watermark_path and ospath.exists(watermark_path):
                cmd[4:4] = [
                    '-i', watermark_path,
                    '-filter_complex', '[0:v][1:v]overlay=main_w-overlay_w-10:main_h-overlay_h-10'
                ]
            
            # Start FFmpeg process
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Read progress from stderr
            while True:
                line = await process.stderr.readline()
                if not line:
                    break
                    
                line = line.decode('utf-8').strip()
                if "time=" in line:
                    time_str = line.split("time=")[1].split()[0]
                    # Convert HH:MM:SS.ms to seconds
                    h, m, s = time_str.split(':')
                    current_time = int(h)*3600 + int(m)*60 + float(s)
                    progress = min(99, (current_time / duration) * 100)
                    if progress_callback:
                        await progress_callback(progress)
            
            # Wait for completion
            await process.wait()
            if process.returncode != 0:
                error = await process.stderr.read()
                raise RuntimeError(f"FFmpeg error: {error.decode('utf-8')}")
            
            return output_path
        
        except Exception as e:
            logger.error(f"Encoding failed: {str(e)}")
            if ospath.exists(output_path):
                osremove(output_path)
            raise

    @staticmethod
    async def get_duration(input_path: str) -> float:
        """Get video duration using FFprobe"""
        cmd = [
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            input_path
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise RuntimeError(f"FFprobe error: {stderr.decode('utf-8')}")
        
        return float(stdout.decode('utf-8').strip())

async def update_progress(chat_id: int, text: str, force_new: bool = False):
    """Improved progress update that handles message not modified errors"""
    try:
        if chat_id in progress_messages and not force_new:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_messages[chat_id],
                    text=text
                )
                return
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except RPCError as e:
                if "MESSAGE_NOT_MODIFIED" not in str(e):
                    raise
        
        # Create new message if editing failed or forced
        msg = await bot.send_message(chat_id, text)
        progress_messages[chat_id] = msg.id
        
    except Exception as e:
        logger.error(f"Progress update failed: {str(e)}")

async def collect_settings(chat_id: int):
    """Collect all settings before download"""
    await bot.send_message(
        chat_id,
        "⚙️ Please configure all settings before download:",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Set Quality", callback_data=f"set_quality_{chat_id}"),
                InlineKeyboardButton("Set Title", callback_data=f"set_title_{chat_id}")
            ],
            [
                InlineKeyboardButton("Set Thumbnail", callback_data=f"set_thumb_{chat_id}"),
                InlineKeyboardButton("Set Watermark", callback_data=f"set_wm_{chat_id}")
            ],
            [InlineKeyboardButton("Start Download", callback_data=f"confirm_download_{chat_id}")]
        ])
    )

async def ask_for_quality(chat_id: int):
    """Ask user to select video quality with proper format"""
    await bot.send_message(
        chat_id,
        "🎚 Select video quality:",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("480p", callback_data=f"quality_480p_{chat_id}"),
                InlineKeyboardButton("720p", callback_data=f"quality_720p_{chat_id}")
            ],
            [
                InlineKeyboardButton("1080p", callback_data=f"quality_1080p_{chat_id}"),
                InlineKeyboardButton("Original", callback_data=f"quality_original_{chat_id}")
            ]
        ])
    )

async def handle_download(chat_id: int, magnet_link: str):
    """Simplified torrent download handler that starts encoding immediately after download"""
    try:
        if chat_id not in user_sessions or user_sessions[chat_id].get("status") != "ready_to_download":
            return await bot.send_message(chat_id, "❌ Settings not configured")
        
        download_path = ospath.join("downloads", f"dl_{chat_id}_{int(time.time())}")
        makedirs(download_path, exist_ok=True)
        
        await update_progress(chat_id, "⚡ Starting download...", force_new=True)

        # Initialize downloader
        downloader = TorrentDownloader(magnet_link, download_path)
        
        # Store download info
        user_sessions[chat_id].update({
            "status": "downloading",
            "download_path": download_path,
            "downloader": downloader,
            "start_time": time.time(),
            "last_update": time.time()
        })

        # Start download
        download_task = asyncio.create_task(downloader.start_download())

        # Track progress by file size only
        last_size = 0
        while True:
            await asyncio.sleep(5)
            
            try:
                current_size = sum(f.stat().st_size for f in Path(download_path).rglob('*') if f.is_file())
                
                # Force completion when download speed drops
                if current_size > 0 and (current_size - last_size) < (1024 * 1024):  # <1MB/s
                    if hasattr(downloader, 'stop'):
                        downloader.stop()
                    break
                
                # Update progress every 30 seconds
                if (time.time() - user_sessions[chat_id]["last_update"]) > 30:
                    speed = ((current_size - last_size) / 
                           (time.time() - user_sessions[chat_id]["last_update"]) / 1024)
                    
                    await update_progress(
                        chat_id,
                        f"📥 Downloading...\n"
                        f"Speed: {speed:.1f} KB/s\n"
                        f"Downloaded: {current_size/(1024*1024):.1f} MB"
                    )
                    last_size = current_size
                    user_sessions[chat_id]["last_update"] = time.time()
                    
            except Exception as e:
                logger.warning(f"Progress update error: {str(e)}")
                continue

        await download_task
        await update_progress(chat_id, "✅ Download complete! Starting processing...", force_new=True)
        
        # Get the largest downloaded file
        downloaded_files = [
            (f.stat().st_size, str(f)) 
            for f in Path(download_path).rglob('*') 
            if f.is_file() and not f.name.endswith('.tmp')
        ]
        
        if not downloaded_files:
            raise Exception("No valid files found after download")

        file_path = max(downloaded_files, key=lambda x: x[0])[1]
        user_sessions[chat_id].update({
            "file_path": file_path,
            "status": "downloaded"
        })
        
        # Immediately start processing
        await start_processing(chat_id)
        
    except Exception as e:
        logger.error(f"Download failed: {str(e)}")
        await update_progress(chat_id, f"❌ Download failed: {str(e)}", force_new=True)
        if chat_id in user_sessions:
            user_sessions.pop(chat_id)

async def start_processing(chat_id: int):
    """Handle encoding and uploading with real-time progress"""
    try:
        session = user_sessions.get(chat_id)
        if not session or session["status"] != "downloaded":
            return await update_progress(chat_id, "❌ No downloaded files found", force_new=True)
        
        file_path = session["file_path"]
        output_path = ospath.join("encoded", f"encoded_{ospath.basename(file_path)}")
        
        # Start encoding with progress tracking
        await update_progress(chat_id, "🔄 Starting video encoding...")
        
        async def progress_callback(progress: float):
            """Callback for FFmpeg progress"""
            await update_progress(
                chat_id,
                f"🔧 Encoding in progress...\n"
                f"Status: {progress:.1f}% complete\n"
                f"File: {ospath.basename(file_path)}"
            )
        
        try:
            # Run encoding with progress callback
            encoded_path = await VideoEncoder.encode_with_progress(
                input_path=file_path,
                output_path=output_path,
                quality=session.get("quality", "original"),
                metadata=session.get("metadata", {}),
                watermark_path=session.get("watermark"),
                thumbnail_path=session.get("thumbnail"),
                progress_callback=progress_callback
            )
            
            # Upload with progress
            await update_progress(chat_id, "☁️ Starting video upload...")
            await bot.send_video(
                chat_id=chat_id,
                video=encoded_path,
                thumb=session.get("thumbnail"),
                caption=f"🎬 {session.get('metadata', {}).get('title', 'Video')}",
                progress=lambda c, t: asyncio.create_task(
                    update_progress(chat_id, f"📤 Upload progress: {c/t*100:.1f}%")
                )
            )
            
        except Exception as e:
            raise Exception(f"Encoding failed: {str(e)}")
        finally:
            if ospath.exists(output_path):
                osremove(output_path)
        
        # Cleanup
        try:
            if ospath.exists(file_path):
                osremove(file_path)
            if "download_path" in session:
                for root, _, files in os.walk(session["download_path"]):
                    for file in files:
                        osremove(ospath.join(root, file))
                os.rmdir(session["download_path"])
        except Exception as e:
            logger.error(f"Cleanup error: {str(e)}")
        
        await update_progress(chat_id, "✅ Processing completed successfully!", force_new=True)
        
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        await update_progress(chat_id, f"❌ Processing failed: {str(e)}", force_new=True)
    finally:
        if chat_id in user_sessions:
            user_sessions.pop(chat_id)
        if chat_id in progress_messages:
            progress_messages.pop(chat_id)

@bot.on_message(filters.command("magnet") & filters.private)
async def magnet_handler(client: Client, message: Message):
    """Handle magnet link command"""
    chat_id = message.chat.id
    args = message.text.split(maxsplit=1)
    
    if len(args) < 2:
        return await message.reply("❗ Please provide a magnet link\nUsage: /magnet <magnet_uri>")
    
    magnet_link = args[1].strip()
    if not magnet_link.startswith("magnet:?"):
        return await message.reply("❌ Invalid magnet link format")
    
    user_sessions[chat_id] = {
        "magnet_link": magnet_link,
        "status": "configuring"
    }
    await collect_settings(chat_id)

@bot.on_callback_query(filters.regex(r"^set_quality_(\d+)$"))
async def quality_set_handler(client: Client, query):
    """Handle quality setting"""
    chat_id = int(query.data.split("_")[2])
    await ask_for_quality(chat_id)

@bot.on_callback_query(filters.regex(r"^quality_(\w+)_(\d+)$"))
async def quality_handler(client: Client, query):
    """Handle quality selection with proper format"""
    quality = query.data.split("_")[1]
    chat_id = int(query.data.split("_")[2])
    
    if chat_id not in user_sessions:
        return await query.answer("Session expired!", show_alert=True)
    
    # Normalize quality parameter
    if quality.isdigit():
        quality = f"{quality}p"
    
    valid_qualities = ["480p", "720p", "1080p", "original"]
    if quality not in valid_qualities:
        return await query.answer("Invalid quality selected!", show_alert=True)
    
    user_sessions[chat_id]["quality"] = quality
    await query.answer(f"Quality set to {quality}")
    await query.message.edit_text(f"✅ Quality: {quality}")
    await collect_settings(chat_id)

@bot.on_callback_query(filters.regex(r"^set_title_(\d+)$"))
async def set_title_handler(client: Client, query):
    """Prompt for title"""
    chat_id = int(query.data.split("_")[2])
    await query.answer("Send the title as text")
    user_sessions[chat_id]["awaiting"] = "title"
    await query.message.edit_text("📝 Please send the title as text")

@bot.on_callback_query(filters.regex(r"^set_thumb_(\d+)$"))
async def set_thumb_handler(client: Client, query):
    """Prompt for thumbnail"""
    chat_id = int(query.data.split("_")[2])
    await query.answer("Send the thumbnail as photo")
    user_sessions[chat_id]["awaiting"] = "thumbnail"
    await query.message.edit_text("🖼️ Please send the thumbnail as photo")

@bot.on_callback_query(filters.regex(r"^set_wm_(\d+)$"))
async def set_wm_handler(client: Client, query):
    """Prompt for watermark"""
    chat_id = int(query.data.split("_")[2])
    await query.answer("Send the watermark as photo")
    user_sessions[chat_id]["awaiting"] = "watermark"
    await query.message.edit_text("💧 Please send the watermark as photo")

@bot.on_callback_query(filters.regex(r"^confirm_download_(\d+)$"))
async def confirm_download_handler(client: Client, query):
    """Start download after settings"""
    chat_id = int(query.data.split("_")[2])
    
    if chat_id not in user_sessions or "magnet_link" not in user_sessions[chat_id]:
        return await query.answer("Session expired!", show_alert=True)
    
    user_sessions[chat_id]["status"] = "ready_to_download"
    await query.answer("Starting download...")
    await handle_download(chat_id, user_sessions[chat_id]["magnet_link"])

@bot.on_message(filters.private & (filters.text | filters.photo))
async def handle_user_input(client: Client, message: Message):
    """Handle user inputs for settings"""
    chat_id = message.chat.id
    
    if chat_id not in user_sessions or "awaiting" not in user_sessions[chat_id]:
        return
    
    if user_sessions[chat_id]["awaiting"] == "title" and message.text:
        if "metadata" not in user_sessions[chat_id]:
            user_sessions[chat_id]["metadata"] = {}
        user_sessions[chat_id]["metadata"]["title"] = message.text
        user_sessions[chat_id].pop("awaiting")
        await message.reply(f"✅ Title set: {message.text}")
        await collect_settings(chat_id)
    
    elif user_sessions[chat_id]["awaiting"] == "thumbnail" and message.photo:
        thumb_path = ospath.join("thumbnails", f"thumb_{chat_id}.jpg")
        await message.download(thumb_path)
        user_sessions[chat_id]["thumbnail"] = thumb_path
        user_sessions[chat_id].pop("awaiting")
        await message.reply("✅ Thumbnail saved!")
        await collect_settings(chat_id)
    
    elif user_sessions[chat_id]["awaiting"] == "watermark" and message.photo:
        wm_path = ospath.join("watermarks", f"wm_{chat_id}.png")
        await message.download(wm_path)
        user_sessions[chat_id]["watermark"] = wm_path
        user_sessions[chat_id].pop("awaiting")
        await message.reply("✅ Watermark saved!")
        await collect_settings(chat_id)

async def cleanup_temp_files():
    """Clean up old temporary files"""
    while True:
        await asyncio.sleep(3600)
        try:
            now = time.time()
            for folder in ["downloads", "encoded", "thumbnails", "watermarks"]:
                for file in os.listdir(folder):
                    file_path = ospath.join(folder, file)
                    if ospath.isfile(file_path) and now - ospath.getmtime(file_path) > 86400:
                        try:
                            osremove(file_path)
                        except:
                            pass
        except Exception as e:
            logger.error(f"Cleanup error: {str(e)}")

# Start cleanup task
asyncio.create_task(cleanup_temp_files())
