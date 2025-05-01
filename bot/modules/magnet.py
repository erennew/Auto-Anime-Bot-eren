import asyncio
import os
import time
import logging
import zipfile
from io import BytesIO
from os import path as ospath, remove as osremove, makedirs
from typing import Optional, Dict, List
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
    """Handles video encoding with FFmpeg"""
    @staticmethod
    async def encode(
        input_path: str,
        output_path: str,
        quality: str,
        metadata: Dict[str, str],
        watermark_path: Optional[str] = None,
        thumbnail_path: Optional[str] = None
    ) -> str:
        """Encode video with specified quality and metadata"""
        try:
            if not ospath.exists(input_path):
                raise FileNotFoundError(f"Input file not found: {input_path}")
            
            presets = {
                '480p': {'vf': 'scale=854:480', 'crf': 23, 'preset': 'fast'},
                '720p': {'vf': 'scale=1280:720', 'crf': 21, 'preset': 'medium'},
                '1080p': {'vf': 'scale=1920:1080', 'crf': 20, 'preset': 'slow'},
                'original': {}
            }
            
            if quality not in presets:
                raise ValueError(f"Invalid quality: {quality}")
            
            input_stream = ffmpeg.input(input_path)
            
            if watermark_path and ospath.exists(watermark_path):
                watermark = ffmpeg.input(watermark_path)
                input_stream = ffmpeg.overlay(
                    input_stream,
                    watermark,
                    x='main_w-overlay_w-10',
                    y='main_h-overlay_h-10'
                )
            
            output_args = {
                'c:v': 'libx264',
                'c:a': 'aac',
                'b:a': '192k',
                'movflags': '+faststart',
                'metadata': f"title={metadata.get('title', '')}",
                'metadata:s:v': f"title={metadata.get('video_title', '')}",
                'metadata:s:a': f"title={metadata.get('audio_title', '')}",
                'y': None
            }
            
            if quality != 'original':
                output_args.update({
                    'vf': presets[quality]['vf'],
                    'crf': presets[quality]['crf'],
                    'preset': presets[quality]['preset']
                })
            
            process = (
                input_stream
                .output(output_path, **output_args)
                .global_args('-progress', 'pipe:1')
                .run_async(pipe_stdout=True, pipe_stderr=True)
            )
            
            while True:
                line = process.stdout.readline().decode('utf-8').strip()
                if not line and process.poll() is not None:
                    break
                if "progress=end" in line:
                    break
            
            if process.returncode != 0:
                error = process.stderr.read().decode('utf-8')
                raise RuntimeError(f"FFmpeg error: {error}")
            
            return output_path
        
        except Exception as e:
            logger.error(f"Encoding failed: {str(e)}")
            if ospath.exists(output_path):
                osremove(output_path)
            raise

async def update_progress(chat_id: int, text: str):
    """Update or create progress message"""
    try:
        if chat_id in progress_messages:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=progress_messages[chat_id],
                text=text
            )
        else:
            msg = await bot.send_message(chat_id, text)
            progress_messages[chat_id] = msg.id
    except FloodWait as e:
        await asyncio.sleep(e.value)
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
    """Ask user to select video quality"""
    await bot.send_message(
        chat_id,
        "🎚 Select video quality:",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("480p", callback_data=f"quality_480_{chat_id}"),
                InlineKeyboardButton("720p", callback_data=f"quality_720_{chat_id}")
            ],
            [
                InlineKeyboardButton("1080p", callback_data=f"quality_1080_{chat_id}"),
                InlineKeyboardButton("Original", callback_data=f"quality_orig_{chat_id}")
            ]
        ])
    )

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
                # If message not modified, we'll create a new one below
        
        # Create new message if editing failed or forced
        msg = await bot.send_message(chat_id, text)
        progress_messages[chat_id] = msg.id
        
    except Exception as e:
        logger.error(f"Progress update failed: {str(e)}")

async def handle_download(chat_id: int, magnet_link: str):
    """Complete torrent download handler with proper progress tracking"""
    try:
        # Initialize download
        download_path = ospath.join("downloads", f"dl_{chat_id}_{int(time.time())}")
        makedirs(download_path, exist_ok=True)
        
        await update_progress(chat_id, "⚡ Starting download...", force_new=True)
        
        # Initialize downloader
        downloader = TorrentDownloader(magnet_link, download_path)
        user_sessions[chat_id] = {
            "status": "downloading",
            "start_time": time.time(),
            "download_path": download_path,
            "last_size": 0,
            "last_update": time.time(),
            "last_message": ""
        }
        
        # Start download
        download_task = asyncio.create_task(downloader.start_download())
        
        # Track progress
        while True:
            await asyncio.sleep(5)  # Check every 5 seconds
            
            try:
                # Get current status
                current_size = sum(
                    ospath.getsize(ospath.join(root, file))
                    for root, _, files in os.walk(download_path)
                    for file in files
                    if ospath.exists(ospath.join(root, file))
                )
                
                # Calculate progress
                total_size = getattr(downloader._torrent_info._info, "total_size", lambda: 1)()
                progress = min(100, (current_size / total_size) * 100) if total_size > 0 else 0
                
                # Calculate speed
                current_time = time.time()
                time_diff = current_time - user_sessions[chat_id]["last_update"]
                speed = ((current_size - user_sessions[chat_id]["last_size"]) / time_diff / 1024) if time_diff > 0 else 0
                
                # Prepare progress message
                progress_msg = (
                    f"📥 Downloading...\n"
                    f"Progress: {progress:.1f}%\n"
                    f"Speed: {speed:.1f} KB/s\n"
                    f"Downloaded: {current_size/(1024*1024):.1f} MB"
                )
                
                # Only update if message changed significantly
                if (progress_msg != user_sessions[chat_id]["last_message"] or 
                    (current_time - user_sessions[chat_id]["last_update"]) > 30):
                    
                    await update_progress(chat_id, progress_msg)
                    user_sessions[chat_id]["last_message"] = progress_msg
                    user_sessions[chat_id]["last_size"] = current_size
                    user_sessions[chat_id]["last_update"] = current_time
                
                # Check for completion
                if progress >= 99.9 and time_diff > 30 and speed < 10:  # Almost complete and speed dropped
                    break
                    
            except Exception as e:
                logger.warning(f"Progress update temporary error: {str(e)}")
                continue
        
        await download_task
        await update_progress(chat_id, "✅ Download complete! Starting processing...", force_new=True)
        
        # Get downloaded files
        downloaded_files = []
        for root, _, files in os.walk(download_path):
            for file in files:
                file_path = ospath.join(root, file)
                if ospath.getsize(file_path) > 0:
                    downloaded_files.append(file_path)
        
        if not downloaded_files:
            raise Exception("No valid files downloaded")
        
        # Handle single vs multiple files
        if len(downloaded_files) > 1:
            user_sessions[chat_id]["is_multi_file"] = True
            await update_progress(chat_id, "📦 Creating archive...")
            
            zip_path = ospath.join(download_path, f"download_{chat_id}.zip")
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                for file in downloaded_files:
                    zipf.write(file, ospath.basename(file))
            
            file_path = zip_path
        else:
            file_path = downloaded_files[0]
        
        user_sessions[chat_id].update({
            "file_path": file_path,
            "status": "downloaded"
        })
        
        await start_processing(chat_id)
        
    except Exception as e:
        logger.error(f"Download failed: {str(e)}")
        await update_progress(chat_id, f"❌ Download failed: {str(e)}", force_new=True)
        if chat_id in user_sessions:
            user_sessions.pop(chat_id)

async def start_processing(chat_id: int):
    """Handle encoding and uploading"""
    try:
        session = user_sessions.get(chat_id)
        if not session or session["status"] != "downloaded":
            return await update_progress(chat_id, "❌ No downloaded files found", force_new=True)
        
        if session.get("is_multi_file"):
            await update_progress(chat_id, "📤 Uploading archive...")
            await bot.send_document(
                chat_id=chat_id,
                document=session["file_path"],
                caption=session.get("metadata", {}).get("title", "Downloaded Files"),
                progress=lambda c, t: asyncio.create_task(
                    update_progress(chat_id, f"📤 Uploading... {c/t*100:.1f}%")
                )
            )
        else:
            await update_progress(chat_id, "🔄 Encoding video...")
            output_path = ospath.join("encoded", f"encoded_{ospath.basename(session['file_path'])}")
            
            encoded_path = await VideoEncoder.encode(
                input_path=session["file_path"],
                output_path=output_path,
                quality=session.get("quality", "original"),
                metadata=session.get("metadata", {}),
                watermark_path=session.get("watermark"),
                thumbnail_path=session.get("thumbnail")
            )
            
            await update_progress(chat_id, "☁️ Uploading video...")
            await bot.send_video(
                chat_id=chat_id,
                video=encoded_path,
                thumb=session.get("thumbnail"),
                caption=f"🎬 {session.get('metadata', {}).get('title', 'Video')}",
                progress=lambda c, t: asyncio.create_task(
                    update_progress(chat_id, f"☁️ Uploading... {c/t*100:.1f}%")
                )
            )
            osremove(encoded_path)
        
        # Cleanup
        try:
            if ospath.exists(session["file_path"]):
                if session.get("is_multi_file"):
                    for root, _, files in os.walk(session["download_path"]):
                        for file in files:
                            osremove(ospath.join(root, file))
                    os.rmdir(session["download_path"])
                else:
                    osremove(session["file_path"])
            
            for file_type in ["thumbnail", "watermark"]:
                if file_type in session and ospath.exists(session[file_type]):
                    osremove(session[file_type])
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
    """Handle quality selection"""
    quality = query.data.split("_")[1]
    chat_id = int(query.data.split("_")[2])
    
    if chat_id not in user_sessions:
        return await query.answer("Session expired!", show_alert=True)
    
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
