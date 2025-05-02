import asyncio
import os
import time
import logging
import shutil
from pathlib import Path
from os import path as ospath, remove as osremove, makedirs
from typing import Optional, Dict, Callable, Tuple
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, RPCError
from torrentp import TorrentDownloader
from datetime import datetime

# Bot configuration
API_ID = 24500584
API_HASH = "449da69cf4081dc2cc74eea828d0c490"
BOT_TOKEN = "1599848664:AAHc75il2BECWK39tiPv4pVf-gZdPt4MFcw"
MAX_CONCURRENT_TASKS = 3
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB

# Initialize bot
bot = Client(
    "video_encoder_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=100,
    max_concurrent_transmissions=5
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("encoder_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Ensure directories exist
for folder in ["downloads", "encoded", "thumbnails", "watermarks", "logs"]:
    makedirs(folder, exist_ok=True)

# Global session tracker
user_sessions: Dict[int, Dict] = {}
progress_messages: Dict[int, int] = {}
active_tasks: Dict[int, asyncio.Task] = {}
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

class VideoEncoder:
    """Enhanced video encoder with HEVC to H.264 conversion"""
    
    @staticmethod
    async def get_video_info(input_path: str) -> Tuple[float, Tuple[int, int]]:
        """Get video duration and resolution with robust error handling"""
        try:
            # First try to get duration and resolution together
            cmd = [
                'ffprobe',
                '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=width,height,duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                input_path
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                info = stdout.decode().strip().split('\n')
                if len(info) >= 3:
                    width, height, duration = info[:3]
                    return float(duration), (int(width), int(height))
            
            # Fallback to separate commands if combined fails
            duration_cmd = [
                'ffprobe',
                '-v', 'error',
                '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                input_path
            ]
            
            duration_process = await asyncio.create_subprocess_exec(
                *duration_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            duration_out, _ = await duration_process.communicate()
            duration = float(duration_out.decode().strip()) if duration_process.returncode == 0 else 0
            
            # Get resolution separately
            res_cmd = [
                'ffprobe',
                '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=width,height',
                '-of', 'csv=p=0',
                input_path
            ]
            
            res_process = await asyncio.create_subprocess_exec(
                *res_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            res_out, _ = await res_process.communicate()
            if res_process.returncode == 0:
                try:
                    width, height = res_out.decode().strip().split(',')
                    return duration, (int(width), int(height))
                except:
                    pass
            
            return duration, (0, 0)
            
        except Exception as e:
            logger.error(f"Video info detection failed: {str(e)}")
            return 0.0, (0, 0)

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
        """Convert HEVC to H.264 with proper handling"""
        async with semaphore:
            try:
                # Validate input
                if not ospath.exists(input_path):
                    raise FileNotFoundError(f"Input file not found: {input_path}")
                
                if ospath.getsize(input_path) > MAX_FILE_SIZE:
                    raise ValueError(f"File too large (>{MAX_FILE_SIZE/1024/1024}MB)")
                
                # Get video info with fallback
                duration, (width, height) = await VideoEncoder.get_video_info(input_path)
                if duration <= 0:
                    logger.warning("Could not determine video duration, using fallback method")
                    duration = 3600  # Default 1 hour if duration can't be determined
                
                # Quality presets for H.264 output
                presets = {
                    '480p': {'height': 480, 'crf': 23, 'preset': 'fast'},
                    '720p': {'height': 720, 'crf': 21, 'preset': 'medium'},
                    '1080p': {'height': 1080, 'crf': 20, 'preset': 'slow'},
                    'original': {'crf': 20, 'preset': 'medium'}
                }
                
                if quality not in presets:
                    raise ValueError(f"Invalid quality: {quality}")
                
                # Base FFmpeg command for H.264 conversion
                cmd = [
                    'ffmpeg',
                    '-hide_banner',
                    '-loglevel', 'error',
                    '-i', input_path,
                    '-c:v', 'libx264',  # Force H.264 codec
                    '-pix_fmt', 'yuv420p',  # Ensure 8-bit output
                    '-movflags', '+faststart',
                    '-c:a', 'aac',
                    '-b:a', '192k',
                    '-metadata', f'title={metadata.get("title", "")}',
                    '-y',
                    output_path
                ]
                
                # Apply quality settings
                if quality != 'original':
                    cmd[4:4] = [
                        '-vf', f'scale=-2:{presets[quality]["height"]}',
                        '-crf', str(presets[quality]["crf"]),
                        '-preset', presets[quality]["preset"]
                    ]
                else:
                    # For original quality, just ensure proper conversion
                    cmd[4:4] = [
                        '-crf', str(presets[quality]["crf"]),
                        '-preset', presets[quality]["preset"]
                    ]
                
                # Add watermark if provided
                if watermark_path and ospath.exists(watermark_path):
                    cmd[4:4] = [
                        '-i', watermark_path,
                        '-filter_complex', '[0:v][1:v]overlay=W-w-10:H-h-10[out]',
                        '-map', '[out]',
                        '-map', '0:a?'
                    ]
                else:
                    # Ensure proper mapping when no watermark
                    cmd[4:4] = ['-map', '0']
                
                logger.info(f"Executing FFmpeg command: {' '.join(cmd)}")
                
                # Start encoding process
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                # Progress tracking
                start_time = time.time()
                last_progress = 0
                stderr_lines = []
                
                while True:
                    line = await process.stderr.readline()
                    if not line:
                        break
                    
                    line = line.decode('utf-8').strip()
                    stderr_lines.append(line)
                    
                    # Improved progress parsing
                    if "time=" in line:
                        try:
                            time_str = line.split("time=")[1].split()[0]
                            h, m, s = time_str.split(':')
                            current_time = int(h)*3600 + int(m)*60 + float(s)
                            progress = min(99, (current_time / duration) * 100)
                            
                            if progress - last_progress >= 1 or progress >= 99:
                                if progress_callback:
                                    await progress_callback(progress)
                                last_progress = progress
                        except Exception as e:
                            logger.warning(f"Progress parsing error: {str(e)}")
                            continue
                
                # Verify completion
                returncode = await process.wait()
                if returncode != 0:
                    error = '\n'.join(stderr_lines[-10:])  # Show last 10 error lines
                    raise RuntimeError(f"FFmpeg error: {error}")
                
                # Verify output
                if not ospath.exists(output_path) or ospath.getsize(output_path) == 0:
                    raise RuntimeError("Encoding failed - empty output file")
                
                # Final progress update
                if progress_callback:
                    await progress_callback(100)
                
                return output_path
            
            except Exception as e:
                logger.error(f"Encoding failed: {str(e)}", exc_info=True)
                if ospath.exists(output_path):
                    try:
                        osremove(output_path)
                    except:
                        pass
                raise

# [Rest of your existing code remains the same - all the handler functions, etc.]
# Only the VideoEncoder class has been modified to properly handle HEVC to H.264 conversion

async def update_progress(chat_id: int, text: str, force_new: bool = False):
    """Enhanced progress updater with rate limiting and better formatting"""
    try:
        now = time.time()
        if chat_id in progress_messages:
            last_update = user_sessions.get(chat_id, {}).get("last_progress_update", 0)
            if not force_new and (now - last_update < 2):  # More frequent updates
                return
            
        if chat_id in progress_messages and not force_new:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_messages[chat_id],
                    text=text
                )
                user_sessions[chat_id]["last_progress_update"] = now
                return
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except RPCError as e:
                if "MESSAGE_NOT_MODIFIED" not in str(e):
                    raise
        
        msg = await bot.send_message(chat_id, text)
        progress_messages[chat_id] = msg.id
        if chat_id in user_sessions:
            user_sessions[chat_id]["last_progress_update"] = now
            
    except Exception as e:
        logger.error(f"Progress update failed: {str(e)}")

async def ask_for_quality(chat_id: int):
    """Show quality selection menu"""
    buttons = [
        [
            InlineKeyboardButton("480p", callback_data=f"quality_480p_{chat_id}"),
            InlineKeyboardButton("720p", callback_data=f"quality_720p_{chat_id}")
        ],
        [
            InlineKeyboardButton("1080p", callback_data=f"quality_1080p_{chat_id}"),
            InlineKeyboardButton("Original", callback_data=f"quality_original_{chat_id}")
        ]
    ]
    await bot.send_message(
        chat_id,
        "🎚 Select video quality:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def collect_settings(chat_id: int):
    """Enhanced settings menu with more options"""
    buttons = [
        [
            InlineKeyboardButton("📏 Quality", callback_data=f"set_quality_{chat_id}"),
            InlineKeyboardButton("📝 Title", callback_data=f"set_title_{chat_id}")
        ],
        [
            InlineKeyboardButton("🖼️ Thumbnail", callback_data=f"set_thumb_{chat_id}"),
            InlineKeyboardButton("💧 Watermark", callback_data=f"set_wm_{chat_id}")
        ],
        [
            InlineKeyboardButton("🚀 Start", callback_data=f"confirm_download_{chat_id}")
        ]
    ]
    
    await bot.send_message(
        chat_id,
        "⚙️ <b>Encoding Settings</b>\n\nConfigure your video options:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def handle_download(chat_id: int, magnet_link: str):
    """Enhanced download handler with better progress tracking"""
    try:
        if chat_id not in user_sessions or user_sessions[chat_id].get("status") != "ready_to_download":
            return await bot.send_message(chat_id, "❌ Settings not configured")
        
        download_path = ospath.join("downloads", f"dl_{chat_id}_{int(time.time())}")
        makedirs(download_path, exist_ok=True)
        
        await update_progress(chat_id, "⚡ Starting download...", force_new=True)

        # Initialize downloader
        downloader = TorrentDownloader(magnet_link, download_path)
        
        user_sessions[chat_id].update({
            "status": "downloading",
            "download_path": download_path,
            "downloader": downloader,
            "start_time": time.time(),
            "last_update": time.time()
        })

        # Start download in background
        download_task = asyncio.create_task(downloader.start_download())
        active_tasks[chat_id] = download_task

        # Progress tracking
        last_size = 0
        while not download_task.done():
            await asyncio.sleep(10)
            
            try:
                current_size = sum(f.stat().st_size for f in Path(download_path).rglob('*') if f.is_file())
                
                # Calculate progress
                elapsed = time.time() - user_sessions[chat_id]["start_time"]
                speed = (current_size - last_size) / elapsed / 1024 if elapsed > 0 else 0
                
                await update_progress(
                    chat_id,
                    f"📥 Downloading...\n"
                    f"⏱️ Elapsed: {datetime.utcfromtimestamp(elapsed).strftime('%H:%M:%S')}\n"
                    f"🚀 Speed: {speed:.1f} KB/s\n"
                    f"📦 Downloaded: {current_size/(1024*1024):.1f} MB"
                )
                
                last_size = current_size
                user_sessions[chat_id]["last_update"] = time.time()
                
            except Exception as e:
                logger.warning(f"Progress update error: {str(e)}")
                continue

        await download_task
        del active_tasks[chat_id]
        
        # Verify download
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
        
        await start_processing(chat_id)
        
    except Exception as e:
        logger.error(f"Download failed: {str(e)}", exc_info=True)
        await update_progress(chat_id, f"❌ Download failed: {str(e)}", force_new=True)
        if chat_id in user_sessions:
            user_sessions.pop(chat_id, None)
        if chat_id in active_tasks:
            active_tasks.pop(chat_id, None)

async def start_processing(chat_id: int):
    """Enhanced processing with better error recovery"""
    try:
        session = user_sessions.get(chat_id)
        if not session or session["status"] != "downloaded":
            return await update_progress(chat_id, "❌ No downloaded files found", force_new=True)
        
        file_path = session["file_path"]
        output_path = ospath.join("encoded", f"encoded_{ospath.basename(file_path)}.mp4")
        
        await update_progress(chat_id, "🔄 Starting video encoding...")
        
        async def progress_callback(progress: float):
            """Enhanced progress callback"""
            await update_progress(
                chat_id,
                f"🔧 Encoding in progress...\n"
                f"📊 Status: {progress:.1f}% complete\n"
                f"📁 File: {ospath.basename(file_path)}"
            )
        
        try:
            # Run encoding
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
                duration=int(await VideoEncoder.get_video_info(encoded_path)[0]),
                width=(await VideoEncoder.get_video_info(encoded_path)[1])[0],
                height=(await VideoEncoder.get_video_info(encoded_path)[1])[1],
                progress=lambda c, t: asyncio.create_task(
                    update_progress(chat_id, f"📤 Upload progress: {c/t*100:.1f}%")
                )
            )
            
        except Exception as e:
            raise Exception(f"Encoding failed: {str(e)}")
        finally:
            # Cleanup
            try:
                if ospath.exists(output_path):
                    osremove(output_path)
                if ospath.exists(file_path):
                    osremove(file_path)
                if "download_path" in session:
                    shutil.rmtree(session["download_path"], ignore_errors=True)
            except Exception as e:
                logger.error(f"Cleanup error: {str(e)}")
        
        await update_progress(chat_id, "✅ Processing completed successfully!", force_new=True)
        
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}", exc_info=True)
        await update_progress(chat_id, f"❌ Processing failed: {str(e)}", force_new=True)
    finally:
        user_sessions.pop(chat_id, None)
        progress_messages.pop(chat_id, None)
        active_tasks.pop(chat_id, None)

async def cleanup_temp_files():
    """Enhanced cleanup with better resource management"""
    while True:
        await asyncio.sleep(3600)  # Run hourly
        try:
            now = time.time()
            for folder in ["downloads", "encoded", "thumbnails", "watermarks"]:
                if not ospath.exists(folder):
                    continue
                    
                for item in os.listdir(folder):
                    item_path = ospath.join(folder, item)
                    try:
                        # Delete files older than 24 hours
                        if ospath.isfile(item_path) and now - ospath.getmtime(item_path) > 86400:
                            osremove(item_path)
                        # Delete empty directories older than 1 hour
                        elif ospath.isdir(item_path) and now - ospath.getmtime(item_path) > 3600:
                            try:
                                os.rmdir(item_path)
                            except OSError:  # Directory not empty
                                pass
                    except Exception as e:
                        logger.warning(f"Cleanup failed for {item_path}: {str(e)}")
                        
        except Exception as e:
            logger.error(f"Cleanup error: {str(e)}", exc_info=True)

async def check_system_requirements():
    """Verify all required tools are available"""
    required = ['ffmpeg', 'ffprobe']
    missing = []
    
    for cmd in required:
        try:
            process = await asyncio.create_subprocess_exec(
                cmd, '-version',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await process.wait()
            if process.returncode != 0:
                missing.append(cmd)
        except:
            missing.append(cmd)
    
    if missing:
        raise RuntimeError(f"Missing required tools: {', '.join(missing)}")

@bot.on_message(filters.command("start"))
async def start_handler(client: Client, message: Message):
    """Initialize bot with system checks"""
    try:
        await check_system_requirements()
        asyncio.create_task(cleanup_temp_files())
        logger.info("System checks passed, bot is ready")
        await message.reply("🤖 Bot is ready! Send /magnet to start")
    except Exception as e:
        logger.critical(f"Startup failed: {str(e)}")
        await message.reply(f"❌ Startup failed: {str(e)}")
        raise

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
        "status": "configuring",
        "metadata": {},
        "quality": "720p"  # Default quality
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

@bot.on_message(filters.command("cancel") & filters.private)
async def cancel_handler(client: Client, message: Message):
    """Handle cancel command"""
    chat_id = message.chat.id
    if chat_id in active_tasks:
        active_tasks[chat_id].cancel()
        await message.reply("⏹️ Current task cancelled")
    else:
        await message.reply("❌ No active task to cancel")

@bot.on_message(filters.command("status") & filters.private)
async def status_handler(client: Client, message: Message):
    """Show current status"""
    chat_id = message.chat.id
    if chat_id in user_sessions:
        status = user_sessions[chat_id].get("status", "unknown")
        await message.reply(f"🔄 Current status: {status.capitalize()}")
    else:
        await message.reply("ℹ️ No active session")

if __name__ == "__main__":
    logger.info("Starting enhanced video encoder bot...")
    try:
        # Initialize cleanup task
        loop = asyncio.get_event_loop()
        loop.create_task(cleanup_temp_files())
        
        # Start the bot
        bot.run()
    except Exception as e:
        logger.critical(f"Bot crashed: {str(e)}", exc_info=True)
    finally:
        logger.info("Bot shutdown complete")
