import asyncio
import os
import time
import logging
import shutil
import re
from pathlib import Path
from os import path as ospath, remove as osremove, makedirs
from typing import Optional, Dict, Callable, List, Tuple, Union
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, RPCError
from torrentp import TorrentDownloader
from datetime import datetime
import humanize
import threading

# Bot configuration
API_ID = 24500584
API_HASH = "449da69cf4081dc2cc74eea828d0c490"
BOT_TOKEN = "1599848664:AAHc75il2BECWK39tiPv4pVf-gZdPt4MFcw"
MAX_CONCURRENT_TASKS = 3
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB
MAX_TOTAL_SIZE = 10 * 1024 * 1024 * 1024  # 10GB for batch

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
active_tasks: Dict[int, List[asyncio.Task]] = {}  # Now stores list of tasks
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

class VideoEncoder:
    """Enhanced video encoder with batch processing support"""
    
    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """Remove special characters from filename"""
        return re.sub(r'[^\w\-_. ]', '', filename)

    @staticmethod
    async def get_video_duration(file_path: str) -> float:
        """Get video duration in seconds using ffprobe"""
        cmd = [
            'ffprobe', '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            file_path
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await process.communicate()
        
        if process.returncode != 0:
            return 0.0
            
        try:
            return float(stdout.decode().strip())
        except:
            return 0.0

    @staticmethod
    async def encode_with_progress(
        input_path: str,
        output_path: str,
        quality: str,
        metadata: Dict[str, str],
        watermark_path: Optional[str] = None,
        thumbnail_path: Optional[str] = None,
        progress_callback: Optional[Callable[[float, str], None]] = None
    ) -> str:
        """Convert video with proper progress callback handling"""
        async with semaphore:
            try:
                # 1. Validate input
                if not ospath.exists(input_path):
                    raise FileNotFoundError(f"Input file not found: {input_path}")
                
                # 2. Get clean title for metadata
                title = metadata.get("title", "Untitled")
                clean_title = VideoEncoder.sanitize_filename(title)
                
                # 3. Get video duration for progress calculation
                duration = await VideoEncoder.get_video_duration(input_path)
                if duration <= 0:
                    duration = 1  # Prevent division by zero
                
                # 4. Build base command
                cmd = ['ffmpeg', '-hide_banner', '-loglevel', 'error', '-i', input_path]
                
                # 5. Handle watermark if exists
                if watermark_path and ospath.exists(watermark_path):
                    cmd.extend(['-i', watermark_path])
                    cmd.extend([
                        '-filter_complex', '[0:v][1:v]overlay=W-w-10:H-h-10[outv]',
                        '-map', '[outv]'
                    ])
                else:
                    cmd.extend(['-map', '0:v?'])
                
                # 6. Handle quality settings
                if quality == 'original':
                    cmd.extend(['-c:v', 'copy'])
                else:
                    presets = {
                        '480p': {'height': 480, 'crf': 28, 'preset': 'veryfast'},
                        '720p': {'height': 720, 'crf': 27, 'preset': 'veryfast'},
                        '1080p': {'height': 1080, 'crf': 26, 'preset': 'veryfast'}
                    }
                    
                    if quality not in presets:
                        raise ValueError(f"Invalid quality: {quality}")
                    
                    cmd.extend([
                        '-vf', f'scale=-2:{presets[quality]["height"]}',
                        '-c:v', 'libx264',
                        '-pix_fmt', 'yuv420p',
                        '-crf', str(presets[quality]["crf"]),
                        '-preset', presets[quality]["preset"]
                    ])
                
                # 7. Handle audio and subtitles
                cmd.extend([
                    '-map', '0:a?',
                    '-c:a', 'copy',
                    '-map', '0:s?',
                    '-c:s', 'copy'
                ])
                
                # 8. Add metadata for all streams
                metadata_cmds = [
                    '-metadata', f'title={clean_title}',
                    '-metadata:s:v:0', f'title={clean_title}',
                    '-metadata:s:a:0', f'title={clean_title}',
                    '-metadata:s:s:0', f'title={clean_title}'
                ]
                
                # Add additional metadata for multiple streams
                for i in range(1, 8):
                    metadata_cmds.extend([
                        f'-metadata:s:a:{i}', f'title={clean_title}',
                        f'-metadata:s:s:{i}', f'title={clean_title}'
                    ])
                
                cmd.extend(metadata_cmds)
                
                # 9. Set output filename with clean title
                output_filename = f"{clean_title}.mkv"
                final_output_path = ospath.join(ospath.dirname(output_path), output_filename)
                
                cmd.extend([
                    '-f', 'matroska',
                    '-y',
                    final_output_path
                ])
                
                # 10. Execute command
                logger.info(f"Executing: {' '.join(cmd)}")
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                # 11. Progress tracking with safe callback handling
                start_time = time.time()
                last_progress = 0
                
                while True:
                    if process.returncode is not None:
                        break
                    
                    # Get current position using ffprobe if available
                    current_pos = 0.0
                    if ospath.exists(final_output_path):
                        current_pos = await VideoEncoder.get_video_duration(final_output_path)
                    
                    if current_pos > 0:
                        progress = min(90, (current_pos / duration) * 90)  # 90% max for encoding
                        
                        # Only update if progress changed significantly
                        if abs(progress - last_progress) >= 1 or progress >= 90:
                            last_progress = progress
                            if progress_callback is not None:
                                try:
                                    file_size = ospath.getsize(input_path)
                                    await progress_callback(
                                        progress,
                                        f"Encoding: {ospath.basename(input_path)}\n"
                                        f"Size: {humanize.naturalsize(file_size)}\n"
                                        f"Time: {humanize.precisedelta(datetime.now() - datetime.fromtimestamp(start_time))}"
                                    )
                                except Exception as e:
                                    logger.warning(f"Progress callback error: {str(e)}")
                    
                    await asyncio.sleep(5)
                
                # 12. Verify completion
                if process.returncode != 0:
                    stderr = await process.stderr.read()
                    error = stderr.decode('utf-8')[-500:] or "Unknown error"
                    raise RuntimeError(f"FFmpeg error: {error}")
                
                if progress_callback is not None:
                    await progress_callback(100, "Encoding complete!")
                
                return final_output_path
            
            except Exception as e:
                logger.error(f"Encoding failed: {str(e)}", exc_info=True)
                if ospath.exists(output_path):
                    try:
                        osremove(output_path)
                    except:
                        pass
                raise

async def update_progress(
    chat_id: int,
    text: str,
    force_new: bool = False,
    progress: Optional[float] = None,
    last_update_time: float = 0
) -> Tuple[Optional[float], Optional[int]]:
    """Update progress message with flood control"""
    try:
        current_time = time.time()
        
        # Don't update more often than every 5 seconds unless forced
        if current_time - last_update_time < 5 and not force_new:
            return last_update_time, progress_messages.get(chat_id)
        
        # Add progress bar if progress is provided
        if progress is not None:
            progress_bar = "[" + "■" * int(progress / 10) + "□" * (10 - int(progress / 10)) + "]"
            text = f"{text}\n\n{progress_bar} {progress:.1f}%"
        
        # Add timestamp
        text = f"{text}\n\n⏱️ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        if chat_id in progress_messages and not force_new:
            try:
                msg = await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_messages[chat_id],
                    text=text
                )
                return current_time, msg.id
            except FloodWait as e:
                logger.warning(f"Flood wait: waiting {e.value} seconds")
                await asyncio.sleep(e.value)
                return await update_progress(chat_id, text, force_new, progress, last_update_time)
            except Exception:
                # If editing fails, we'll send a new message
                pass
        
        # If editing failed or we need a new message
        try:
            msg = await bot.send_message(chat_id=chat_id, text=text)
            progress_messages[chat_id] = msg.id
            return current_time, msg.id
        except FloodWait as e:
            logger.warning(f"Flood wait: waiting {e.value} seconds")
            await asyncio.sleep(e.value)
            return await update_progress(chat_id, text, force_new, progress, last_update_time)
        
    except Exception as e:
        logger.error(f"Failed to update progress: {str(e)}")
        return last_update_time, progress_messages.get(chat_id)
async def handle_torrent_download(chat_id: int, magnet_link: str) -> Tuple[str, List[Dict]]:
    """Robust torrent downloader with flood control"""
    download_dir = ospath.join("downloads", f"dl_{chat_id}_{int(time.time())}")
    makedirs(download_dir, exist_ok=True)
    
    try:
        last_update_time, _ = await update_progress(chat_id, "🔍 Initializing torrent download...")
        
        # Initialize torrent downloader with timeout
        torrent_downloader = TorrentDownloader(magnet_link, download_dir)
        
        # Track download status
        start_time = time.time()
        last_size = 0
        stall_count = 0
        
        def run_download():
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(torrent_downloader.start_download())
                loop.close()
            except Exception as e:
                logger.error(f"Download thread error: {str(e)}")

        download_thread = threading.Thread(target=run_download, daemon=True)
        download_thread.start()

        # Monitor download progress
        while download_thread.is_alive():
            await asyncio.sleep(5)  # Check every 5 seconds
            
            # Calculate download progress
            try:
                current_size = 0
                file_count = 0
                for root, _, files in os.walk(download_dir):
                    for f in files:
                        if not f.startswith('.'):  # Skip hidden files
                            file_path = ospath.join(root, f)
                            if ospath.exists(file_path):
                                current_size += ospath.getsize(file_path)
                                file_count += 1
                
                # Check for stalled download
                if current_size <= last_size:
                    stall_count += 1
                    if stall_count > 12:  # 60 seconds without progress
                        raise ValueError("Download stalled - no progress for 60 seconds")
                else:
                    stall_count = 0
                
                last_size = current_size
                
                # Update progress with flood control
                elapsed = time.time() - start_time
                speed = humanize.naturalsize(current_size/elapsed) + "/s" if elapsed > 0 else ""
                
                last_update_time, _ = await update_progress(
                    chat_id,
                    f"⬇️ Downloading torrent\n"
                    f"📦 Size: {humanize.naturalsize(current_size)}\n"
                    f"📄 Files: {file_count}\n"
                    f"🚀 Speed: {speed}\n"
                    f"⏱️ Elapsed: {humanize.precisedelta(elapsed)}",
                    progress=min(99, (elapsed/600)*100),
                    last_update_time=last_update_time
                )
                
            except Exception as e:
                logger.warning(f"Progress update error: {str(e)}")
                continue

        # Verify downloaded files
        all_files = []
        for root, _, files in os.walk(download_dir):
            for f in files:
                if not f.startswith('.'):  # Skip hidden files
                    file_path = ospath.join(root, f)
                    try:
                        if ospath.exists(file_path) and ospath.getsize(file_path) > 0:
                            all_files.append({
                                'name': f,
                                'size': ospath.getsize(file_path),
                                'path': file_path,
                                'is_video': f.lower().endswith(('.mkv', '.mp4', '.avi', '.mov', '.flv', '.wmv', '.webm'))
                            })
                    except Exception as e:
                        logger.warning(f"Skipping file {f}: {str(e)}")
                        continue

        if not all_files:
            if not os.listdir(download_dir):
                raise ValueError("Download directory is completely empty - invalid torrent")
            else:
                raise ValueError("No valid files found - may be permission issues")

        await update_progress(chat_id, f"✅ Download complete! Found {len(all_files)} files")
        return download_dir, all_files
        
    except Exception as e:
        logger.error(f"Torrent download failed: {str(e)}", exc_info=True)
        if ospath.exists(download_dir):
            try:
                shutil.rmtree(download_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"Cleanup failed: {str(e)}")
        raise ValueError(f"Download failed: {str(e)}")
async def process_single_file(
    chat_id: int,
    file_path: str,
    output_dir: str,
    quality: str,
    metadata: Dict[str, str],
    watermark_path: Optional[str],
    thumbnail_path: Optional[str],
    file_index: int,
    total_files: int
) -> Optional[str]:
    """Process a single file with progress tracking"""
    try:
        # Define progress callback
        async def progress_callback(progress: float, status: str):
            await update_progress(
                chat_id,
                f"🔧 Processing file {file_index + 1}/{total_files}\n"
                f"📄 {ospath.basename(file_path)}\n"
                f"{status}",
                progress=progress
            )
        
        # Process file
        encoded_path = await VideoEncoder.encode_with_progress(
            input_path=file_path,
            output_path=ospath.join(output_dir, f"temp_{file_index}.mkv"),
            quality=quality,
            metadata=metadata,
            watermark_path=watermark_path,
            thumbnail_path=thumbnail_path,
            progress_callback=progress_callback
        )
        
        return encoded_path
        
    except Exception as e:
        logger.error(f"Failed to process {file_path}: {str(e)}")
        await update_progress(chat_id, f"❌ Failed to process {ospath.basename(file_path)}: {str(e)}")
        return None

async def upload_file(
    chat_id: int,
    file_path: str,
    upload_mode: str,
    thumbnail_path: Optional[str],
    metadata: Dict[str, str],
    file_index: int,
    total_files: int
) -> bool:
    """Upload a file with progress tracking"""
    try:
        # Define upload progress callback
        async def upload_progress(current, total):
            progress = (current / total) * 100
            await update_progress(
                chat_id,
                f"📤 Uploading file {file_index + 1}/{total_files}\n"
                f"📄 {ospath.basename(file_path)}\n"
                f"📦 {humanize.naturalsize(current)}/{humanize.naturalsize(total)}",
                progress=progress
            )
        
        # Get file size for caption
        file_size = ospath.getsize(file_path)
        
        # Send file
        if upload_mode == "document":
            await bot.send_document(
                chat_id=chat_id,
                document=file_path,
                thumb=thumbnail_path,
                caption=f"📄 {metadata.get('title', 'File')} ({humanize.naturalsize(file_size)})",
                progress=upload_progress
            )
        else:
            await bot.send_video(
                chat_id=chat_id,
                video=file_path,
                thumb=thumbnail_path,
                caption=f"🎬 {metadata.get('title', 'Video')} ({humanize.naturalsize(file_size)})",
                progress=upload_progress
            )
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to upload {file_path}: {str(e)}")
        await update_progress(chat_id, f"❌ Failed to upload {ospath.basename(file_path)}: {str(e)}")
        return False

async def process_batch(
    chat_id: int,
    files: List[Dict],
    quality: str,
    metadata: Dict[str, str],
    watermark_path: Optional[str],
    thumbnail_path: Optional[str],
    upload_mode: str
) -> None:
    """Process a batch of files with proper tracking"""
    try:
        # Create output directory
        output_dir = ospath.join("encoded", f"batch_{chat_id}_{int(time.time())}")
        makedirs(output_dir, exist_ok=True)
        
        total_files = len(files)
        processed_files = []
        uploaded_files = []
        
        # Process each file
        for idx, file_info in enumerate(files):
            file_path = file_info['path']
            
            # Skip processing for non-video files, just upload them
            if not file_info.get('is_video', False):
                success = await upload_file(
                    chat_id=chat_id,
                    file_path=file_path,
                    upload_mode="document",  # Always upload non-video as document
                    thumbnail_path=None,
                    metadata=metadata,
                    file_index=idx,
                    total_files=total_files
                )
                
                if success:
                    uploaded_files.append(file_path)
                continue
            
            # Process video files
            encoded_path = await process_single_file(
                chat_id=chat_id,
                file_path=file_path,
                output_dir=output_dir,
                quality=quality,
                metadata=metadata,
                watermark_path=watermark_path,
                thumbnail_path=thumbnail_path,
                file_index=idx,
                total_files=total_files
            )
            
            if encoded_path:
                processed_files.append(encoded_path)
                
                # Upload file
                success = await upload_file(
                    chat_id=chat_id,
                    file_path=encoded_path,
                    upload_mode=upload_mode,
                    thumbnail_path=thumbnail_path,
                    metadata=metadata,
                    file_index=idx,
                    total_files=total_files
                )
                
                if success:
                    uploaded_files.append(encoded_path)
        
        # Final status
        success_count = len(uploaded_files)
        await update_progress(
            chat_id,
            f"✅ Batch processing complete!\n"
            f"📊 {success_count}/{total_files} files processed successfully",
            force_new=True
        )
        
    except Exception as e:
        logger.error(f"Batch processing failed: {str(e)}", exc_info=True)
        await update_progress(chat_id, f"❌ Batch processing failed: {str(e)}", force_new=True)
    finally:
        # Cleanup
        try:
            for file_path in processed_files:
                if ospath.exists(file_path):
                    osremove(file_path)
            
            if 'output_dir' in locals() and ospath.exists(output_dir):
                shutil.rmtree(output_dir, ignore_errors=True)
        except Exception as e:
            logger.error(f"Cleanup error: {str(e)}")

async def start_processing(chat_id: int):
    """Handle processing for both single and batch files"""
    try:
        session = user_sessions.get(chat_id)
        if not session:
            return await update_progress(chat_id, "❌ No active session found", force_new=True)
        
        if session["status"] == "downloaded":
            # Single file processing
            file_path = session["file_path"]
            files = [{'path': file_path, 'size': ospath.getsize(file_path)}]
        elif session["status"] == "batch_downloaded":
            # Batch processing
            files = session["files"]
        else:
            return await update_progress(chat_id, "❌ No downloaded files found", force_new=True)
        
        # Start processing task
        task = asyncio.create_task(process_batch(
            chat_id=chat_id,
            files=files,
            quality=session.get("quality", "720p"),
            metadata=session.get("metadata", {}),
            watermark_path=session.get("watermark"),
            thumbnail_path=session.get("thumbnail"),
            upload_mode=session.get("upload_mode", "video")
        ))
        
        # Store task
        if chat_id not in active_tasks:
            active_tasks[chat_id] = []
        active_tasks[chat_id].append(task)
        
        # Wait for task to complete
        try:
            await task
        except asyncio.CancelledError:
            await update_progress(chat_id, "⏹️ Processing cancelled", force_new=True)
        except Exception as e:
            logger.error(f"Processing error: {str(e)}", exc_info=True)
        
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}", exc_info=True)
        await update_progress(chat_id, f"❌ Processing failed: {str(e)}", force_new=True)
    finally:
        # Cleanup
        try:
            if "session" in locals():
                if "download_path" in session:
                    shutil.rmtree(session["download_path"], ignore_errors=True)
                if "file_path" in session and ospath.exists(session["file_path"]):
                    osremove(session["file_path"])
        
            if chat_id in user_sessions:
                user_sessions.pop(chat_id, None)
            if chat_id in progress_messages:
                progress_messages.pop(chat_id, None)
            if chat_id in active_tasks:
                for t in active_tasks[chat_id]:
                    if not t.done():
                        t.cancel()
                active_tasks.pop(chat_id, None)
        except Exception as e:
            logger.error(f"Cleanup error: {str(e)}")  # <-- This part was missing!

# Now this can be defined safely
async def ask_for_quality(chat_id: int):
    """Show quality selection menu with visual indicators"""
    buttons = [
        [
            InlineKeyboardButton("🎥 480p", callback_data=f"quality_480p_{chat_id}"),
            InlineKeyboardButton("🎥 720p", callback_data=f"quality_720p_{chat_id}")
        ],
        [
            InlineKeyboardButton("🎥 1080p", callback_data=f"quality_1080p_{chat_id}"),
            InlineKeyboardButton("📼 Original", callback_data=f"quality_original_{chat_id}")
        ]
    ]
    await bot.send_message(
        chat_id,
        "**🎚 Select Video Quality**\n\n"
        "Choose the output quality for your video:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def collect_settings(chat_id: int):
    """Enhanced settings menu with batch options"""
    session = user_sessions.get(chat_id, {})
    
    # Build quality indicator
    quality = session.get("quality", "720p")
    quality_icon = "🟢" if quality != "original" else "🔵"
    
    # Build upload mode indicator
    upload_mode = session.get("upload_mode", "video")
    upload_icon = "🎥" if upload_mode == "video" else "📄"
    
    buttons = [
        [
            InlineKeyboardButton(f"{quality_icon} Quality: {quality}", callback_data=f"set_quality_{chat_id}"),
            InlineKeyboardButton("✏️ Title", callback_data=f"set_title_{chat_id}")
        ],
        [
            InlineKeyboardButton("🖼️ Thumbnail", callback_data=f"set_thumb_{chat_id}"),
            InlineKeyboardButton("💧 Watermark", callback_data=f"set_wm_{chat_id}")
        ],
        [
            InlineKeyboardButton(f"{upload_icon} Upload as {'Video' if upload_mode == 'video' else 'Document'}", 
                               callback_data=f"toggle_upload_{chat_id}")
        ],
        [
            InlineKeyboardButton("🚀 Start Processing", callback_data=f"confirm_download_{chat_id}")
        ]
    ]
    
    await bot.send_message(
        chat_id,
        "**⚙️ Encoding Settings**\n\n"
        "Configure your processing options:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def handle_download(chat_id: int, magnet_link: str):
    """Handle torrent download with comprehensive error reporting"""
    try:
        await update_progress(chat_id, "🔄 Starting download process...")
        
        try:
            download_dir, files = await handle_torrent_download(chat_id, magnet_link)
        except ValueError as e:
            error_msg = str(e)
            
            if "directory is empty" in error_msg:
                await update_progress(
                    chat_id,
                    "❌ Torrent appears to be empty or invalid\n"
                    "This could mean:\n"
                    "1. The torrent contains no files\n"
                    "2. The magnet link is invalid\n"
                    "3. The torrent is private or requires authentication",
                    force_new=True
                )
            elif "no accessible files" in error_msg:
                await update_progress(
                    chat_id,
                    "❌ Could not access downloaded files\n"
                    "This could be due to:\n"
                    "1. Permission issues\n"
                    "2. Corrupted download\n"
                    "3. Filesystem problems",
                    force_new=True
                )
            elif "timed out" in error_msg:
                await update_progress(
                    chat_id,
                    "❌ Download timed out after 30 minutes\n"
                    "Possible reasons:\n"
                    "1. Slow connection\n"
                    "2. No seeders available\n"
                    "3. Torrent is too large",
                    force_new=True
                )
            else:
                await update_progress(
                    chat_id,
                    f"❌ Download failed: {error_msg}\n"
                    "Please try again or check the magnet link",
                    force_new=True
                )
            return
        
        # Update session and start processing
        user_sessions[chat_id].update({
            "download_path": download_dir,
            "status": "batch_downloaded" if len(files) > 1 else "downloaded",
            "files": files,
            "file_path": files[0]['path'] if files else None
        })
        
        await start_processing(chat_id)
        
    except Exception as e:
        logger.error(f"Download processing failed: {str(e)}", exc_info=True)
        await update_progress(
            chat_id,
            f"❌ Unexpected error during download: {str(e)}\n"
            "Please report this issue",
            force_new=True
        )
        
        # Cleanup
        if chat_id in user_sessions:
            user_sessions.pop(chat_id, None)
        if chat_id in progress_messages:
            progress_messages.pop(chat_id, None)
        if chat_id in active_tasks:
            for task in active_tasks[chat_id]:
                if not task.done():
                    task.cancel()
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
        "quality": "720p", # Default quality
        "upload_mode": "video"  
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

@bot.on_callback_query(filters.regex(r"^toggle_upload_(\d+)$"))
async def toggle_upload_handler(client: Client, query):
    """Toggle upload mode between video and document"""
    chat_id = int(query.data.split("_")[2])
    
    if chat_id not in user_sessions:
        return await query.answer("Session expired!", show_alert=True)
    
    current_mode = user_sessions[chat_id].get("upload_mode", "video")
    new_mode = "document" if current_mode == "video" else "video"
    user_sessions[chat_id]["upload_mode"] = new_mode
    
    await query.answer(f"Upload mode set to {new_mode}")
    await collect_settings(chat_id)

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

    awaiting = user_sessions[chat_id]["awaiting"]

    # Handle title
    if awaiting == "title" and message.text:
        user_sessions[chat_id]["metadata"]["title"] = message.text
        user_sessions[chat_id].pop("awaiting")
        await message.reply(f"✅ Title set: {message.text}")
        await collect_settings(chat_id)

    # Handle thumbnail
    elif awaiting == "thumbnail" and message.photo:
        thumb_path = ospath.join("thumbnails", f"thumb_{chat_id}.jpg")
        await message.download(thumb_path)
        user_sessions[chat_id]["thumbnail"] = thumb_path
        user_sessions[chat_id].pop("awaiting")
        await message.reply("✅ Thumbnail saved!")
        await collect_settings(chat_id)

    # Handle watermark
    elif awaiting == "watermark" and message.photo:
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
        for task in active_tasks[chat_id]:
            if not task.done():
                task.cancel()
        await message.reply("⏹️ All active tasks cancelled")
    else:
        await message.reply("❌ No active tasks to cancel")

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
    logger.info("Starting video encoder bot...")
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(cleanup_temp_files())
        bot.run()
    except Exception as e:
        logger.critical(f"Bot crashed: {str(e)}", exc_info=True)
    finally:
        logger.info("Bot shutdown complete")
