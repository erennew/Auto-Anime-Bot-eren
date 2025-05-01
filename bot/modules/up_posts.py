from json import loads as jloads
from os import path as ospath, execl
from sys import executable

from aiohttp import ClientSession
from bot import Var, bot, ffQueue
from bot.core.text_utils import TextEditor
from bot.core.reporter import rep


from pyrogram.enums import ParseMode  # Add this import at the top

async def upcoming_animes():
    """Post today's anime schedule with clean formatting"""
    global TD_SCHR
    
    if not Var.SEND_SCHEDULE:
        return

    try:
        # Fetch schedule with retry logic
        schedule = await fetch_schedule_with_retry()
        
        # Prepare the header
        header = """
<b>🌸 𝗔𝗡𝗜𝗠𝗘 𝗦𝗖𝗛𝗘𝗗𝗨𝗟𝗘 🌸</b>
<code>──────────────────────</code>
<b>📅 Today's Releases • IST</b>
<code>──────────────────────</code>
"""

        # Prepare anime entries
        anime_entries = []
        
        for anime in schedule["schedule"][:15]:  # Limit to 15 anime
            try:
                editor = TextEditor(anime["title"])
                await editor.load_anilist()
                data = editor.adata
                
                # Create clean entry format
                title = data.get('title', {}).get('english') or anime['title']
                score = data.get('averageScore', 'N/A')
                
                entry = f"""
<b>🎬 {title}</b>
<blockquote>🕒 {anime['time']}  •  ⭐ {score}/100</blockquote>
"""
                anime_entries.append(entry)

            except Exception as e:
                LOGS.error(f"Error processing anime {anime['title']}: {e}")
                continue

        # Compose footer
        total_anime = len(anime_entries)
        footer = f"""
<code>──────────────────────</code>
<b>🌠 {total_anime} releases today</b>
"""

        message_text = header + "\n".join(anime_entries) + footer

        # Send or update the schedule message (no buttons)
        if TD_SCHR:
            try:
                await TD_SCHR.edit_text(
                    message_text,
                    disable_web_page_preview=True,
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                # If editing fails, send new message
                TD_SCHR = await bot.send_message(
                    Var.MAIN_CHANNEL,
                    message_text,
                    disable_web_page_preview=True,
                    parse_mode=ParseMode.HTML
                )
                await TD_SCHR.pin(disable_notification=True)
        else:
            TD_SCHR = await bot.send_message(
                Var.MAIN_CHANNEL,
                message_text,
                disable_web_page_preview=True,
                parse_mode=ParseMode.HTML
            )

        # Ensure queue processing
        if not ffQueue.empty():
            await ffQueue.join()

    except Exception as e:
        error_msg = f"⚠️ Schedule Error: {str(e)[:200]}"
        await rep.report(error_msg, "error")
        LOGS.exception("Failed to post schedule")
async def update_shdr(name, link):
    if TD_SCHR is not None:
        # Split message into lines to find and update the anime status
        TD_lines = TD_SCHR.text.split('\n')
        for i, line in enumerate(TD_lines):
            if line.startswith(f"📌 {name}"):
                TD_lines[i+2] = f"    • **Status :** ✅ __Uploaded__\n    • **Link :** {link}"
        await TD_SCHR.edit("\n".join(TD_lines))
