from asyncio import sleep as asleep, gather
from os import path as ospath
from aiofiles.os import remove as aioremove, rmtree as aiormtree
from pyrogram.filters import command, private, user
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import FloodWait

from bot import bot, bot_loop, Var, ani_cache, ffQueue, ffLock
from bot.core.database import db
from bot.core.func_utils import (
    decode, is_fsubbed, get_fsubs, editMessage, 
    sendMessage, new_task, convertTime, getfeed
)
from bot.core.auto_animes import get_animes
from bot.core.reporter import rep

# User Commands
@bot.on_message(command('start') & private)
@new_task
async def start_msg(client, message):
    """Enhanced start command with better formatting"""
    uid = message.from_user.id
    txtargs = message.text.split()
    temp = await sendMessage(message, "<i>Checking access...</i>")
    
    # Force subscribe check
    if not await is_fsubbed(uid):
        txt, btns = await get_fsubs(uid, txtargs)
        return await editMessage(temp, txt, InlineKeyboardMarkup(btns))
    
    # Normal start flow
    if len(txtargs) <= 1:
        await temp.delete()
        btns = []
        for elem in Var.START_BUTTONS.split():
            try:
                bt, link = elem.split('|', maxsplit=1)
                btns.append([InlineKeyboardButton(bt, url=link)])
            except:
                continue
        
        user = message.from_user
        smsg = Var.START_MSG.format(
            first_name=user.first_name,
            last_name=user.last_name or "",
            mention=user.mention,
            user_id=user.id
        )
        
        if Var.START_PHOTO:
            await message.reply_photo(
                photo=Var.START_PHOTO,
                caption=smsg,
                reply_markup=InlineKeyboardMarkup(btns) if btns else None
            )
        else:
            await sendMessage(message, smsg, InlineKeyboardMarkup(btns) if btns else None)
        return
    
    # File access handling
    try:
        arg = (await decode(txtargs[1])).split('-')
        if len(arg) == 2 and arg[0] == 'get':
            fid = int(int(arg[1]) / abs(int(Var.FILE_STORE)))
            msg = await client.get_messages(Var.FILE_STORE, fid)
            
            if msg.empty:
                return await editMessage(temp, "<b>File not found!</b>")
                
            nmsg = await msg.copy(message.chat.id)
            await temp.delete()
            
            if Var.AUTO_DEL:
                async def auto_del(msg, timer):
                    await asleep(timer)
                    await msg.delete()
                
                info = await sendMessage(
                    message, 
                    f'⏳ File will auto-delete in {convertTime(Var.DEL_TIMER)}'
                )
                bot_loop.create_task(auto_del(nmsg, Var.DEL_TIMER))
                bot_loop.create_task(auto_del(info, Var.DEL_TIMER))
                
    except Exception as e:
        await rep.report(f"User {uid} error: {str(e)}", "error")
        await editMessage(temp, "❌ Invalid or expired link!")

# Admin Commands
@bot.on_message(command('pause') & private & user(Var.ADMINS))
async def pause_fetch(_, m: Message):
    ani_cache['fetch_animes'] = False
    await sendMessage(m, "⏸️ <b>Anime fetching paused!</b>")

@bot.on_message(command('resume') & private & user(Var.ADMINS))
async def resume_fetch(_, m: Message):
    ani_cache['fetch_animes'] = True
    await sendMessage(m, "▶️ <b>Anime fetching resumed!</b>")

@bot.on_message(command('log') & private & user(Var.ADMINS))
async def send_logs(_, m: Message):
    try:
        await m.reply_document("bot.log", caption="📄 <b>Bot Logs</b>")
    except Exception as e:
        await sendMessage(m, f"❌ Failed to get logs: {str(e)}")

@bot.on_message(command('addlink') & private & user(Var.ADMINS))
@new_task
async def add_rss(_, m: Message):
    if len(m.command) < 2:
        return await sendMessage(m, "ℹ️ Usage: <code>/addlink [rss_url]</code>")
    
    url = m.text.split()[1]
    if url in Var.RSS_ITEMS:
        return await sendMessage(m, "⚠️ RSS feed already exists!")
    
    Var.RSS_ITEMS.append(url)
    await sendMessage(m, f"""
✅ <b>New RSS Feed Added</b>
━━━━━━━━━━━━━━
<b>URL:</b> <code>{url}</code>
<b>Total Feeds:</b> {len(Var.RSS_ITEMS)}
""")

@bot.on_message(command('addtask') & private & user(Var.ADMINS))
@new_task
async def add_task(_, m: Message):
    if len(m.command) < 2:
        return await sendMessage(m, "ℹ️ Usage: <code>/addtask [url] (index)</code>")
    
    args = m.text.split()
    url = args[1]
    index = int(args[2]) if len(args) > 2 and args[2].isdigit() else 0
    
    if not (task := await getfeed(url, index)):
        return await sendMessage(m, "❌ Failed to fetch torrent!")
    
    bot_loop.create_task(get_animes(task.title, task.link, force=True))
    await sendMessage(m, f"""
🎬 <b>Manual Task Added</b>
━━━━━━━━━━━━━━
<b>Title:</b> {task.title}
<b>URL:</b> <code>{url}</code>
<b>Index:</b> {index}
""")

@bot.on_message(command('queue') & private & user(Var.ADMINS))
async def show_queue(_, m: Message):
    queue_info = [
        f"📊 <b>Encoding Queue</b>",
        f"• Pending: {ffQueue.qsize()}",
        f"• Locked: {'🔒' if ffLock.locked() else '🔓'}"
    ]
    await sendMessage(m, "\n".join(queue_info))

@bot.on_message(command('status') & private & user(Var.ADMINS))
async def bot_status(_, m: Message):
    status_msg = [
        "🤖 <b>Bot Status</b>",
        f"• Fetching: {'✅ ON' if ani_cache['fetch_animes'] else '❌ OFF'}",
        f"• Ongoing: {len(ani_cache['ongoing'])}",
        f"• Completed: {len(ani_cache['completed'])}",
        f"• RSS Feeds: {len(Var.RSS_ITEMS)}",
        f"• Qualities: {', '.join(Var.QUALS)}"
    ]
    await sendMessage(m, "\n".join(status_msg))

@bot.on_message(command('clean') & private & user(Var.ADMINS))
async def cleanup(_, m: Message):
    try:
        await gather(
            aiormtree("downloads"),
            aiormtree("encode"),
            aiormtree("thumbs")
        )
        await sendMessage(m, "🧹 <b>Temporary files cleaned!</b>")
    except Exception as e:
        await sendMessage(m, f"❌ Clean failed: {str(e)}")

@bot.on_message(command('setquals') & private & user(Var.ADMINS))
async def set_qualities(_, m: Message):
    if len(m.command) < 2:
        return await sendMessage(m, f"""
ℹ️ Current qualities: {', '.join(Var.QUALS)}
Usage: <code>/setquals 1080 720 480</code>
""")
    
    Var.QUALS = m.command[1:]
    await sendMessage(m, f"""
✅ <b>Quality presets updated!</b>
━━━━━━━━━━━━━━
<b>New settings:</b> {', '.join(Var.QUALS)}
""")

@bot.on_message(command('broadcast') & private & user(Var.ADMINS))
async def broadcast(_, m: Message):
    if len(m.command) < 2:
        return await sendMessage(m, "ℹ️ Usage: <code>/broadcast [message]</code>")
    
    msg = m.text.split(' ', 1)[1]
    users = []  # Replace with actual user list from DB
    
    status = await sendMessage(m, f"📢 Broadcasting to {len(users)} users...")
    success = 0
    
    for user_id in users:
        try:
            await bot.send_message(user_id, msg)
            success += 1
            await asleep(0.3)  # Prevent flooding
        except Exception:
            continue
    
    await editMessage(status, f"""
✅ <b>Broadcast Complete</b>
━━━━━━━━━━━━━━
<b>Total:</b> {len(users)} users
<b>Success:</b> {success}
<b>Failed:</b> {len(users) - success}
""")
from pyrogram.filters import command, user
from pyrogram.types import Message

@bot.on_message(command('updateschedule') & user(Var.ADMINS))
async def manual_trigger(_, m: Message):
    try:
        # Call the schedule function
        await upcoming_animes()  
        await m.reply("✅ Schedule successfully sent to MAIN_CHANNEL!")
    except Exception as e:
        await m.reply(f"❌ Failed to update schedule: {str(e)}")
        await rep.report(f"Manual schedule update failed: {e}", "error")
