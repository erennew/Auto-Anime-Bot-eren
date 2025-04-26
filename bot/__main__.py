from asyncio import create_task, create_subprocess_exec, run as asyrun, all_tasks, sleep as asleep
from aiofiles import open as aiopen
from pyrogram import idle
from pyrogram.filters import command, user
from os import path as ospath, execl, kill
from sys import executable
from signal import SIGKILL
import json

from bot import bot, Var, bot_loop, sch, LOGS, ffQueue, ffLock, ffpids_cache, ff_queued
from bot.core.auto_animes import fetch_animes
from bot.core.func_utils import clean_up, new_task, editMessage
from bot.modules.up_posts import upcoming_animes
from bot.core.encoder import start_encode  # ✅ Your real encode function

TASK_STATE_FILE = "task_state.json"
MAX_RETRIES = 3

task_retries = {}

# Save tasks before restart
async def save_task_state():
    if not ffQueue.empty():
        task_ids = []
        async with ffLock:
            while not ffQueue.empty():
                task_id = await ffQueue.get()
                task_ids.append(task_id)
        async with aiopen(TASK_STATE_FILE, "w") as f:
            await f.write(json.dumps(task_ids))
        LOGS.info(f"Saved {len(task_ids)} tasks before restart.")

# Restore tasks after restart
async def restore_task_state():
    if ospath.isfile(TASK_STATE_FILE):
        async with aiopen(TASK_STATE_FILE, "r") as f:
            data = await f.read()
            try:
                task_ids = json.loads(data)
                for task_id in task_ids:
                    await ffQueue.put(task_id)
                    ff_queued[task_id].clear()
                LOGS.info(f"Restored {len(task_ids)} tasks after restart.")
            except Exception as e:
                LOGS.error(f"Failed to restore tasks: {e}")

@bot.on_message(command('restart') & user(Var.ADMINS))
@new_task
async def restart_cmd(client, message):
    rmessage = await message.reply('<i>Restarting...</i>')
    if sch.running:
        sch.shutdown(wait=False)
    await save_task_state()
    await clean_up()
    if len(ffpids_cache) != 0:
        for pid in ffpids_cache:
            try:
                LOGS.info(f"Process ID : {pid}")
                kill(pid, SIGKILL)
            except (OSError, ProcessLookupError):
                LOGS.error("Killing Process Failed !!")
                continue
    await (await create_subprocess_exec('python3', 'update.py')).wait()
    async with aiopen(".restartmsg", "w") as f:
        await f.write(f"{rmessage.chat.id}\n{rmessage.id}\n")
    execl(executable, executable, "-m", "bot")

async def restart_notify():
    if ospath.isfile(".restartmsg"):
        with open(".restartmsg") as f:
            chat_id, msg_id = map(int, f)
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text="<i>Restarted !</i>")
        except Exception as e:
            LOGS.error(e)

async def handle_encoding(post_id):
    try:
        LOGS.info(f"Encoding started for {post_id}")
        ret = await start_encode(post_id)  # ✅ REAL ENCODE FUNCTION
        if ret != 0:
            raise Exception(f"Encode failed with exit code {ret}")
        LOGS.info(f"Encoding finished for {post_id}")
        ff_queued[post_id].set()
    except Exception as e:
        LOGS.error(f"Encoding failed for {post_id}: {e}")
        task_retries[post_id] = task_retries.get(post_id, 0) + 1
        if task_retries[post_id] <= MAX_RETRIES:
            LOGS.warning(f"Retrying {post_id} ({task_retries[post_id]} attempt)")
            await ffQueue.put(post_id)
            ff_queued[post_id].clear()
        else:
            LOGS.error(f"Max retries exceeded for {post_id}")

async def queue_loop():
    LOGS.info("Queue Loop Started !!")
    while True:
        if not ffQueue.empty():
            post_id = await ffQueue.get()
            await asleep(1.5)
            bot_loop.create_task(handle_encoding(post_id))
            async with ffLock:
                ffQueue.task_done()
        await asleep(10)

async def main():
    sch.add_job(upcoming_animes, "cron", hour=0, minute=30)
    await bot.start()
    await restart_notify()
    await restore_task_state()
    LOGS.info('Auto Anime Bot Started!')
    sch.start()
    bot_loop.create_task(queue_loop())
    await fetch_animes()
    await idle()
    LOGS.info('Auto Anime Bot Stopped!')
    await bot.stop()
    for task in all_tasks():
        task.cancel()
    await clean_up()
    LOGS.info('Finished AutoCleanUp !!')

if __name__ == '__main__':
    bot_loop.run_until_complete(main())
