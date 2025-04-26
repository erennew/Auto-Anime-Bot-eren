from json import loads as jloads
from os import path as ospath, execl
from sys import executable

from aiohttp import ClientSession
from bot import Var, bot, ffQueue
from bot.core.text_utils import TextEditor
from bot.core.reporter import rep

async def upcoming_animes():
    if Var.SEND_SCHEDULE:
        try:
            async with ClientSession() as ses:
                res = await ses.get("https://subsplease.org/api/?f=schedule&h=true&tz=Asia/Kolkata")
                aniContent = jloads(await res.text())["schedule"]
            text = "<b>📆 Today's Anime Releases Schedule [IST]</b>\n\n"
            for i in aniContent:
                aname = TextEditor(i["title"])
                await aname.load_anilist()
                text += f''' <a href="https://subsplease.org/shows/{i['page']}">{aname.adata.get('title', {}).get('english') or i['title']}</a>\n    • <b>Time</b> : {i["time"]} hrs\n\n'''
            
            # Send the message and pin it
            TD_SCHR = await bot.send_message(Var.MAIN_CHANNEL, text)
            if TD_SCHR:
                await (await TD_SCHR.pin()).delete()  # Pin and then delete the pin

        except Exception as err:
            await rep.report(f"Error fetching anime schedule: {str(err)}", "error")
    
    # Ensure queue processing
    if not ffQueue.empty():
        await ffQueue.join()

    # Report bot restart
    await rep.report("Auto Restarting..!!", "info")
    
    # Restart bot process (ensure the restart is properly handled)
    execl(executable, executable, "-m", "bot")


async def update_shdr(name, link):
    if TD_SCHR is not None:
        # Split message into lines to find and update the anime status
        TD_lines = TD_SCHR.text.split('\n')
        for i, line in enumerate(TD_lines):
            if line.startswith(f"📌 {name}"):
                TD_lines[i+2] = f"    • **Status :** ✅ __Uploaded__\n    • **Link :** {link}"
        await TD_SCHR.edit("\n".join(TD_lines))
