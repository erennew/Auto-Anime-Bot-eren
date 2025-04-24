import asyncio
import os
import re
from math import floor
from time import time
from os import path as ospath
from aiofiles import open as aiopen
from aiofiles.os import remove as aioremove, rename as aiorename
from asyncio import sleep as asleep, gather, create_subprocess_shell, create_task
from asyncio.subprocess import PIPE
from typing import Optional

from bot import Var, bot_loop, ffpids_cache, LOGS
from .func_utils import mediainfo, convertBytes, convertTime, editMessage
from .reporter import rep

class FFEncoder:
    def __init__(self, message, path: str, name: str, qual: str):
        self.__proc = None
        self.is_cancelled = False
        self.message = message
        self.__name = name
        self.__qual = str(qual).strip()  # Ensure clean quality string
        self.dl_path = path
        self.__total_time: Optional[float] = None
        self.out_path = ospath.join("encode", f"{name}_{self.__qual}p.mkv")
        self.__prog_file = f'prog_{self.__qual}_{int(time())}.txt'
        self.__start_time = time()

        # Validate quality
        if self.__qual not in Var.QUALS:
            raise ValueError(f"Invalid quality: {self.__qual}. Must be one of {Var.QUALS}")

    async def progress(self) -> None:
        """Track and report encoding progress"""
        self.__total_time = await mediainfo(self.dl_path, get_duration=True) or 1.0
        
        while not (self.__proc is None or self.is_cancelled):
            try:
                async with aiopen(self.__prog_file, 'r') as p:
                    text = await p.read()
                
                if text:
                    # Parse progress data
                    time_match = re.findall(r"out_time_ms=(\d+)", text)
                    size_match = re.findall(r"total_size=(\d+)", text)
                    
                    time_done = floor(int(time_match[-1]) / 1000000) if time_match else 1
                    ensize = int(size_match[-1]) if size_match else 0
                    
                    # Calculate metrics
                    diff = time() - self.__start_time
                    speed = ensize / diff if diff > 0 else 0
                    percent = round((time_done/self.__total_time)*100, 2)
                    tsize = ensize / (max(percent, 0.01)/100)
                    eta = (tsize-ensize)/max(speed, 0.01) if speed > 0 else 0
        
                    # Create progress bar
                    bar = floor(percent/8)*"█" + (12 - floor(percent/8))*"▒"
                    
                    # Format progress message
                    progress_str = f"""
<blockquote>‣ <b>Anime Name:</b> <i>{self.__name}</i></blockquote>
<blockquote>‣ <b>Quality:</b> <code>{self.__qual}p</code></blockquote>
<blockquote>‣ <b>Status:</b> <i>Encoding</i>
    <code>[{bar}]</code> {percent}%</blockquote>
<blockquote>   ‣ <b>Size:</b> {convertBytes(ensize)} of ~{convertBytes(tsize)}
    ‣ <b>Speed:</b> {convertBytes(speed)}/s
    ‣ <b>Time Elapsed:</b> {convertTime(diff)}
    ‣ <b>ETA:</b> {convertTime(eta)}</blockquote>
<blockquote>‣ <b>Files Encoded:</b> {Var.QUALS.index(self.__qual)+1}/{len(Var.QUALS)}</blockquote>
                    """
                
                    await editMessage(self.message, progress_str)
                    
                    # Check if encoding finished
                    if re.findall(r"progress=(\w+)", text)[-1] == 'end':
                        break
            except Exception as e:
                LOGS.error(f"Progress error: {str(e)}")
            await asleep(8)

    async def start_encode(self) -> Optional[str]:
        """Start the encoding process with proper error handling"""
        try:
            # Clean up old progress files
            if ospath.exists(self.__prog_file):
                await aioremove(self.__prog_file)
            
            # Create progress file
            async with aiopen(self.__prog_file, 'w'):
                LOGS.info(f"Created progress file: {self.__prog_file}")
            
            # Prepare temp files with quality in names
            dl_npath = ospath.join("encode", f"input_{self.__qual}p.mkv")
            out_npath = ospath.join("encode", f"output_{self.__qual}p.mkv")
            await aiorename(self.dl_path, dl_npath)
            
            # Get FFmpeg command for this quality
            ffcode = getattr(Var, f"FFCODE_{self.__qual}").format(
                dl_npath, 
                self.__prog_file, 
                out_npath
            )
            
            LOGS.info(f'Starting {self.__qual}p encode with command: {ffcode}')
            
            # Start process with timeout
            self.__proc = await create_subprocess_shell(
                ffcode,
                stdout=PIPE,
                stderr=PIPE
            )
            proc_pid = self.__proc.pid
            ffpids_cache.append(proc_pid)
            
            # Run encoding and progress tracking
            try:
                await asyncio.wait_for(
                    gather(
                        create_task(self.progress()),
                        self.__proc.wait()
                    ),
                    timeout=14400  # 4 hour timeout
                )
            except asyncio.TimeoutError:
                await rep.report(f"{self.__qual}p encode timed out after 4 hours", "error")
                raise
            
            # Clean up
            ffpids_cache.remove(proc_pid)
            await aiorename(dl_npath, self.dl_path)
            
            if self.is_cancelled:
                return None
            
            if self.__proc.returncode == 0:
                if ospath.exists(out_npath):
                    await aiorename(out_npath, self.out_path)
                    LOGS.info(f"Successfully encoded {self.__qual}p version")
                    return self.out_path
                else:
                    await rep.report(f"Output file missing for {self.__qual}p", "error")
            else:
                error_msg = (await self.__proc.stderr.read()).decode().strip()
                await rep.report(f"{self.__qual}p encode failed: {error_msg}", "error")
                
        except Exception as e:
            await rep.report(f"Encoding error for {self.__qual}p: {str(e)}", "error")
            if self.__proc:
                try:
                    self.__proc.kill()
                except:
                    pass
            return None
            
        return None
            
    async def cancel_encode(self) -> None:
        """Cancel the current encoding process"""
        self.is_cancelled = True
        if self.__proc is not None:
            try:
                self.__proc.kill()
                LOGS.info(f"Cancelled {self.__qual}p encode")
            except Exception as e:
                LOGS.error(f"Error cancelling encode: {str(e)}")
