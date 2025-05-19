from calendar import month_name
from datetime import datetime
from random import choice
from asyncio import sleep as asleep
from aiohttp import ClientSession
from anitopy import parse

from bot import Var, bot
from .ffencoder import ffargs
from .func_utils import handle_logs
from .reporter import rep


CAPTION_FORMAT = """
<b>╭───❖ <i>{title}</i> ❖──╮</b>

<b>├・🌀</b> <i>Genres:</i>    <i>{genres}</i>
<b>├・🎞️</b> <i>Episode:</i>   <code>{ep_no}</code>
<b>╰・🔊</b> <i>Audio/Subs:</i> <code>Japanese | English</code>

<blockquote>✦ <i>Powered By</i> {cred} ✦</blockquote>
"""


GENRES_EMOJI = {
    "Action": choice(["👊", "💥", "🔫"]),
    "Adventure": choice(["🪂", "🧗‍♀️", "🗺️"]),
    "Comedy": choice(["🤣", "😂", "😆"]),
    "Drama": choice(["🎭", "🎬", "🍿"]),
    "Ecchi": choice(["💋", "🥵", "👙"]),
    "Fantasy": choice(["🧞", "🧚", "🌌"]),
    "Hentai": "🔞",
    "Horror": choice(["☠️", "👻", "🩸"]),
    "Mahou Shoujo": choice(["☯️", "✨", "🌟"]),
    "Mecha": choice(["🤖", "🚀", "⚙️"]),
    "Music": choice(["🎸", "🎹", "🎤"]),
    "Mystery": choice(["🔮", "🕵️", "🧩"]),
    "Psychological": choice(["♟️", "🧠", "🌀"]),
    "Romance": choice(["💞", "💘", "🥰"]),
    "Sci-Fi": choice(["🛸", "👽", "🚨"]),
    "Slice of Life": choice(["☘️", "🍁", "🏡"]),
    "Sports": choice(["⚽️", "🏀", "🎾"]),
    "Supernatural": choice(["🫧", "🌠", "⚡"]),
    "Thriller": choice(["🥶", "🔪", "😱"]),
    "Isekai": choice(["🌐", "🌀", "🚪"]),
    "Shounen": choice(["💪", "🔥", "⚔️"]),
    "Shoujo": choice(["💝", "🌸", "🎀"]),
    "Seinen": choice(["🎩", "📚", "🧐"]),
    "Josei": choice(["💄", "👠", "💍"]),
    "Military": choice(["🎖️", "🪖", "🔫"]),
    "Police": choice(["👮", "🚔", "🚨"]),
    "Demons": choice(["👹", "😈", "👺"]),
    "Magic": choice(["🔮", "🪄", "✨"]),
    "Super Power": choice(["💫", "🦸", "⚡"]),
    "Vampire": choice(["🧛", "🦇", "🍷"]),
    "Yaoi": "👬",
    "Yuri": "👭",
    "Harem": choice(["💕", "👨‍👩‍👧‍👦", "💑"]),
    "Historical": choice(["🏛️", "⏳", "🗡️"]),
    "Martial Arts": choice(["🥋", "👊", "💪"]),
    "Parody": choice(["🃏", "🎭", "🤹"]),
    "Samurai": choice(["🗡️", "🥷", "⚔️"]),
    "School": choice(["🎒", "📚", "✏️"]),
    "Space": choice(["🚀", "👽", "🪐"]),
    "Zombies": choice(["🧟", "☠️", "🧠"])
}
ANIME_GRAPHQL_QUERY = """
query ($id: Int, $search: String, $seasonYear: Int) {
  Media(id: $id, type: ANIME, format_not_in: [MOVIE, MUSIC, MANGA, NOVEL, ONE_SHOT], search: $search, seasonYear: $seasonYear) {
    id
    idMal
    title {
      romaji
      english
      native
    }
    type
    format
    status(version: 2)
    description(asHtml: false)
    startDate {
      year
      month
      day
    }
    endDate {
      year
      month
      day
    }
    season
    seasonYear
    episodes
    duration
    chapters
    volumes
    countryOfOrigin
    source
    hashtag
    trailer {
      id
      site
      thumbnail
    }
    updatedAt
    coverImage {
      large
    }
    bannerImage
    genres
    synonyms
    averageScore
    meanScore
    popularity
    trending
    favourites
    studios {
      nodes {
         name
         siteUrl
      }
    }
    isAdult
    nextAiringEpisode {
      airingAt
      timeUntilAiring
      episode
    }
    airingSchedule {
      edges {
        node {
          airingAt
          timeUntilAiring
          episode
        }
      }
    }
    externalLinks {
      url
      site
    }
    siteUrl
  }
}
"""

class AniLister:
    def __init__(self, anime_name: str, year: int) -> None:
        self.__api = "https://graphql.anilist.co"
        self.__ani_name = anime_name
        self.__ani_year = year
        self.__vars = {'search' : self.__ani_name, 'seasonYear': self.__ani_year}
    
    def __update_vars(self, year=True) -> None:
        if year:
            self.__ani_year -= 1
            self.__vars['seasonYear'] = self.__ani_year
        else:
            self.__vars = {'search' : self.__ani_name}
    
    async def post_data(self):
        async with ClientSession() as sess:
            async with sess.post(self.__api, json={'query': ANIME_GRAPHQL_QUERY, 'variables': self.__vars}) as resp:
                return (resp.status, await resp.json(), resp.headers)
        
    async def get_anidata(self):
        res_code, resp_json, res_heads = await self.post_data()
        while res_code == 404 and self.__ani_year > 2020:
            self.__update_vars()
            await rep.report(f"AniList Query Name: {self.__ani_name}, Retrying with {self.__ani_year}", "warning", log=False)
            res_code, resp_json, res_heads = await self.post_data()
        
        if res_code == 404:
            self.__update_vars(year=False)
            res_code, resp_json, res_heads = await self.post_data()
        
        if res_code == 200:
            return resp_json.get('data', {}).get('Media', {}) or {}
        elif res_code == 429:
            f_timer = int(res_heads['Retry-After'])
            await rep.report(f"AniList API FloodWait: {res_code}, Sleeping for {f_timer} !!", "error")
            await asleep(f_timer)
            return await self.get_anidata()
        elif res_code in [500, 501, 502]:
            await rep.report(f"AniList Server API Error: {res_code}, Waiting 5s to Try Again !!", "error")
            await asleep(5)
            return await self.get_anidata()
        else:
            await rep.report(f"AniList API Error: {res_code}", "error", log=False)
            return {}
    
class TextEditor:
    def __init__(self, name):
        self.__name = name
        self.adata = {}
        self.pdata = parse(name)

    async def load_anilist(self):
        cache_names = []
        for option in [(False, False), (False, True), (True, False), (True, True)]:
            ani_name = await self.parse_name(*option)
            if ani_name in cache_names:
                continue
            cache_names.append(ani_name)
            self.adata = await AniLister(ani_name, datetime.now().year).get_anidata()
            if self.adata:
                break

    @handle_logs
    async def get_id(self):
        if (ani_id := self.adata.get('id')) and str(ani_id).isdigit():
            return ani_id
            
    @handle_logs
    async def parse_name(self, no_s=False, no_y=False):
        anime_name = self.pdata.get("anime_title")
        anime_season = self.pdata.get("anime_season")
        anime_year = self.pdata.get("anime_year")
        if anime_name:
            pname = anime_name
            if not no_s and self.pdata.get("episode_number") and anime_season:
                pname += f" {anime_season}"
            if not no_y and anime_year:
                pname += f" {anime_year}"
            return pname
        return anime_name
        
    @handle_logs
    async def get_poster(self):
        if anime_id := await self.get_id():
            return f"https://img.anili.st/media/{anime_id}"
        return "https://telegra.ph/file/112ec08e59e73b6189a20.jpg"
        
    @handle_logs
    async def get_upname(self, qual=""):
        anime_name = self.pdata.get("anime_title")
        codec = 'HEVC' if 'libx265' in ffargs[qual] else 'AV1' if 'libaom-av1' in ffargs[qual] else ''
        lang = 'Multi-Audio' if 'multi-audio' in self.__name.lower() else 'Sub'
        anime_season = str(ani_s[-1]) if (ani_s := self.pdata.get('anime_season', '01')) and isinstance(ani_s, list) else str(ani_s)
        if anime_name and self.pdata.get("episode_number"):
            titles = self.adata.get('title', {})
            return f"""[S{anime_season}-{'E'+str(self.pdata.get('episode_number')) if self.pdata.get('episode_number') else ''}] {titles.get('english') or titles.get('romaji') or titles.get('native')} {'['+qual+'p]' if qual else ''} {'['+codec.upper()+'] ' if codec else ''}{'['+lang+']'} {Var.BRAND_UNAME}.mkv"""

    @handle_logs
    async def get_caption(self):
        sd = self.adata.get('startDate', {})
        startdate = f"{month_name[sd['month']]} {sd['day']}, {sd['year']}" if sd.get('day') and sd.get('year') else ""
        ed = self.adata.get('endDate', {})
        enddate = f"{month_name[ed['month']]} {ed['day']}, {ed['year']}" if ed.get('day') and ed.get('year') else ""
        titles = self.adata.get("title", {})
        
        return CAPTION_FORMAT.format(
                title=titles.get('english') or titles.get('romaji') or title.get('native'),
                form=self.adata.get("format") or "N/A",
                genres=", ".join(f"{GENRES_EMOJI[x]} #{x.replace(' ', '_').replace('-', '_')}" for x in (self.adata.get('genres') or [])),
                avg_score=f"{sc}%" if (sc := self.adata.get('averageScore')) else "N/A",
                status=self.adata.get("status") or "N/A",
                start_date=startdate or "N/A",
                end_date=enddate or "N/A",
                t_eps=self.adata.get("episodes") or "N/A",
                plot= (desc if (desc := self.adata.get("description") or "N/A") and len(desc) < 200 else desc[:200] + "..."),
                ep_no=self.pdata.get("episode_number"),
                cred=Var.BRAND_UNAME,
            )
