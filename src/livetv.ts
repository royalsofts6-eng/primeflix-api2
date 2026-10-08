/**
 * PrimeFlix Live TV — auto-updating channel system.
 *
 * Design:
 * - Curated channel list (static, always available as fallback)
 * - 12h refresh: fetch dearbulut health-checked M3U playlists,
 *   match curated channels by name, probe streams, cache results.
 * - GET /v1/livetv/channels serves from cache (fast, private-cached).
 * - GET /v1/cron/livetv-refresh triggers the refresh (external cron;
 *   bounded to complete inside Vercel's 60s maxDuration).
 *
 * There is NO background refresh on serverless: fire-and-forget promises do
 * not survive the invocation, so getChannels() never pretends to refresh —
 * it serves cache/CURATED and the cron endpoint is the only refresh trigger.
 */

import { cacheGet, cacheSet } from "./cache.js";

// ── Types ───────────────────────────────────────────────────────────────────

export interface Channel {
  id: string;
  name: string;
  category: ChannelCategory;
  country: "pk" | "in" | "int";
  type: "hls" | "youtube";
  url: string;
  fallbacks: string[];
  logo?: string;
}

export type ChannelCategory =
  | "pk-entertainment"
  | "pk-movies"
  | "pk-sports"
  | "pk-news"
  | "in-entertainment"
  | "in-movies"
  | "sports"
  | "in-news"
  | "islamic";

export const CATEGORY_LABELS: Record<ChannelCategory, string> = {
  "pk-entertainment": "Pakistani Entertainment",
  "pk-movies": "Pakistani Movies",
  "pk-sports": "Pakistani Sports",
  "pk-news": "Pakistani News",
  "in-entertainment": "Indian Entertainment",
  "in-movies": "Indian Movies",
  sports: "Sports & Cricket",
  "in-news": "Indian News",
  islamic: "Islamic",
};

// ── Curated channel list ────────────────────────────────────────────────────
// Primary URLs from verified research (2026-10-08). The 12h refresh job
// replaces expiring/signed URLs with fresh ones from health-checked playlists.

const CURATED: Channel[] = [
  // — Pakistani Entertainment —
  {
    id: "a-plus-tv",
    name: "A-Plus TV",
    category: "pk-entertainment",
    country: "pk",
    type: "hls",
    url: "https://cdn4.mjunoon.tv:8087/streamtest/118M/chunks.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/uu0UJme.png",
  },
  {
    id: "hum-tv",
    name: "Hum TV",
    category: "pk-entertainment",
    country: "pk",
    type: "hls",
    url: "https://g4wlkwx8l23a-hls-live.5centscdn.com/HUM/271ddf829afeece44d8732757fba1a66.sdp/playlist.m3u8",
    fallbacks: [],
    logo: "https://i.ibb.co/Tx4GfKT5/Hum-TV-HD.png",
  },
  {
    id: "express-entertainment",
    name: "Express Entertainment",
    category: "pk-entertainment",
    country: "pk",
    type: "hls",
    url: "https://ml-pull-dvc-myco.io:2096/EXPRESS_ENTERTAINMENT/index.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/rgHbb8W.png",
  },
  {
    id: "atv-pk",
    name: "ATV",
    category: "pk-entertainment",
    country: "pk",
    type: "hls",
    url: "",
    fallbacks: [],
    logo: "https://upload.wikimedia.org/wikipedia/en/8/83/Atv_pakistan.PNG",
  },
  {
    id: "bol-entertainment",
    name: "Bol Entertainment",
    category: "pk-entertainment",
    country: "pk",
    type: "hls",
    url: "https://vodzong.mjunoon.tv:8087/streamtest/Channel5-159-4/playlist.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/eajSRFo.png",
  },
  {
    id: "aaj-entertainment",
    name: "Aaj Entertainment",
    category: "pk-entertainment",
    country: "pk",
    type: "hls",
    url: "https://ml-pull-dvc-myco.io:2096/AAJ_ENTERTAINMENT/index.m3u8",
    fallbacks: [],
    logo: "https://i.ibb.co/xt5RBDds/Aaj-Entertainment-HD.png",
  },
  {
    id: "harpal-geo",
    name: "HarPal Geo",
    category: "pk-entertainment",
    country: "pk",
    type: "hls",
    url: "https://jk3lz82elw79-hls-live.5centscdn.com/harPalGeo/955ad3298db330b5ee880c2c9e6f23a0.sdp/playlist.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/NX3vvAX.png",
  },
  {
    id: "8xm",
    name: "8XM",
    category: "pk-entertainment",
    country: "pk",
    type: "hls",
    url: "https://cdn4.mjunoon.tv:8087/streamtest/131M/chunks.m3u8",
    fallbacks: [],
    logo: "https://i.ibb.co/Kc0xHyBb/8XM-Logo.png",
  },
  {
    id: "discover-pakistan",
    name: "Discover Pakistan",
    category: "pk-entertainment",
    country: "pk",
    type: "hls",
    url: "https://livecdn.live247stream.com/discoverpakistan/web/playlist.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/IJH47fJ.png",
  },

  // — Pakistani Movies —
  {
    id: "filmax",
    name: "Filmax",
    category: "pk-movies",
    country: "pk",
    type: "hls",
    url: "https://s3.ideationtec.live/Filmax/Filmax.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/eLmdZ6k.png",
  },
  {
    id: "bs-film",
    name: "BS Film",
    category: "pk-movies",
    country: "pk",
    type: "hls",
    url: "https://live20.bozztv.com/akamaissh101/ssh101/bsfilm/playlist.m3u8",
    fallbacks: [],
    logo: "https://www.vivalivetv.com/public/files/shows/0/1/3953-640x360-FFFFFF.jpg",
  },
  {
    id: "filmazia",
    name: "Filmazia",
    category: "pk-movies",
    country: "pk",
    type: "hls",
    url: "http://103.250.28.74:8000/play/a02k/index.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/hpTCANa.png",
  },

  // — Pakistani Sports —
  {
    id: "m-sports",
    name: "M Sports",
    category: "pk-sports",
    country: "pk",
    type: "hls",
    url: "https://cdn.rabta.stream/M-Sports/index.m3u8",
    fallbacks: [],
    logo: "https://msports.pk/wp-content/uploads/2026/02/m-sports-300.png",
  },
  {
    id: "ptv-sports",
    name: "PTV Sports",
    category: "pk-sports",
    country: "pk",
    type: "hls",
    url: "https://tvsen7.aynascope.net/Sports1/index.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/CPm6GHA.png",
  },
  {
    id: "pk-sports",
    name: "PK Sports",
    category: "pk-sports",
    country: "pk",
    type: "hls",
    url: "https://lbgo.bozztv.com/ssh101/ssh101/pksportshd/playlist.m3u8",
    fallbacks: [],
    logo: "https://raw.githubusercontent.com/songwenhui239/Songwenhui239/refs/heads/main/PK%20Sports.jpeg",
  },

  // — Pakistani News (verified HLS 2026-10-08) —
  {
    id: "dunya-news",
    name: "Dunya News",
    category: "pk-news",
    country: "pk",
    type: "hls",
    url: "https://intl.dunyanews.tv/livehd/ngrp:dunyalivehd_2_all/playlist.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/1PbtW0y.png",
  },
  {
    id: "92-news",
    name: "92 News HD",
    category: "pk-news",
    country: "pk",
    type: "hls",
    url: "http://92news.vdn.dstreamone.net/92newshd/92hd/playlist.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/gp1Ao4s.jpeg",
  },
  {
    id: "samaa-tv",
    name: "Samaa TV",
    category: "pk-news",
    country: "pk",
    type: "hls",
    url: "https://vodzong.mjunoon.tv:8087/streamtest/SAMAA-173/playlist.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/r3U4A1P.png",
  },
  {
    id: "geo-news",
    name: "Geo News",
    category: "pk-news",
    country: "pk",
    type: "youtube",
    url: "https://www.youtube.com/@GeoNews/live",
    fallbacks: [],
  },
  {
    id: "ary-news",
    name: "ARY News",
    category: "pk-news",
    country: "pk",
    type: "hls",
    url: "https://cdn07lhr.tamashaweb.com:8087/jazzauth/vsat-arynews-abr/live/vsat-arynews-H/chunks_dvr_timeshift-0-3600.m3u8",
    fallbacks: ["https://www.youtube.com/@ARYNews/live"],
    logo: "https://i.imgur.com/R4KtTbJ.jpg",
  },
  {
    id: "24-news-hd",
    name: "24 News HD",
    category: "pk-news",
    country: "pk",
    type: "hls",
    url: "https://cdn4.mjunoon.tv:8087/streamtest/146M/chunks.m3u8",
    fallbacks: [],
    logo: "https://upload.wikimedia.org/wikipedia/en/9/93/24_News_HD_Logo.png",
  },
  {
    id: "capital-tv",
    name: "Capital TV",
    category: "pk-news",
    country: "pk",
    type: "hls",
    url: "https://cdn4.mjunoon.tv:8087/streamtest/111M/chunks.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/UxAE5O4.png",
  },
  {
    id: "neo-news",
    name: "Neo News",
    category: "pk-news",
    country: "pk",
    type: "hls",
    url: "https://vodzong.mjunoon.tv:8087/streamtest/Neo-110/playlist.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/9mrbPRs.png",
  },
  {
    id: "news-one",
    name: "News One",
    category: "pk-news",
    country: "pk",
    type: "hls",
    url: "https://vodzong.mjunoon.tv:8087/streamtest/NEWS1-128/playlist.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/ivrRYMk.png",
  },
  {
    id: "lahore-news",
    name: "Lahore News",
    category: "pk-news",
    country: "pk",
    type: "hls",
    url: "https://vcdn.dunyanews.tv/lahorelive/ngrp:lnews_1_all/playlist.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/bQfQeEA.jpeg",
  },
  {
    id: "ktn-news",
    name: "KTN News",
    category: "pk-news",
    country: "pk",
    type: "hls",
    url: "https://vodzong.mjunoon.tv:8087/streamtest/KTNNews-151/playlist.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/RtEzpPQ.png",
  },
  {
    id: "such-tv",
    name: "Such TV",
    category: "pk-news",
    country: "pk",
    type: "hls",
    url: "https://video.primexsports.com/suchnews/live/playlist.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/yYMh3JJ.png",
  },

  // — Indian Entertainment —
  {
    id: "star-plus",
    name: "StarPlus",
    category: "in-entertainment",
    country: "in",
    type: "hls",
    url: "http://51.75.127.199:3141/starplushd/index.m3u8",
    fallbacks: [],
    logo: "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/india/star-plus-in.png",
  },
  {
    id: "colors-tv",
    name: "Colors",
    category: "in-entertainment",
    country: "in",
    type: "hls",
    url: "http://51.75.127.199:3141/colorssd/index.m3u8",
    fallbacks: [],
  },
  {
    id: "sony-tv",
    name: "Sony TV",
    category: "in-entertainment",
    country: "in",
    type: "hls",
    url: "http://38.96.178.205/SONYHD/index.m3u8",
    fallbacks: [],
    logo: "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/india/sony-entertainment-television-in.png",
  },
  {
    id: "star-utsav",
    name: "Star Utsav",
    category: "in-entertainment",
    country: "in",
    type: "hls",
    url: "http://51.75.127.199:3141/starutsav/index.m3u8",
    fallbacks: [],
    logo: "https://dtil.tmsimg.com/assets/s159132_ld_h15_aa.png?lock=720x540",
  },
  {
    id: "zee-tv",
    name: "Zee TV",
    category: "in-entertainment",
    country: "in",
    type: "hls",
    url: "http://51.75.127.199:3141/zeetv/index.m3u8",
    fallbacks: [],
    logo: "https://xstreamcp-assets-msp.streamready.in/assets/LIVETV/LIVECHANNEL/LIVETV_LIVETVCHANNEL_ZEE_TV/images/LOGO_HD/LOGO_HD_image.png",
  },
  {
    id: "star-bharat",
    name: "Star Bharat",
    category: "in-entertainment",
    country: "in",
    type: "hls",
    url: "http://51.75.127.199:3141/starbharat/index.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/Q8ajPij.png",
  },
  {
    id: "9x-jalwa",
    name: "9X Jalwa",
    category: "in-entertainment",
    country: "in",
    type: "hls",
    url: "https://wiselp.wiseplayout.com/9X_Jalwa/master.m3u8",
    fallbacks: [],
    logo: "https://xstreamcp-assets-msp.streamready.in/assets/LIVETV/LIVECHANNEL/LIVETV_LIVETVCHANNEL_9X_JALWA/images/LOGO_HD/image.png",
  },
  {
    id: "b4u-music",
    name: "B4U Music",
    category: "in-entertainment",
    country: "in",
    type: "hls",
    url: "https://amg01408-amg01408c5-amgplt0747.playout.now3.amagi.tv/b4um001/playlist.m3u8",
    fallbacks: [],
    logo: "https://dtil.tmsimg.com/assets/s158141_ld_h15_aa.png?lock=720x540",
  },
  {
    id: "zoom-tv",
    name: "Zoom",
    category: "in-entertainment",
    country: "in",
    type: "hls",
    url: "https://dai.google.com/linear/hls/event/JCAm25qkRXiKcK1AJMlvKQ/master.m3u8",
    fallbacks: [],
    logo: "https://xstreamcp-assets-msp.streamready.in/assets/LIVETV/LIVECHANNEL/LIVETV_LIVETVCHANNEL_ZOOM/images/LOGO_HD/image.png",
  },

  // — Indian Movies —
  {
    id: "star-gold",
    name: "Star Gold",
    category: "in-movies",
    country: "in",
    type: "hls",
    url: "http://51.75.127.199:3141/stargoldselecthd/index.m3u8",
    fallbacks: [],
    logo: "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/india/star-gold-in.png",
  },
  {
    id: "sony-max",
    name: "Sony Max",
    category: "in-movies",
    country: "in",
    type: "hls",
    url: "http://107.167.16.138/sonymax2/index.m3u8?token=test",
    fallbacks: [],
    logo: "https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/india/sony-max-in.png",
  },
  {
    id: "zee-cinema",
    name: "Zee Cinema",
    category: "in-movies",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },
  {
    id: "colors-cineplex",
    name: "Colors Cineplex",
    category: "in-movies",
    country: "in",
    type: "hls",
    url: "http://51.75.127.199:3141/colorscineplexhd/index.m3u8",
    fallbacks: ["http://51.75.127.199:3141/colorscineplexbollywood/index.m3u8"],
  },
  {
    id: "all-time-movies",
    name: "All Time Movies",
    category: "in-movies",
    country: "in",
    type: "hls",
    url: "https://samitaorigin.tangotv.in/ALLTIMEMOVIES/SAMITAORIGIN/index.m3u8",
    fallbacks: [],
    logo: "https://yt3.googleusercontent.com/U4INXhwmEUOABHoemQBpI6C9t4jb9iBmDvZ3ZT3lAb9Au_jVl32NL8XDpy-9cBjRJ2LP69Ovzg=s900-c-k-c0x00ffffff-no-rj",
  },
  {
    id: "star-utsav-movies",
    name: "Star Utsav Movies",
    category: "in-movies",
    country: "in",
    type: "hls",
    url: "http://51.75.127.199:3141/starutsavmovies/index.m3u8",
    fallbacks: [],
    logo: "https://dtil.tmsimg.com/assets/s143856_ld_h15_aa.png?lock=720x540",
  },
  {
    id: "b4u-movies",
    name: "B4U Movies",
    category: "in-movies",
    country: "in",
    type: "hls",
    url: "https://streams.tangotv.in/B4UMOVIES/ORIGIN/index.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/M9kMFJl.png",
  },

  // — Sports & Cricket —
  {
    id: "willow",
    name: "Willow",
    category: "sports",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },
  {
    id: "star-sports-2",
    name: "Star Sports 2",
    category: "sports",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
    logo: "https://img10.hotstar.com/image/upload/f_auto/sources/r1/cms/prod/7957/1783000567957-h.jpg",
  },
  {
    id: "sony-ten-1",
    name: "Sony Ten 1",
    category: "sports",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },
  {
    id: "dd-sports",
    name: "DD Sports",
    category: "sports",
    country: "in",
    type: "hls",
    url: "https://mumbai-edge.smartplaytv.in/DDSports/index.m3u8",
    fallbacks: [],
    logo: "https://ltsk-cdn.s3.eu-west-1.amazonaws.com/jumpstart/Temp_Live/cdn/HLS/Channel/transparentImages/DD%20Sports.png",
  },
  {
    id: "star-sports-select-1",
    name: "Star Sports Select 1",
    category: "sports",
    country: "in",
    type: "hls",
    url: "http://103.151.60.162:2122/play/a026/index.m3u8?hls",
    fallbacks: [],
    logo: "https://img10.hotstar.com/image/upload/f_auto/sources/r1/cms/prod/1176/1783001141176-h.jpg",
  },
  {
    id: "star-sports-select-2",
    name: "Star Sports Select 2",
    category: "sports",
    country: "in",
    type: "hls",
    url: "http://103.151.60.162:2122/play/a027/index.m3u8?hls",
    fallbacks: [],
    logo: "https://img10.hotstar.com/image/upload/f_auto/sources/r1/cms/prod/7266/1783001217266-h.jpg",
  },
  {
    id: "ten-cricket",
    name: "Ten Cricket",
    category: "sports",
    country: "in",
    type: "hls",
    url: "http://103.151.60.162:2122/play/a0fj/index.m3u8?hls",
    fallbacks: [],
    logo: "https://i.imgur.com/K5XIFuW.png",
  },
  {
    id: "cricket-gold",
    name: "Cricket Gold",
    category: "sports",
    country: "in",
    type: "hls",
    url: "",
    fallbacks: [],
  },

  // — Indian News —
  {
    id: "abp-news",
    name: "ABP News",
    category: "in-news",
    country: "in",
    type: "hls",
    url: "https://d1rc86nwwc9fag.cloudfront.net/vglive-sk-472500/abpnews/master.m3u8",
    fallbacks: [],
    logo: "https://upload.wikimedia.org/wikipedia/commons/thumb/4/48/ABP_News_logo.svg/500px-ABP_News_logo.svg.png",
  },
  {
    id: "aaj-tak",
    name: "Aaj Tak",
    category: "in-news",
    country: "in",
    type: "hls",
    url: "https://livehub-voidnet.onrender.com/cluster/streamcore/in/AAJTAK_REDIS.m3u8",
    fallbacks: ["https://www.youtube.com/@aajtak/live"],
    logo: "https://i.imgur.com/gS9Qkfy.png",
  },
  {
    id: "india-tv",
    name: "India TV",
    category: "in-news",
    country: "in",
    type: "hls",
    url: "https://pl-indiatvnews.akamaized.net/out/v1/db79179b608641ceaa5a4d0dd0dca8da/index.m3u8",
    fallbacks: [],
    logo: "https://xstreamcp-assets-msp.streamready.in/assets/LIVETV/LIVECHANNEL/LIVETV_LIVETVCHANNEL_INDIA_TV/images/LOGO_HD/image.png",
  },
  {
    id: "zee-news",
    name: "Zee News",
    category: "in-news",
    country: "in",
    type: "hls",
    url: "https://dknttpxmr0dwf.cloudfront.net/index_57.m3u8",
    fallbacks: [],
    logo: "https://dtil.tmsimg.com/assets/GNLZZGG0023VWYC.png?lock=720x540",
  },

  // — Islamic (verified live 2026-10-08) —
  {
    id: "makkah-tv",
    name: "Makkah TV",
    category: "islamic",
    country: "int",
    type: "hls",
    url: "https://media2.streambrothers.com:1936/8122/8122/playlist.m3u8",
    fallbacks: [],
    logo: "https://i.ibb.co/r2k71w5N/download.png",
  },
  {
    id: "madani-channel-urdu",
    name: "Madani Channel Urdu",
    category: "islamic",
    country: "pk",
    type: "hls",
    url: "https://streaming.madanichannel.tv/static/streaming-playlists/hls/b9790f10-cb0d-4e30-82bf-84a756234e58/master.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/MitLeCJ.png",
  },
  {
    id: "madani-channel-english",
    name: "Madani Channel English",
    category: "islamic",
    country: "pk",
    type: "hls",
    url: "http://tvsen7.aynascope.net/MadaniTV/index.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/Abi9j0A.png",
  },
  {
    id: "peace-tv-urdu",
    name: "Peace TV Urdu",
    category: "islamic",
    country: "pk",
    type: "hls",
    url: "https://dzkyvlfyge.erbvr.com/PeaceTvUrdu/index.m3u8",
    fallbacks: [],
    logo: "https://github.com/fawazahmed0/tiger/raw/master/peace/urdu.jpg",
  },
  {
    id: "peace-tv-english",
    name: "Peace TV English",
    category: "islamic",
    country: "int",
    type: "hls",
    url: "https://dzkyvlfyge.erbvr.com/PeaceTvEnglish/index.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/rjgCM2B.png",
  },
  {
    id: "huda-tv",
    name: "Huda TV",
    category: "islamic",
    country: "int",
    type: "hls",
    url: "https://cdn.bestream.io:19360/elfaro1/elfaro1.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/1UUjU26.png",
  },
  {
    id: "iqraa-quran",
    name: "Iqraa Quran",
    category: "islamic",
    country: "int",
    type: "hls",
    url: "https://playlist.fasttvcdn.com/pl/dlkqw1ftuvuuzkcb4pxdcg/Iqraafasttv2/playlist.m3u8",
    fallbacks: [],
    logo: "https://i.imgur.com/HPVsIa4.png",
  },
  {
    id: "quran-tv",
    name: "Quran TV",
    category: "islamic",
    country: "int",
    type: "hls",
    url: "https://ncdn.telewebion.ir/quran/live/playlist.m3u8",
    fallbacks: [],
    logo: "https://upload.wikimedia.org/wikipedia/fa/d/df/Quarntvlogo.png",
  },
];

// ── Playlist sources ────────────────────────────────────────────────────────

const PLAYLISTS = {
  pk: "https://dearbulut.github.io/iptv/playlists/country/pk.m3u",
  in: "https://dearbulut.github.io/iptv/playlists/country/in.m3u",
};

const CACHE_KEY = "livetv:channels:v1";
const TTL_MS = 12 * 60 * 60 * 1000; // 12h
const STALE_MS = 7 * 24 * 60 * 60 * 1000; // 7d stale fallback

// ── M3U parsing ─────────────────────────────────────────────────────────────

interface PlaylistEntry {
  name: string;
  url: string;
  logo?: string;
  group?: string;
}

function parseM3U(text: string): PlaylistEntry[] {
  const entries: PlaylistEntry[] = [];
  const lines = text.split("\n");
  let pending: Partial<PlaylistEntry> | null = null;

  for (const raw of lines) {
    const line = raw.trim();
    if (line.startsWith("#EXTINF")) {
      // #EXTINF:-1 tvg-logo="..." group-title="...",Channel Name
      const nameMatch = line.match(/,(.*)$/);
      const logoMatch = line.match(/tvg-logo="([^"]*)"/);
      const groupMatch = line.match(/group-title="([^"]*)"/);
      pending = {
        name: (nameMatch?.[1] || "").trim(),
        logo: logoMatch?.[1] || undefined,
        group: groupMatch?.[1] || undefined,
      };
    } else if (line && !line.startsWith("#") && pending) {
      if (line.startsWith("http")) {
        entries.push({ name: pending.name || "Unknown", url: line, logo: pending.logo, group: pending.group });
      }
      pending = null;
    }
  }
  return entries;
}

// Normalize names for fuzzy matching: "Star Plus HD" -> "starplus"
function norm(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]/g, "");
}

// ── HTTP helpers ────────────────────────────────────────────────────────────

async function fetchText(url: string, timeoutMs = 15000): Promise<string | null> {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: ctrl.signal,
      headers: { "User-Agent": "PrimeFlix/1.0" },
    });
    if (!res.ok) return null;
    return await res.text();
  } catch {
    return null;
  } finally {
    clearTimeout(t);
  }
}

/**
 * Probe a stream URL. Returns true if it looks alive.
 * Uses GET with a Range header instead of HEAD — many HLS servers
 * mishandle HEAD (405/hang), which caused false "dead" markings.
 */
async function probeUrl(url: string, timeoutMs = 8000): Promise<boolean> {
  if (!url || !url.startsWith("http")) return false;
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: "GET",
      signal: ctrl.signal,
      headers: { "User-Agent": "PrimeFlix/1.0", Range: "bytes=0-1023" },
      redirect: "follow",
    });
    // Abort immediately — headers (200/206) are all we need.
    ctrl.abort();
    return res.ok;
  } catch {
    return false;
  } finally {
    clearTimeout(t);
  }
}

// ── Refresh pipeline ────────────────────────────────────────────────────────

export interface RefreshResult {
  refreshedAt: number;
  total: number;
  alive: number;
  channels: Channel[];
}

// Refresh budget: the cron invocation must complete inside Vercel maxDuration
// (60s, verified 2026-10-08). Whatever hasn't finished by the deadline keeps
// its previous known-good state (anti-downgrade), and partial results are
// written to cache — a refresh never returns empty-handed.
const REFRESH_BUDGET_MS = 50000;
// Concurrency caps: 63 channels × ~8 candidates with unbounded Promise.all
// used to fire hundreds of simultaneous fetches. Now: 6 channels at a time,
// 4 concurrent probes per channel.
const CHANNEL_BATCH = 6;
const PROBE_CONCURRENCY = 4;
const PROBE_TIMEOUT_MS = 6000;

/** Tiny promise pool. */
function pLimit(n: number) {
  let active = 0;
  const queue: (() => void)[] = [];
  const release = () => {
    active--;
    const f = queue.shift();
    if (f) f();
  };
  return async <T>(fn: () => Promise<T>): Promise<T> => {
    if (active >= n) await new Promise<void>((res) => queue.push(res));
    active++;
    try {
      return await fn();
    } finally {
      release();
    }
  };
}

const WATCH_HOSTS = /(^|\.)(youtube\.com|youtu\.be|facebook\.com|fb\.watch|dailymotion\.com|vimeo\.com)$/i;

/**
 * HLS channels may only ever be fed real stream URLs. Playlist candidates are
 * never filtered by content-type, and probeUrl() returns true for ANY http
 * 200 — including youtube.com watch pages. Without this filter a YouTube
 * page could become an HLS channel's `url` and ExoPlayer would choke on HTML.
 */
function isHlsCandidate(url: string): boolean {
  let host = "";
  try {
    host = new URL(url).hostname.toLowerCase();
  } catch {
    return false;
  }
  if (WATCH_HOSTS.test(host)) return false;
  return /\.m3u8(\?|#|$)/i.test(url);
}

/**
 * Refresh ONE channel: harvest playlist candidates, probe, pick best.
 * Returns null when it produced nothing usable — the caller then keeps the
 * previous known-good state for that channel.
 */
async function refreshOne(
  ch: Channel,
  index: Map<string, PlaylistEntry[]>,
  probeLimit: <T>(fn: () => Promise<T>) => Promise<T>
): Promise<Channel | null> {
  const key = norm(ch.name);
  const candidates: { url: string; logo?: string }[] = [];
  const seen = new Set<string>();

  const addCandidate = (e: PlaylistEntry) => {
    // HLS channels: only real .m3u8 stream URLs are eligible (P1-5).
    // YouTube-type channels resolve at play time and skip this path.
    if (ch.type === "hls" && !isHlsCandidate(e.url)) return;
    if (!seen.has(e.url)) {
      seen.add(e.url);
      candidates.push({ url: e.url, logo: e.logo });
    }
  };

  // Direct name match + common variants
  const variants = [key, key.replace(/tv$/, ""), key.replace(/^sony/, "set")];
  for (const v of variants) {
    const entries = index.get(v);
    if (entries) {
      for (const e of entries) addCandidate(e);
    }
  }

  // Also try partial matching (e.g. "starplus" in "starplushd")
  if (candidates.length === 0) {
    for (const [k, entries] of index) {
      if (k.includes(key) || key.includes(k)) {
        for (const e of entries) {
          if (candidates.length < 5) addCandidate(e);
        }
      }
    }
  }

  // Logo: curated first, else first playlist tvg-logo found.
  const harvestedLogo = ch.logo || candidates.find((c) => c.logo)?.logo || "";

  // Keep curated URL as first candidate (it's verified)
  const all = ch.url ? [{ url: ch.url, logo: ch.logo }, ...candidates] : candidates;

  // YouTube channels: no probing (resolved at play time)
  if (ch.type === "youtube") {
    return { ...ch, fallbacks: candidates.slice(0, 3).map((c) => c.url), logo: harvestedLogo };
  }

  // Probe with bounded concurrency
  const probes = await Promise.all(
    all.map((u) => probeLimit(() => probeUrl(u.url, PROBE_TIMEOUT_MS)))
  );
  const alive = all.filter((_, i) => probes[i]);

  return {
    ...ch,
    url: alive[0]?.url || "",
    fallbacks: alive.slice(1, 4).map((a) => a.url),
    logo: harvestedLogo,
  };
}

/**
 * Fetch health-checked playlists, match curated channels, probe streams.
 * Triggered ONLY by /v1/cron/livetv-refresh (external cron). Completes
 * inside the 60s maxDuration: bounded concurrency + 50s deadline + partial
 * results written early.
 */
export async function refreshChannels(): Promise<RefreshResult> {
  const deadline = Date.now() + REFRESH_BUDGET_MS;

  // 1. Fetch both playlists in parallel (inside the budget)
  const [pkText, inText] = await Promise.all([
    fetchText(PLAYLISTS.pk, 10000),
    fetchText(PLAYLISTS.in, 10000),
  ]);

  // 2. Build name -> entries index
  const index = new Map<string, PlaylistEntry[]>();
  for (const text of [pkText, inText]) {
    if (!text) continue;
    for (const e of parseM3U(text)) {
      const key = norm(e.name);
      const arr = index.get(key) || [];
      arr.push(e);
      index.set(key, arr);
    }
  }

  // 3. Refresh channels in bounded batches. Baseline = previous known-good
  // state (or curated); each batch races the REMAINING budget, so the whole
  // refresh can never overrun maxDuration. Batches that don't finish in time
  // simply keep their baseline — partial results are always written.
  const prev = cacheGet<RefreshResult>(CACHE_KEY)?.value;
  const prevById = new Map((prev?.channels || []).map((c) => [c.id, c]));
  const probeLimit = pLimit(PROBE_CONCURRENCY);
  const channels: Channel[] = CURATED.map((ch) => prevById.get(ch.id) || ch);

  for (let i = 0; i < CURATED.length; i += CHANNEL_BATCH) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) break;
    const batch = CURATED.slice(i, i + CHANNEL_BATCH);
    const done = await Promise.race([
      Promise.all(batch.map((ch) => refreshOne(ch, index, probeLimit))),
      new Promise<null>((res) => setTimeout(() => res(null), remaining)),
    ]);
    if (done === null) break; // budget exhausted mid-batch — baseline kept
    done.forEach((ch, j) => {
      if (ch) channels[i + j] = ch;
    });
  }

  const alive = channels.filter((c) => c.url || c.type === "youtube").length;
  const result: RefreshResult = {
    refreshedAt: Date.now(),
    total: channels.length,
    alive,
    channels,
  };

  // Anti-downgrade guard: a flaky probe run must not nuke the channel list.
  // If this refresh found fewer working channels than the previous cache,
  // restore previously-working URLs so one bad run can't hide channels.
  // (Fixed 2026-10-08: the old merge could duplicate the restored URL into
  // `fallbacks` — the merged list is now deduped with `url` excluded.)
  if (prev && result.alive < prev.alive) {
    const prevByName = new Map(prev.channels.map((c) => [norm(c.name), c]));
    result.channels = result.channels.map((ch) => {
      const p = prevByName.get(norm(ch.name));
      if (p && p.url && !ch.url) {
        const merged = [p.url, ...p.fallbacks, ...ch.fallbacks].filter(
          (u, idx, arr) => u && arr.indexOf(u) === idx
        );
        return { ...ch, url: p.url, fallbacks: merged.slice(1, 5) };
      }
      return ch;
    });
    result.alive = result.channels.filter(
      (c) => c.url || c.type === "youtube",
    ).length;
  }

  cacheSet(CACHE_KEY, result, TTL_MS, STALE_MS);
  return result;
}

/** A channel is playable if it has a stream URL or is YouTube-type (resolved at play time). */
export function isPlayable(ch: Channel): boolean {
  return ch.type === "youtube" || !!ch.url;
}

/** Remove dead channels (no URL) so the app only shows working ones. */
export function hideDead(channels: Channel[]): Channel[] {
  return channels.filter(isPlayable);
}

/** Curated channels with no playable URL — reported honestly in the
 *  `pending` array of /v1/livetv/channels instead of silently flickering
 *  in and out of the list between instances. */
export function pendingChannels(channels: Channel[]): Channel[] {
  return channels.filter((c) => !isPlayable(c));
}

/**
 * Get channels — serves cache (fast). On cold start seeds from CURATED.
 * There is deliberately NO background refresh here: fire-and-forget promises
 * cannot complete on serverless. /v1/cron/livetv-refresh is the only trigger.
 * The full channel list (including unavailable ones) is always returned —
 * the route splits playable vs `pending` honestly.
 */
export async function getChannels(): Promise<RefreshResult> {
  const cached = cacheGet<RefreshResult>(CACHE_KEY);
  if (cached) return cached.value;
  // Cold start: serve curated list immediately (fast), seed the cache.
  const result: RefreshResult = {
    refreshedAt: Date.now(),
    total: CURATED.length,
    alive: hideDead(CURATED).length,
    channels: CURATED,
  };
  cacheSet(CACHE_KEY, result, TTL_MS, STALE_MS);
  return result;
}

/** Group channels by category for the API response. */
export function groupByCategory(channels: Channel[]) {
  const groups: Record<string, { label: string; channels: Channel[] }> = {};
  for (const ch of channels) {
    if (!groups[ch.category]) {
      groups[ch.category] = { label: CATEGORY_LABELS[ch.category], channels: [] };
    }
    groups[ch.category].channels.push(ch);
  }
  return groups;
}
