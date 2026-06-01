"""
Twitter API 交互模块
通过 twitterapi.io REST API 获取 Twitter/X 推文数据
"""

import asyncio
import hashlib
import re
import time
from pathlib import Path
from typing import Optional

import httpx
from astrbot.api import logger

# 有效的图片质量选项
IMAGE_QUALITY_OPTIONS = ("large", "orig")

# 媒体缓存目录名
DEFAULT_MEDIA_CACHE_DIR = "twitter_media_cache"

# 媒体缓存过期时间（秒）：7 天
MEDIA_CACHE_MAX_AGE_SECONDS = 7 * 24 * 3600

# 媒体缓存最大总大小（字节）：500 MB
MEDIA_CACHE_MAX_SIZE_BYTES = 500 * 1024 * 1024


class TwitterAPI:
    """Twitter API 交互类，通过 twitterapi.io 获取推文"""

    BASE_URL = "https://api.twitterapi.io"

    def __init__(
        self,
        api_key: str = "",
        proxy: Optional[str] = None,
        image_quality: str = "orig",
        media_cache_dir: str = "",
    ):
        self.api_key = api_key
        self.proxy = proxy
        self.image_quality = (
            image_quality if image_quality in IMAGE_QUALITY_OPTIONS else "orig"
        )
        self._client: Optional[httpx.AsyncClient] = None

        # 媒体文件本地缓存
        if media_cache_dir:
            self._media_cache_dir = Path(media_cache_dir)
        else:
            self._media_cache_dir = (
                Path(__file__).resolve().parent / DEFAULT_MEDIA_CACHE_DIR
            )
        self._media_cache_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"媒体缓存目录: {self._media_cache_dir}")

    async def _get_client(self) -> httpx.AsyncClient:
        """获取或创建异步 HTTP 客户端"""
        if self._client is None or self._client.is_closed:
            proxy = self.proxy if self.proxy else None
            self._client = httpx.AsyncClient(
                proxy=proxy,
                http2=True,
                timeout=30.0,
                follow_redirects=True,
                headers={
                    "x-api-key": self.api_key,
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                },
            )
        return self._client

    async def close(self):
        """关闭 HTTP 客户端"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def _get(self, path: str, params: dict = None) -> dict:
        """发送 GET 请求到 twitterapi.io"""
        client = await self._get_client()
        url = f"{self.BASE_URL}{path}"
        try:
            resp = await client.get(url, params=params or {}, timeout=30.0)
            if resp.status_code != 200:
                logger.warning(
                    f"twitterapi.io 请求失败: {url}, 状态码: {resp.status_code}, "
                    f"响应: {resp.text[:200]}"
                )
                return {}
            return resp.json()
        except Exception as e:
            logger.error(f"twitterapi.io 请求异常: {url}, {e}")
            return {}

    @staticmethod
    def _format_duration(duration_millis: int) -> str:
        """将毫秒时长格式化为 mm:ss 或 hh:mm:ss"""
        if not duration_millis:
            return ""
        seconds = duration_millis // 1000
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        if hours > 0:
            return f"{hours}:{minutes:02d}:{secs:02d}"
        return f"{minutes}:{secs:02d}"

    @staticmethod
    def _parse_content_length(value: str) -> Optional[int]:
        """解析正数形式的 Content-Length 响应头。"""
        try:
            length = int(str(value or "").strip())
        except (TypeError, ValueError):
            return None
        return length if length > 0 else None

    @staticmethod
    def _parse_content_range_total(value: str) -> Optional[int]:
        """从 Content-Range 响应头解析文件总大小。"""
        match = re.search(r"/(\d+)\s*$", str(value or ""))
        if not match:
            return None
        try:
            total = int(match.group(1))
        except ValueError:
            return None
        return total if total > 0 else None

    async def get_remote_file_size(self, url: str) -> Optional[int]:
        """尽量在不下载正文的情况下探测远程文件大小。"""
        url = str(url or "").strip()
        if not url:
            return None

        client = await self._get_client()
        try:
            resp = await client.head(url, timeout=15.0)
            if resp.status_code < 400:
                size = self._parse_content_length(
                    resp.headers.get("content-length", "")
                )
                if size is not None:
                    return size
        except Exception as e:
            logger.debug(f"HEAD 探测远程文件大小失败: {url}, {e}")

        try:
            async with client.stream(
                "GET",
                url,
                headers={"Range": "bytes=0-0"},
                timeout=15.0,
            ) as resp:
                if resp.status_code >= 400:
                    return None
                size = self._parse_content_range_total(
                    resp.headers.get("content-range", "")
                )
                if size is not None:
                    return size
                if resp.status_code == 206:
                    return None
                return self._parse_content_length(
                    resp.headers.get("content-length", "")
                )
        except Exception as e:
            logger.debug(f"Range 探测远程文件大小失败: {url}, {e}")
            return None

    async def download_media(self, url: str, suffix: str = ".jpg") -> str | None:
        """通过代理下载媒体文件到本地缓存，返回本地路径。

        优先从本地媒体缓存读取；缓存未命中时通过 HTTP 下载并存入缓存。
        用于解决 AstrBot 直连 Twitter CDN 超时的问题。

        返回:
            本地文件路径（缓存或下载），失败返回 None
        """
        url = str(url or "").strip()
        if not url:
            return None

        cache_key = self._media_cache_key(url)
        cached_path = self._get_from_media_cache(cache_key)
        if cached_path:
            logger.debug(f"媒体缓存命中: {url[:60]}...")
            return cached_path

        # 缓存未命中，下载
        proxy = self.proxy if self.proxy else None

        client = httpx.AsyncClient(
            proxy=proxy,
            timeout=60.0,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            },
        )

        try:
            last_error = None
            for attempt in range(2):
                try:
                    resp = await client.get(url, timeout=60.0)
                    if resp.status_code != 200:
                        logger.warning(
                            f"下载媒体失败 (尝试 {attempt + 1}/2): {url[:60]}..., "
                            f"状态码: {resp.status_code}"
                        )
                        if attempt < 1:
                            await asyncio.sleep(2)
                            continue
                        return None

                    content_type = resp.headers.get("content-type", "")
                    # 根据 Content-Type 确定后缀
                    ext = suffix
                    if "png" in content_type:
                        ext = ".png"
                    elif "gif" in content_type:
                        ext = ".gif"
                    elif "webp" in content_type:
                        ext = ".webp"
                    elif "mp4" in content_type:
                        ext = ".mp4"

                    # 保存到媒体缓存
                    path = self._save_to_media_cache(cache_key, ext, resp.content)
                    logger.debug(f"媒体下载并缓存: {url[:60]}... -> {path}")
                    return path

                except Exception as e:
                    last_error = e
                    if attempt < 1:
                        logger.debug(
                            f"媒体下载重试 (尝试 {attempt + 1}/2): {url[:60]}..., {e}"
                        )
                        await asyncio.sleep(2)
                        continue

            logger.warning(f"下载媒体异常: {url[:60]}..., {last_error}")
            return None
        finally:
            await client.aclose()

    # ========== 媒体文件本地缓存 ==========

    @staticmethod
    def _media_cache_key(url: str) -> str:
        """基于 URL 生成媒体缓存键（SHA256 哈希）"""
        return hashlib.sha256(url.encode()).hexdigest()

    def _get_from_media_cache(self, cache_key: str) -> str | None:
        """从本地缓存查找媒体文件，返回路径或 None

        自动检测缓存目录中匹配 hash 的文件（可能带不同后缀）。
        """
        for f in self._media_cache_dir.glob(f"{cache_key}.*"):
            if f.is_file() and f.stat().st_size > 0:
                return str(f)
        return None

    def _save_to_media_cache(
        self, cache_key: str, ext: str, content: bytes
    ) -> str:
        """将媒体内容写入本地缓存，返回文件路径"""
        path = self._media_cache_dir / f"{cache_key}{ext}"
        with open(path, "wb") as f:
            f.write(content)
        return str(path)

    def clear_media_cache(self) -> int:
        """清空所有缓存的媒体文件，返回删除的文件数"""
        count = 0
        try:
            for f in self._media_cache_dir.iterdir():
                if f.is_file():
                    f.unlink()
                    count += 1
            logger.info(f"已清空媒体缓存: {count} 个文件")
        except Exception as e:
            logger.warning(f"清空媒体缓存异常: {e}")
        return count

    def cleanup_old_media_cache(
        self,
        max_age_seconds: int = MEDIA_CACHE_MAX_AGE_SECONDS,
        max_total_bytes: int = MEDIA_CACHE_MAX_SIZE_BYTES,
    ) -> int:
        """清理过期和超量的媒体缓存文件。

        先删除超过 max_age_seconds 的过期文件；
        再按修改时间从旧到新删除，直到总大小低于 max_total_bytes。

        返回删除的文件数。
        """
        now = time.time()
        files = [
            (f, f.stat().st_mtime, f.stat().st_size)
            for f in self._media_cache_dir.iterdir()
            if f.is_file()
        ]

        if not files:
            return 0

        removed = 0
        remaining: list[tuple[Path, float, int]] = []

        # 按修改时间排序（旧的在前）
        files.sort(key=lambda x: x[1])

        for f, mtime, size in files:
            if now - mtime > max_age_seconds:
                try:
                    f.unlink()
                    removed += 1
                except OSError:
                    pass
            else:
                remaining.append((f, mtime, size))

        # 检查总大小是否超限
        total_size = sum(s for _, _, s in remaining)
        if total_size > max_total_bytes:
            # 从最旧的文件开始删除
            for f, _mtime_unused, size in remaining:
                if total_size <= max_total_bytes:
                    break
                try:
                    f.unlink()
                    total_size -= size
                    removed += 1
                except OSError:
                    pass

        if removed > 0:
            logger.debug(f"媒体缓存清理完成: 删除 {removed} 个文件")

        return removed

    async def validate_api_key(self) -> bool:
        """验证 API key 是否有效（通过获取账户余额）"""
        if not self.api_key:
            return False
        try:
            data = await self._get("/oapi/my/info")
            # 成功返回: {"recharge_credits": ..., "total_bonus_credits": ...}
            if data and ("recharge_credits" in data or "total_bonus_credits" in data):
                logger.info(f"twitterapi.io API key 验证成功")
                return True
            return False
        except Exception as e:
            logger.error(f"twitterapi.io API key 验证失败: {e}")
            return False

    # ========== 推文数据映射 ==========

    def _parse_media(self, media_list: list) -> tuple[list[str], list[str], list[dict]]:
        """从 twitterapi.io 的 extendedEntities.media 解析图片、视频和视频封面。

        twitterapi.io 返回的 media 对象可能不含 type 字段，需要通过其他字段区分：
        - 图片: media_url_https 存在，无 videoInfo
        - 视频: videoInfo.variants[] 存在，或 ext_master_playlist_only 有内容

        返回:
            (images, videos, video_previews)
        """
        images: list[str] = []
        videos: list[str] = []
        video_previews: list[dict] = []

        for media in media_list:
            media_url = media.get("media_url_https", "")
            video_info = media.get("videoInfo") or {}
            ext_playlists = media.get("ext_playlists") or []
            ext_master_playlist = media.get("ext_master_playlist_only") or []

            # 判断是否为视频：有 videoInfo.variants 或 ext_master_playlist_only 非空
            is_video = bool(
                video_info.get("variants")
                or ext_master_playlist
                or ext_playlists
            )

            if is_video:
                # 视频封面（poster）
                video_previews.append({
                    "poster": media_url,
                    "duration": self._format_duration(
                        video_info.get("duration_millis", 0)
                    ),
                })

                # 视频变体：选择最高码率 mp4
                variants = video_info.get("variants", [])
                best_url = ""
                best_bitrate = -1
                for variant in variants:
                    ct = variant.get("content_type", "")
                    if "mp4" in ct:
                        bitrate = variant.get("bitrate", 0) or 0
                        if bitrate > best_bitrate:
                            best_bitrate = bitrate
                            best_url = variant.get("url", "")
                if best_url:
                    videos.append(best_url)
                elif ext_master_playlist:
                    # 回退：使用 m3u8 播放清单 URL（可能无法直接发送）
                    logger.debug(f"视频仅有 m3u8 流，无 mp4 直链")
            else:
                # 图片
                if self.image_quality == "orig" and media_url:
                    images.append(f"{media_url}?format=jpg&name=orig")
                elif media_url:
                    images.append(media_url)

        return images, videos, video_previews

    def _parse_quote(self, quoted_tweet: dict) -> dict | None:
        """解析引用推文为插件内部格式。

        若引用的推文是转推，向下处理一层，使用原始推文的内容，
        但不再处理该原始推文所引用的推文。
        """
        if not quoted_tweet:
            return None

        # 若引用推文是转推，向下处理一层，使用原始推文内容
        inner_retweet = quoted_tweet.get("retweeted_tweet")
        if inner_retweet:
            quoted_tweet = inner_retweet

        author = quoted_tweet.get("author") or {}
        quote_username = author.get("userName", "")
        quote_name = author.get("name", "")

        # 引用推文的 media
        extended_entities = quoted_tweet.get("extendedEntities") or {}
        media_list = extended_entities.get("media", [])
        q_images, q_videos, q_video_previews = self._parse_media(media_list)

        return {
            "author": quote_name,
            "username": quote_username,
            "avatar": author.get("profilePicture", ""),
            "verified": author.get("isVerified") or author.get("isBlueVerified") or False,
            "date": quoted_tweet.get("createdAt", ""),
            "tweet_id": quoted_tweet.get("id", ""),
            "text": quoted_tweet.get("text", ""),
            "images": q_images,
            "videos": q_videos,
            "video_previews": q_video_previews,
        }

    def _parse_tweet(
        self, tweet: dict, fallback_username: str = ""
    ) -> dict:
        """将 twitterapi.io 推文数据映射为插件内部统一格式。

        推文有 3 种类型，均只向下处理一层：
          1. 直接推文：retweeted_tweet=None, quoted_tweet=None → 返回推文本身
          2. 转推：retweeted_tweet!=None → 使用被转推文内容，不解析其 quoted_tweet
          3. 引用推文：quoted_tweet!=None → 解析引用推文一层，不递归

        参数:
            tweet: twitterapi.io 返回的推文对象
            fallback_username: 无 author 信息时的回退用户名

        返回:
            插件内部格式的推文字典
        """
        author = tweet.get("author") or {}

        # 处理转推（retweeted_tweet）
        retweet = None
        is_retweet = False
        retweeted_tweet = tweet.get("retweeted_tweet")
        if retweeted_tweet:
            is_retweet = True
            retweet = {
                "retweeter_username": author.get("userName", fallback_username),
                "retweeter_screen_name": author.get("name", ""),
            }
            # 使用原始推文的内容
            tweet = retweeted_tweet
            author = tweet.get("author") or {}

        tweet_id = tweet.get("id", "")
        username = author.get("userName", fallback_username)
        screen_name = author.get("name", username)
        avatar = author.get("profilePicture", "")
        verified = (
            author.get("isVerified")
            or author.get("isBlueVerified")
            or False
        )
        created_at = tweet.get("createdAt", "")

        # 统计信息（twitterapi.io 使用 camelCase 字段）
        stats = {
            "comments": str(tweet.get("replyCount", 0) or 0),
            "retweets": str(tweet.get("retweetCount", 0) or 0),
            "likes": str(tweet.get("likeCount", 0) or 0),
            "views": str(tweet.get("viewCount", 0) or 0),
        }

        text = tweet.get("text", "")
        # possibly_sensitive 可能在推文级别或作者级别
        is_r18 = (
            tweet.get("possibly_sensitive")
            or tweet.get("possiblySensitive")
            or author.get("possiblySensitive")
            or False
        )

        # 媒体解析
        extended_entities = tweet.get("extendedEntities") or {}
        media_list = extended_entities.get("media", [])
        images, videos, video_previews = self._parse_media(media_list)

        # 引用推文：转推时不向下处理被转推文所引用的推文（仅一层）
        if is_retweet:
            quote = None
        else:
            quote = self._parse_quote(tweet.get("quoted_tweet") or None)

        return {
            "tweet_id": tweet_id,
            "username": username,
            "screen_name": screen_name,
            "avatar": avatar,
            "verified": verified,
            "date": created_at,
            "stats": stats,
            "text": text,
            "images": images,
            "videos": videos,
            "video_previews": video_previews,
            "quote": quote,
            "retweet": retweet,
            "is_r18": is_r18,
        }

    # ========== 用户相关 API ==========

    async def get_user_info(self, username: str) -> dict:
        """获取 Twitter 用户信息（通过 twitterapi.io）

        返回:
            {"status": bool, "screen_name": str, "bio": str, "user_name": str}
        """
        if not self.api_key:
            return {
                "status": False,
                "screen_name": "",
                "bio": "",
                "user_name": username,
            }

        data = await self._get(
            "/twitter/user/info", {"userName": username}
        )
        if not data:
            return {
                "status": False,
                "screen_name": "",
                "bio": "",
                "user_name": username,
            }

        user_data = data.get("data") or {}
        return {
            "status": True,
            "screen_name": user_data.get("name", username),
            "bio": user_data.get("description", ""),
            "user_name": user_data.get("userName", username),
        }

    # ========== 时间线相关 API ==========

    async def _fetch_timeline_tweets(
        self, username: str, since_id: str = ""
    ) -> list[dict]:
        """从 twitterapi.io 获取用户推文列表（内部方法）。

        返回推文对象列表（格式为 twitterapi.io 原始格式），按时间倒序。
        """
        if not self.api_key:
            return []

        data = await self._get(
            "/twitter/user/last_tweets", {"userName": username}
        )
        if not data:
            return []

        # 响应可能在 data.tweets 或顶层 tweets 中
        if "data" in data and isinstance(data["data"], dict):
            tweets = data["data"].get("tweets", [])
        elif "tweets" in data:
            tweets = data["tweets"]
        elif "data" in data and isinstance(data["data"], list):
            tweets = data["data"]
        else:
            tweets = []

        if not isinstance(tweets, list):
            tweets = []

        return tweets

    async def get_user_new_tweets_parsed(
        self,
        username: str,
        since_id: str = "",
        include_retweets: bool = True,
        limit: int = 0,
    ) -> list[dict]:
        """获取用户新推文（已解析为内部格式），单次 API 调用。

        将 /twitter/user/last_tweets 返回的推文直接解析为插件内部格式，
        无需再单独调用 get_tweet()，大幅减少 API 调用次数。

        参数:
            username: 推主用户名
            since_id: 已知最新推文 ID，仅返回比此 ID 更新的推文；
                      为空时返回最新推文（不限制 since_id）
            include_retweets: 是否包含转推
            limit: 限制返回条数（0=不限制）

        返回:
            已解析的推文字典列表（时间正序，最旧的在前）
        """
        tweets = await self._fetch_timeline_tweets(username)

        if not tweets:
            return []

        parsed: list[dict] = []

        for tweet in tweets:
            tid = str(tweet.get("id", ""))

            # 跳过置顶推文
            if tweet.get("isPinned"):
                continue

            # since_id 过滤
            if since_id:
                try:
                    if int(tid) <= int(since_id):
                        continue
                except ValueError:
                    continue

            # 解析推文
            parsed_tweet = self._parse_tweet(tweet, fallback_username=username)

            # 转推过滤
            if not include_retweets and parsed_tweet.get("retweet"):
                continue

            parsed.append(parsed_tweet)

            if limit > 0 and len(parsed) >= limit:
                break

        # 时间正序（twitterapi.io 返回最新在前，需要反转）
        parsed.reverse()
        return parsed

    async def get_user_tweets_since_time(
        self,
        username: str,
        since_time: float,
        include_retweets: bool = True,
    ) -> list[dict]:
        """使用 advanced_search 获取 since_time 之后的新推文（已解析为内部格式）。

        通过 /twitter/tweet/advanced_search 接口，使用 since_time 过滤推文，
        避免 /twitter/user/last_tweets 返回固定数量导致的推文遗漏问题。
        支持游标分页，确保两次轮询之间的所有推文都被获取。

        参数:
            username: 推主用户名
            since_time: Unix 时间戳（秒），仅返回此时间之后发布的推文
            include_retweets: 是否包含转推（False 时在 query 和客户端双重过滤）

        返回:
            已解析的推文字典列表（时间正序，最旧的在前）
        """
        if not self.api_key:
            return []

        # 构建查询：from:用户名 since_time:Unix时间戳
        # from: 操作符默认排除 native retweets，需要显式 include:nativeretweets 才能获取
        query = f"from:{username} since_time:{int(since_time)}"
        if include_retweets:
            query += " include:nativeretweets"
        else:
            query += " -filter:retweets"

        all_tweets: list[dict] = []
        cursor: str | None = None
        max_pages = 10  # 安全上限，防止无限循环

        for _ in range(max_pages):
            params: dict = {
                "query": query,
                "queryType": "Latest",
            }
            if cursor:
                params["cursor"] = cursor

            data = await self._get("/twitter/tweet/advanced_search", params)
            if not data:
                break

            # 响应可能在 data.tweets 或顶层 tweets 中
            tweets: list[dict] = []
            if "data" in data and isinstance(data["data"], dict):
                tweets = data["data"].get("tweets", [])
            elif "tweets" in data:
                tweets = data["tweets"]
            elif "data" in data and isinstance(data["data"], list):
                tweets = data["data"]

            if not isinstance(tweets, list) or not tweets:
                break

            for tweet in tweets:
                # 跳过置顶推文
                if tweet.get("isPinned"):
                    continue

                # 解析推文
                parsed_tweet = self._parse_tweet(tweet, fallback_username=username)

                # 转推过滤（客户端二次确认，advanced_search 的 -filter:retweets 不完整）
                if not include_retweets and parsed_tweet.get("retweet"):
                    continue

                all_tweets.append(parsed_tweet)

            # 分页处理
            has_next_page = data.get("has_next_page", False)
            if "data" in data and isinstance(data["data"], dict):
                has_next_page = data["data"].get("has_next_page", has_next_page)
                next_cursor = data["data"].get("next_cursor", "")
            else:
                next_cursor = data.get("next_cursor", "")

            if not has_next_page or not next_cursor:
                break
            cursor = next_cursor

        # 时间正序（advanced_search 返回最新在前，需要反转）
        all_tweets.reverse()
        return all_tweets

    async def get_user_newtimeline(
        self, username: str, since_id: str = ""
    ) -> list[str]:
        """获取用户比 since_id 更新的推文 ID 列表

        参数:
            username: 推主用户名
            since_id: 已知最新推文 ID，仅返回比此 ID 更新的推文；
                      为空时仅返回最新一条推文 ID（用于首次订阅定位）

        返回:
            新推文 ID 列表（时间正序），无新推文时返回空列表
        """
        tweets = await self._fetch_timeline_tweets(username)

        if not tweets:
            return []

        if not since_id:
            # 首次订阅：只返回最新一条
            return [str(tweets[0].get("id", ""))] if tweets else []

        # 过滤出比 since_id 更新的推文
        new_items = []
        for tweet in tweets:
            tid = str(tweet.get("id", ""))
            if not tid:
                continue
            try:
                if int(tid) <= int(since_id):
                    continue
            except ValueError:
                continue
            new_items.append(tid)

        # 时间正序（twitterapi.io 返回最新在前，需要反转）
        new_items.reverse()
        return new_items

    async def get_user_timeline_items(
        self, username: str, since_id: str = "", limit: int = 0
    ) -> list[dict]:
        """获取用户时间线条目，包含转帖元数据。

        参数:
            username: 推主用户名
            since_id: 已知最新推文 ID
            limit: 限制返回条数（0=不限制）

        返回:
            时间线条目列表（无 since_id 时最新在前；有 since_id 时时间正序）
        """
        tweets = await self._fetch_timeline_tweets(username)

        if not tweets:
            return []

        parsed_items: list[dict] = []

        for tweet in tweets:
            tid = str(tweet.get("id", ""))

            # 跳过置顶推文（twitterapi.io 返回 isPinned 字段）
            if tweet.get("isPinned"):
                continue

            author = tweet.get("author") or {}
            tweet_username = author.get("userName", username)

            # since_id 过滤
            if since_id:
                try:
                    if int(tid) <= int(since_id):
                        continue
                except ValueError:
                    continue

            # 检测是否为转推
            retweeted_tweet = tweet.get("retweeted_tweet")
            is_retweet = retweeted_tweet is not None

            item = {
                "tweet_id": tid,
                "username": tweet_username,
                "is_retweet": is_retweet,
                "retweeter_username": author.get("userName", username),
                "retweeter_screen_name": author.get("name", ""),
            }

            parsed_items.append(item)

            if limit > 0 and len(parsed_items) >= limit:
                break

        if since_id:
            # twitterapi.io 返回的是最新在前，需要反转
            parsed_items.reverse()
        return parsed_items

    # ========== 推文详情 API ==========

    async def get_tweet(self, username: str, tweet_id: str) -> dict:
        """获取推文详细信息（通过 twitterapi.io）

        返回:
            插件内部格式的推文字典
        """
        result = {
            "tweet_id": tweet_id,
            "username": username,
            "screen_name": username,
            "avatar": "",
            "verified": False,
            "date": "",
            "stats": {
                "comments": "0",
                "retweets": "0",
                "likes": "0",
                "views": "0",
            },
            "text": "",
            "images": [],
            "videos": [],
            "video_previews": [],
            "quote": None,
            "retweet": None,
            "is_r18": False,
        }

        if not self.api_key or not tweet_id:
            return result

        data = await self._get(
            "/twitter/tweets", {"tweet_ids": tweet_id}
        )
        if not data:
            return result

        tweets = data.get("tweets", [])
        if not tweets:
            return result

        tweet = tweets[0]
        return self._parse_tweet(tweet, fallback_username=username)
