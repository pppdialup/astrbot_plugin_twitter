"""
Twitter API 交互模块
通过 Nitter 镜像站获取 Twitter/X 推文数据
"""

import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from astrbot.api import logger

# 内置 Nitter 镜像站列表
WEBSITE_LIST = [
    "https://nitter.net",
]


class TwitterAPI:
    """Twitter API 交互类，通过 Nitter 镜像站获取推文"""

    def __init__(self, proxy: Optional[str] = None, nitter_url: str = ""):
        self.proxy = proxy
        self.nitter_url = nitter_url
        self._client: Optional[httpx.AsyncClient] = None

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
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                },
            )
        return self._client

    async def close(self):
        """关闭 HTTP 客户端"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def check_website_available(self, website_list: list[str]) -> Optional[str]:
        """检测可用的镜像站，返回第一个可用的 URL"""
        client = await self._get_client()
        for url in website_list:
            try:
                test_url = f"{url}/elonmusk"
                resp = await client.get(test_url, timeout=15.0)
                if resp.status_code == 200:
                    logger.info(f"Nitter 镜像站可用: {url}")
                    self.nitter_url = url
                    return url
                logger.debug(f"Nitter 镜像站不可用: {url}, 状态码: {resp.status_code}")
            except Exception as e:
                logger.debug(f"Nitter 镜像站检测异常: {url}, 错误: {e}")
                continue
        logger.warning("所有 Nitter 镜像站均不可用")
        return None

    async def get_user_info(self, username: str) -> dict:
        """获取 Twitter 用户信息

        Returns:
            {"status": bool, "screen_name": str, "bio": str, "user_name": str}
        """
        if not self.nitter_url:
            return {"status": False, "screen_name": "", "bio": "", "user_name": username}

        client = await self._get_client()
        url = f"{self.nitter_url}/{username}"
        try:
            resp = await client.get(url, timeout=15.0)
            if resp.status_code != 200:
                return {"status": False, "screen_name": "", "bio": "", "user_name": username}

            soup = BeautifulSoup(resp.text, "html.parser")

            name_elem = soup.select_one("a.profile-card-fullname")
            screen_name = name_elem.get_text(strip=True) if name_elem else username

            bio_elem = soup.select_one("div.profile-bio")
            bio = bio_elem.get_text(strip=True) if bio_elem else ""

            return {
                "status": True,
                "screen_name": screen_name,
                "bio": bio,
                "user_name": username,
            }
        except Exception as e:
            logger.error(f"获取用户信息失败 {username}: {e}")
            return {"status": False, "screen_name": "", "bio": "", "user_name": username}

    async def get_user_newtimeline(self, username: str, since_id: str = "") -> list[str]:
        """获取用户比 since_id 更新的推文 ID 列表

        Nitter 时间线按最新优先排列，返回结果按时间正序（最旧在前）。

        Args:
            username: 推主用户名
            since_id: 已知最新推文 ID，仅返回比此 ID 更新的推文；
                      为空时仅返回最新一条推文 ID（用于首次订阅定位）

        Returns:
            新推文 ID 列表（时间正序），无新推文时返回空列表
        """
        if not self.nitter_url:
            return []

        client = await self._get_client()
        url = f"{self.nitter_url}/{username}"
        try:
            resp = await client.get(url, timeout=15.0)
            if resp.status_code != 200:
                return []

            soup = BeautifulSoup(resp.text, "html.parser")

            # 获取所有 timeline-item，跳过置顶推文
            timeline_items = soup.select("div.timeline-item")
            new_ids: list[str] = []

            for item in timeline_items:
                # 检测置顶推文标记并跳过
                if item.select_one(".pinned, .icon-pin"):
                    continue

                link = item.select_one("a.tweet-link")
                if not link:
                    continue

                href = link.get("href", "")
                match = re.search(r"/status/(\d+)", href)
                if match:
                    tweet_id = match.group(1)
                    if not since_id:
                        # 无 since_id 时仅取最新一条（用于首次订阅定位）
                        return [tweet_id]

                    try:
                        if int(tweet_id) > int(since_id):
                            new_ids.append(tweet_id)
                        else:
                            # 时间线按最新优先，遇到 <= since_id 的即可停止
                            break
                    except ValueError:
                        # ID 解析异常，跳过
                        continue

            # 反转为时间正序（最旧在前）
            new_ids.reverse()
            return new_ids
        except Exception as e:
            logger.error(f"获取用户时间线失败 {username}: {e}")
            return []

    async def get_tweet(self, username: str, tweet_id: str) -> dict:
        """获取推文详细信息

        Returns:
            推文信息字典，包含 text, images, videos, quote, is_r18, screen_name 等
        """
        result = {
            "tweet_id": tweet_id,
            "username": username,
            "screen_name": username,
            "text": "",
            "images": [],
            "videos": [],
            "quote": None,
            "is_r18": False,
        }

        if not self.nitter_url:
            return result

        client = await self._get_client()
        nitter_url = f"{self.nitter_url}/{username}/status/{tweet_id}"

        try:
            resp = await client.get(nitter_url, timeout=20.0)
            if resp.status_code != 200:
                logger.warning(f"获取推文失败: {nitter_url}, 状态码: {resp.status_code}")
                return result

            soup = BeautifulSoup(resp.text, "html.parser")

            # 限定在主贴容器内，避免匹配评论/回复区内容
            # Nitter 推文详情页：主贴在 div.main-tweet 内，评论在其后
            main_tweet = soup.select_one("div.main-tweet")
            if not main_tweet:
                # 某些实例可能没有 main-tweet 包裹，回退到整个页面
                logger.warning(
                    f"未找到 div.main-tweet 容器，"
                    f"回退到整个页面选择（可能匹配评论区内容）"
                )
                main_tweet = soup

            # 获取显示名称
            fullname_elem = main_tweet.select_one("a.fullname")
            if fullname_elem:
                result["screen_name"] = fullname_elem.get_text(strip=True)

            # 获取推文正文
            content_elem = main_tweet.select_one("div.tweet-content.media-body")
            if content_elem:
                result["text"] = content_elem.get_text(strip=True)
            else:
                # 调试：尝试更宽松的选择器，排查 HTML 结构差异
                fallback_elem = main_tweet.select_one("div.tweet-content")
                if fallback_elem:
                    result["text"] = fallback_elem.get_text(strip=True)
                    logger.info(
                        f"推文正文通过 div.tweet-content 提取（无 media-body 类）"
                    )
                else:
                    logger.warning(
                        f"未找到推文正文，"
                        f"main-tweet 内 HTML 片段: "
                        f"{str(main_tweet)[:500]}"
                    )

            # 获取图片（仅主贴，排除视频/GIF缩略图）
            # Nitter 中真实图片附件使用 <a class="still-image"> 包裹 <img>，
            # 而视频缩略图 <img loading="lazy"> 不在 still-image 内，以此区分
            img_elems = main_tweet.select("a.still-image img")
            for img in img_elems:
                src = img.get("src", "")
                if src:
                    if not src.startswith("http"):
                        src = f"{self.nitter_url}{src}"
                    result["images"].append(src)

            # 获取视频/GIF（仅主贴）
            # Nitter 视频有三种HTML形态：
            #   1) mp4播放启用: <video><source src=""></video>
            #   2) m3u8/vmap格式: <video data-url=""> (无src/source)
            #   3) 播放被禁用: 仅有 <img> 缩略图 + <div class="video-overlay">
            video_elems = main_tweet.select("div.attachment video")
            seen_urls: set[str] = set()
            for video in video_elems:
                # 方式1: <source src="">（mp4格式）
                for source in video.find_all("source"):
                    src = source.get("src", "")
                    if src:
                        if not src.startswith("http"):
                            src = f"{self.nitter_url}{src}"
                        if src not in seen_urls:
                            seen_urls.add(src)
                            result["videos"].append(src)
                # 方式2: <video src="">（GIF或直接src）
                src = video.get("src", "")
                if src:
                    if not src.startswith("http"):
                        src = f"{self.nitter_url}{src}"
                    if src not in seen_urls:
                        seen_urls.add(src)
                        result["videos"].append(src)
                # 方式3: <video data-url="">（m3u8/vmap格式）
                data_url = video.get("data-url", "")
                if data_url:
                    if not data_url.startswith("http"):
                        data_url = f"{self.nitter_url}{data_url}"
                    if data_url not in seen_urls:
                        seen_urls.add(data_url)
                        result["videos"].append(data_url)

            # 检测视频附件但未提取到视频URL的情况
            video_overlays = main_tweet.select("div.video-overlay")
            if video_overlays and not result["videos"]:
                logger.warning(
                    f"检测到视频附件但未提取到视频URL，"
                    f"可能 Nitter 实例({self.nitter_url})禁用了视频播放。"
                    f"请在 Nitter 实例偏好设置中启用 mp4 playback"
                )

            # 获取引用推文（仅主贴）
            quote_elem = main_tweet.select_one("div.quote")
            if quote_elem:
                quote_text_elem = quote_elem.select_one("div.tweet-content")
                quote_author = quote_elem.select_one("a.fullname")
                result["quote"] = {
                    "author": quote_author.get_text(strip=True) if quote_author else "",
                    "text": quote_text_elem.get_text(strip=True) if quote_text_elem else "",
                }

            # 检测 R18 标记（仅主贴）
            r18_elem = main_tweet.select_one(".nsfw")
            result["is_r18"] = r18_elem is not None

        except Exception as e:
            logger.error(f"获取推文详情失败 {username}/{tweet_id}: {e}")

        return result

def get_next_website(website_list: list[str], current: str) -> Optional[str]:
    """获取列表中当前镜像站的下一个（循环）"""
    if not website_list:
        return None
    try:
        idx = website_list.index(current)
        return website_list[(idx + 1) % len(website_list)]
    except (ValueError, IndexError):
        return website_list[0]
