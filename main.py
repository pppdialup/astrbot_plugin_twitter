"""
AstrBot Twitter 推文转发插件
基于 twitterapi.io REST API，支持订阅推主、定时推送、链接识别、合并转发消息、推文翻译

指令列表:
  /推特关注 <推主id> [r18] [媒体]          - 订阅推主
  /推特批量关注 <推主id1> <推主id2> ... [r18] [媒体]  - 批量订阅推主
  /推特取关 <推主id>                        - 取关推主
  /推特批量取关 <推主id1> <推主id2> ...     - 批量取关推主
  /推特清空订阅                             - 清空所有订阅（仅管理员）
  /推特清空缓存                             - 清空推文缓存（仅管理员）
  /推特列表                                 - 查看当前订阅列表
  /推特推送 开启/关闭                       - 开启/关闭推送
  /推特测试 <推主id>                        - 立即获取并推送指定推主最新一条推文

配置项:
  【twitterapi.io 设置】
    API Key (twitter_api_key)                     - twitterapi.io 平台 API Key
  【基础设置】
    代理地址 (twitter_proxy)                      - 如 http://127.0.0.1:7890
    轮询间隔 (twitter_poll_interval)              - 默认 5 分钟
  【定时任务调度】
    休眠开始时间 (twitter_sleep_start)            - 默认 0 (0:00)
    休眠结束时间 (twitter_sleep_end)              - 默认 8 (8:00)
  【Redis 缓存设置】
    Redis 主机/端口/密码/数据库编号
  【消息格式】
    合并转发消息 (twitter_use_node)               - 默认开启
    含媒体时隐藏文字 (twitter_no_text)            - 默认关闭
    图片质量 (twitter_image_quality)              - orig / large
    集体转发 (twitter_collective_forward)         - 默认关闭
    附带帖子链接 (twitter_include_tweet_link)     - 默认开启
  【内容过滤】
    推送转帖 (twitter_include_retweets)           - 默认开启
    链接识别 (twitter_link_recognition_enabled)   - 默认开启
  【翻译设置】
    翻译开关 (twitter_translate_enabled)          - 默认关闭
    目标语言 (twitter_translate_target_lang)      - 默认简体中文
    LLM Provider (twitter_translate_provider_id)  - 留空自动选择

当消息中包含 twitter.com 或 x.com 的推文链接时，自动解析并发送推文内容。
"""

import asyncio
import datetime
import json
import re
from dataclasses import dataclass, field

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Node, Nodes
from astrbot.api.star import Context, Star
import astrbot.api.message_components as Comp

from .twitter_api import TwitterAPI
from .twitter_renderer import (
    build_tweet_card_context,
    load_tweet_card_template,
    tweet_card_render_options,
)

# Twitter/X 链接正则
TWITTER_LINK_PATTERN = re.compile(
    r"(https?://(?:twitter\.com|x\.com)/([a-zA-Z0-9_]+)/status/(\d+))"
)

# KV 存储键名
KV_SUBS_KEY = "twitter_subs"

# Redis 缓存键前缀
REDIS_KEY_PREFIX = "twitter:tweet:"


@dataclass
class CachedTweet:
    """缓存的推文数据，用于集体转发"""

    username: str
    tweet_info: dict
    sub_config: dict
    nickname: str
    translated_text: str | None = None
    translate_model: str | None = None


class TwitterPlugin(Star):
    """Twitter 推文转发插件主类"""

    def _cfg(self, block: str, key: str, default, *legacy_keys: str):
        """读取分组配置，并兼容旧版顶层扁平配置。"""
        block_config = self.config.get(block, {}) or {}
        if isinstance(block_config, dict):
            val = block_config.get(key)
            if val is not None:
                return val

        for cfg_key in (key, *legacy_keys):
            val = self.config.get(cfg_key)
            if val is not None:
                return val

        return default

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config

        # 读取 twitterapi.io 配置
        self.api_key = str(
            self._cfg("twitterapi", "twitter_api_key", "")
            or self._cfg("basic", "twitter_nitter_url", "")  # 兼容旧版配置项名
            or ""
        ).strip()

        # 读取基础配置
        self.proxy = str(self._cfg("basic", "twitter_proxy", "") or "") or None
        self.use_node = bool(self._cfg("message_format", "twitter_use_node", True))
        self.no_text = bool(self._cfg("message_format", "twitter_no_text", False))
        self.link_recognition_enabled = bool(
            self._cfg(
                "content_filter",
                "twitter_link_recognition_enabled",
                True,
            )
        )
        self.poll_interval = max(
            1, int(self._cfg("basic", "twitter_poll_interval", 5))
        )
        self.collective_forward = bool(
            self._cfg("message_format", "twitter_collective_forward", False)
        )
        self.include_retweets = bool(
            self._cfg("content_filter", "twitter_include_retweets", True)
        )
        self.include_tweet_link = bool(
            self._cfg(
                "message_format",
                "twitter_include_tweet_link",
                True,
                "twitter_retweet_include_link",
            )
        )
        self.text_render_mode = str(
            self._cfg(
                "message_format",
                "twitter_text_render_mode",
                "text",
            )
            or "text"
        ).strip().lower()
        if self.text_render_mode not in ("text", "screenshot"):
            logger.warning(
                f"未知推文文本渲染模式: {self.text_render_mode}，已回退为 text"
            )
            self.text_render_mode = "text"
        self.screenshot_theme = str(
            self._cfg(
                "message_format",
                "twitter_screenshot_theme",
                "dark",
            )
            or "dark"
        ).strip().lower()
        if self.screenshot_theme not in ("dark", "light"):
            logger.warning(
                f"未知截图主题: {self.screenshot_theme}，已回退为 dark"
            )
            self.screenshot_theme = "dark"
        self.video_max_size_mb = max(
            1,
            int(
                self._cfg(
                    "message_format",
                    "twitter_video_max_size_mb",
                    256,
                )
            ),
        )
        self.collective_max_authors = max(
            1,
            int(
                self._cfg(
                    "message_format",
                    "twitter_collective_max_authors",
                    5,
                )
            ),
        )
        self.translate_enabled = bool(
            self._cfg("translation", "twitter_translate_enabled", False)
        )
        self.translate_target_lang = str(
            self._cfg(
                "translation",
                "twitter_translate_target_lang",
                "简体中文",
            )
            or "简体中文"
        )
        self.translate_provider_id = str(
            self._cfg("translation", "twitter_translate_provider_id", "") or ""
        ).strip()
        self.image_quality = str(
            self._cfg("message_format", "twitter_image_quality", "orig") or "orig"
        ).strip()

        # 读取休眠时间配置
        self.sleep_start = int(self._cfg("schedule", "twitter_sleep_start", 0))
        self.sleep_end = int(self._cfg("schedule", "twitter_sleep_end", 8))
        # 如果开始和结束相同，则不启用休眠
        self._sleep_enabled = self.sleep_start != self.sleep_end

        # 读取 Redis 配置
        self.redis_host = str(
            self._cfg("redis", "twitter_redis_host", "127.0.0.1")
        ).strip()
        self.redis_port = int(self._cfg("redis", "twitter_redis_port", 6379))
        self.redis_password = str(
            self._cfg("redis", "twitter_redis_password", "")
        ).strip()
        self.redis_db = int(self._cfg("redis", "twitter_redis_db", 0))

        # 初始化 Twitter API
        self.twitter_api = TwitterAPI(
            api_key=self.api_key,
            proxy=self.proxy,
            image_quality=self.image_quality,
        )

        # Redis 客户端
        self._redis: "redis.Redis | None" = None

        # 定时任务句柄
        self._poll_task: asyncio.Task | None = None
        self._running = False

        # 集体转发推文缓存：{umo: [CachedTweet, ...]}
        self._collected_tweets: dict[str, list[CachedTweet]] = {}

    # ========== Redis 缓存管理 ==========

    def _init_redis(self):
        """初始化 Redis 连接"""
        try:
            import redis as redis_lib

            redis_kwargs = {
                "host": self.redis_host,
                "port": self.redis_port,
                "db": self.redis_db,
                "socket_connect_timeout": 5,
                "socket_timeout": 5,
                "decode_responses": True,
                "protocol": 2,  # RESP2: 兼容旧版 Redis (3.x)，避免 HELLO 命令报错
            }
            if self.redis_password:
                redis_kwargs["password"] = self.redis_password

            self._redis = redis_lib.Redis(**redis_kwargs)
            # 测试连接
            self._redis.ping()
            logger.info(
                f"Redis 连接成功: {self.redis_host}:{self.redis_port} "
                f"db={self.redis_db}"
            )
        except ImportError:
            logger.warning(
                "redis 包未安装，推文缓存功能将使用内存缓存。"
                "建议执行: pip install redis"
            )
            self._redis = None
        except Exception as e:
            logger.warning(f"Redis 连接失败 ({self.redis_host}:{self.redis_port}): {e}")
            self._redis = None

    # 内存缓存回退（当 Redis 不可用时）
    _memory_cache: dict[str, str] = {}

    def _cache_key(self, username: str) -> str:
        """生成缓存键"""
        return f"{REDIS_KEY_PREFIX}{username}:latest"

    async def _cache_get_tweet(self, username: str) -> dict | None:
        """从缓存获取推文数据"""
        cache_key = self._cache_key(username)
        try:
            if self._redis:
                data = self._redis.get(cache_key)
                if data:
                    return json.loads(data)
            else:
                data = self._memory_cache.get(cache_key)
                if data:
                    return json.loads(data)
        except Exception as e:
            logger.warning(f"读取缓存失败 ({username}): {e}")
        return None

    async def _cache_set_tweet(self, username: str, tweet_data: dict):
        """将推文数据写入缓存"""
        cache_key = self._cache_key(username)
        try:
            json_str = json.dumps(tweet_data, ensure_ascii=False, default=str)
            if self._redis:
                # Redis 缓存：设置 1 小时过期
                self._redis.setex(cache_key, 3600, json_str)
            else:
                self._memory_cache[cache_key] = json_str
            logger.debug(f"推文缓存已更新: {username}")
        except Exception as e:
            logger.warning(f"写入缓存失败 ({username}): {e}")

    # ========== 休眠时间检查 ==========

    def _is_in_sleep_period(self) -> bool:
        """检查当前是否处于休眠时间段"""
        if not self._sleep_enabled:
            return False

        now = datetime.datetime.now()
        current_hour = now.hour

        if self.sleep_start < self.sleep_end:
            # 正常区间，如 0:00 - 8:00
            return self.sleep_start <= current_hour < self.sleep_end
        else:
            # 跨日区间，如 22:00 - 6:00
            return current_hour >= self.sleep_start or current_hour < self.sleep_end

    # ========== 生命周期 ==========

    async def initialize(self):
        """插件初始化"""
        logger.info("Twitter 推文转发插件初始化中...")

        # 集体转发模式与合并转发消息的兼容性校验
        if self.collective_forward and not self.use_node:
            logger.warning(
                "集体转发模式已开启但合并转发消息未开启，集体转发功能不会生效。"
                "请同时开启「使用合并转发消息」配置项。"
            )

        # 初始化 Redis
        self._init_redis()

        # 清理过期的媒体缓存文件（7 天以上 + 总大小超过 500MB）
        try:
            media_removed = self.twitter_api.cleanup_old_media_cache()
            if media_removed > 0:
                logger.info(f"启动时清理过期媒体缓存: {media_removed} 个文件")
        except Exception as e:
            logger.debug(f"媒体缓存清理跳过: {e}")

        # 验证 API key
        if not self.api_key:
            logger.warning(
                "未配置 twitterapi.io API Key，推文功能暂不可用。"
                "请在配置中设置 twitterapi.io 的 API Key"
            )
        else:
            is_valid = await self.twitter_api.validate_api_key()
            if is_valid:
                logger.info("twitterapi.io API Key 验证成功")
            else:
                logger.warning(
                    "twitterapi.io API Key 验证失败，请检查 Key 是否正确。"
                    "推文功能可能不可用"
                )

        # 启动定时轮询任务
        if self.api_key:
            self._running = True
            self._poll_task = asyncio.create_task(self._poll_tweets())
            if self._sleep_enabled:
                logger.info(
                    f"推文轮询已启动，间隔 {self.poll_interval} 分钟，"
                    f"休眠时段: {self.sleep_start}:00 - {self.sleep_end}:00"
                )
            else:
                logger.info(f"推文轮询已启动，间隔 {self.poll_interval} 分钟")

        logger.info("Twitter 推文转发插件初始化完成")

    async def terminate(self):
        """插件销毁"""
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        # 停止前发送缓存的推文，避免丢失
        if self._collected_tweets:
            logger.info("正在发送剩余缓存的推文...")
            await self._flush_collected_tweets()
        await self.twitter_api.close()
        # 关闭 Redis 连接
        if self._redis:
            try:
                self._redis.close()
            except Exception:
                pass
        logger.info("Twitter 推文转发插件已停止")

    # ========== 数据管理（KV 存储） ==========

    async def _get_subs(self) -> dict:
        """获取全部订阅数据"""
        return await self.get_kv_data(KV_SUBS_KEY, {})

    async def _save_subs(self, data: dict):
        """保存全部订阅数据"""
        await self.put_kv_data(KV_SUBS_KEY, data)

    # ========== 工具方法 ==========

    @staticmethod
    def _build_nickname(username: str, screen_name: str) -> str:
        """构建推主昵称显示

        参数:
            username: 推主用户名（如 elonmusk）
            screen_name: 显示昵称（如 Elon Musk）

        返回:
            格式化后的昵称，如 "@elonmusk (Elon Musk)" 或 "@elonmusk"
        """
        nickname = f"@{username}"
        if screen_name and screen_name != username:
            nickname += f" ({screen_name})"
        return nickname

    @staticmethod
    def _build_author_display(username: str, screen_name: str) -> str:
        """构建推文作者显示，兼容只有昵称或用户名的情况。"""
        username = str(username or "").lstrip("@")
        screen_name = str(screen_name or "")
        if username:
            return TwitterPlugin._build_nickname(username, screen_name or username)
        return screen_name or "未知用户"

    @staticmethod
    def _attach_timeline_item_metadata(tweet_info: dict, item: dict):
        """把时间线条目上的转帖元数据补到推文详情里。"""
        tweet_info["username"] = str(
            item.get("username") or tweet_info.get("username") or ""
        )
        if item.get("is_retweet"):
            tweet_info["retweet"] = {
                "retweeter_username": str(item.get("retweeter_username") or ""),
                "retweeter_screen_name": str(item.get("retweeter_screen_name") or ""),
            }

    @staticmethod
    def _tweet_has_media(tweet_info: dict) -> bool:
        """判断主贴或引用帖是否包含媒体。"""
        if tweet_info.get("images") or tweet_info.get("videos"):
            return True
        quote = tweet_info.get("quote") or {}
        return bool(quote.get("images") or quote.get("videos"))

    @staticmethod
    def _is_stream_video_url(video_url: str) -> bool:
        """判断视频 URL 是否为流媒体清单类资源。"""
        url = str(video_url or "").lower()
        return ".m3u8" in url or "vmap" in url

    def _video_limit_message(
        self, video_url: str, size_bytes: int | None = None
    ) -> str:
        """构建超限视频降级为链接时展示给用户的文本。"""
        size_note = ""
        if size_bytes:
            size_mb = size_bytes / 1024 / 1024
            size_note = f"（约 {size_mb:.1f} MB）"
        return (
            f"\n视频大小超过 {self.video_max_size_mb} MB{size_note}，"
            f"已改为发送链接：{video_url}"
        )

    async def _video_exceeds_size_limit(
        self, video_url: str
    ) -> tuple[bool, int | None]:
        """尽量检查视频大小；未知大小和流媒体 URL 默认放行。"""
        if self._is_stream_video_url(video_url):
            return False, None

        size_bytes = await self.twitter_api.get_remote_file_size(video_url)
        if size_bytes is None:
            return False, None

        limit_bytes = self.video_max_size_mb * 1024 * 1024
        return size_bytes > limit_bytes, size_bytes

    async def _append_media_components(
        self, chain: list, images: list, videos: list, context_label: str = "推文"
    ):
        """把图片和视频追加到消息链，供主贴和引用帖复用。

        图片通过代理预下载到临时文件再发送，避免 AstrBot 直连
        Twitter CDN (pbs.twimg.com) 超时。
        """
        for img_url in images:
            try:
                local_path = await self.twitter_api.download_media(str(img_url))
                if local_path:
                    img_comp = Comp.Image.fromFileSystem(local_path)
                    if img_comp is not None:
                        chain.append(img_comp)
                    else:
                        # 本地文件加载失败，使用文本链接
                        logger.warning(f"{context_label}图片文件加载失败: {img_url}")
                        chain.append(Comp.Plain(f"\n图片: {img_url}"))
                else:
                    # 下载失败，使用文本链接（不用 Comp.Image.fromURL，
                    # 因为 AstrBot 直连 pbs.twimg.com 在中国大陆必定超时）
                    logger.warning(f"{context_label}图片下载失败: {img_url}")
                    chain.append(Comp.Plain(f"\n图片: {img_url}"))
            except Exception as e:
                logger.warning(f"添加{context_label}图片失败: {img_url}, {e}")
                chain.append(Comp.Plain(f"\n图片: {img_url}"))

        for v_url in videos:
            video_url = str(v_url)
            try:
                exceeds_limit, size_bytes = await self._video_exceeds_size_limit(
                    video_url
                )
                if exceeds_limit:
                    logger.warning(
                        f"{context_label}视频超过大小限制，已改为链接: {video_url}"
                    )
                    chain.append(
                        Comp.Plain(
                            str(self._video_limit_message(video_url, size_bytes))
                        )
                    )
                    continue

                video_comp = Comp.Video.fromURL(video_url)
                if video_comp is not None:
                    chain.append(video_comp)
            except Exception as e:
                logger.warning(
                    f"添加{context_label}视频失败，回退为链接: {video_url}, {e}"
                )
                chain.append(Comp.Plain(str(f"\n视频: {video_url}")))

    async def _maybe_translate(
        self, tweet_info: dict, umo: str
    ) -> tuple[str | None, str | None]:
        """根据翻译配置，翻译推文文本和引用推文文本。

        优先使用 tweet_info 中已缓存的翻译（_translated_text / _translate_model），
        避免重复调用 LLM。翻译完成后将结果回写到 tweet_info 中以便缓存持久化。

        参数:
            tweet_info: 推文信息字典（翻译结果会回写到此字典）
            umo: 会话标识，用于获取 Provider

        返回:
            (主贴翻译后的文本, 翻译模型名称)；引用推文译文写入 quote.translated_text。
        """
        if not self.translate_enabled:
            return None, None

        original_text = str(tweet_info.get("text") or "")
        quote = tweet_info.get("quote") or {}
        quote_text = str(quote.get("text") or "")

        translated_text: str | None = None
        translate_model: str | None = None

        # ---- 主贴翻译 ----
        if original_text.strip():
            # 检查是否有已缓存的翻译
            cached_translation = tweet_info.get("_translated_text")
            cached_model = tweet_info.get("_translate_model")
            if cached_translation is not None:
                logger.debug(f"命中翻译缓存，跳过 LLM 调用")
                translated_text = cached_translation
                translate_model = cached_model
            else:
                main_translated, main_model = await self._translate_text(
                    original_text, umo
                )
                if main_model:
                    translated_text = main_translated
                    translate_model = main_model
                    # 回写到 tweet_info 以便后续缓存
                    tweet_info["_translated_text"] = translated_text
                    tweet_info["_translate_model"] = translate_model

        # ---- 引用推文翻译 ----
        if quote_text.strip():
            # 检查引用推文是否已有缓存翻译
            if quote.get("translated_text"):
                logger.debug("命中引用翻译缓存，跳过 LLM 调用")
                translate_model = translate_model or tweet_info.get("_translate_model")
            else:
                quote_translated, quote_model = await self._translate_text(
                    quote_text, umo
                )
                if quote_model:
                    quote["translated_text"] = quote_translated
                    translate_model = translate_model or quote_model

        return translated_text, translate_model

    async def _get_translate_provider_id(self, umo: str) -> str | None:
        """获取翻译用的 LLM Provider ID，按优先级回退

        回退顺序：
        1. 配置中指定的 provider_id
        2. 当前会话的 Provider
        3. 第一个可用的 Provider
        """
        # 1. 配置指定的 Provider
        if self.translate_provider_id:
            provider = self.context.get_provider_by_id(self.translate_provider_id)
            if provider:
                logger.debug(
                    f"翻译使用配置指定的 Provider: {self.translate_provider_id}"
                )
                return self.translate_provider_id
            logger.warning(
                f"配置的翻译 Provider '{self.translate_provider_id}' 不可用，尝试回退"
            )

        # 2. 当前会话的 Provider
        try:
            provider_id = await self.context.get_current_chat_provider_id(umo=umo)
            if provider_id:
                logger.debug(f"翻译使用当前会话的 Provider: {provider_id}")
                return provider_id
        except Exception as e:
            logger.warning(f"无法获取会话 Provider ID: {e}")

        # 3. 第一个可用的 Provider
        try:
            providers = self.context.get_all_providers()
            if providers:
                provider_id = providers[0].meta().id
                logger.debug(f"翻译使用第一个可用 Provider: {provider_id}")
                return provider_id
        except Exception as e:
            logger.warning(f"无法获取可用 Provider: {e}")

        logger.error("翻译功能：未找到任何可用的 LLM Provider")
        return None

    async def _translate_text(self, text: str, umo: str) -> tuple[str, str | None]:
        """翻译推文文本

        参考 astrbot_plugin_qq_group_daily_analysis 项目的 LLM 调用思路：
        - 使用 system_prompt 分离翻译指令与待翻译内容，提高翻译质量和可靠性
        - 翻译失败时简单重试一次

        参数:
            text: 原始文本
            umo: 订阅者的会话标识，用于获取 Provider

        返回:
            (翻译后的文本, 执行翻译的模型名称)；翻译失败时返回 (原文, None)
        """
        if not text or not text.strip():
            return text, None

        provider_id = await self._get_translate_provider_id(umo)
        if not provider_id:
            return text, None

        system_prompt = (
            f"你是一个专业的翻译助手。请将用户提供的文本翻译为{self.translate_target_lang}。"
            f"规则：仅输出翻译结果，不要添加任何解释、前缀、注释或原文对照。"
            f"保持原文的语气和格式（如换行、表情符号等）。"
        )

        max_retries = 1
        for attempt in range(max_retries):
            try:
                llm_resp = await self.context.llm_generate(
                    chat_provider_id=provider_id,
                    prompt=text,
                    system_prompt=system_prompt,
                )
                translated = llm_resp.completion_text
                if translated and translated.strip():
                    # 获取模型名称用于标注
                    model_name = provider_id
                    try:
                        provider = self.context.get_provider_by_id(provider_id)
                        if provider and hasattr(provider, "meta"):
                            meta = provider.meta()
                            if meta and hasattr(meta, "model_name"):
                                model_name = meta.model_name or provider_id
                    except Exception:
                        pass
                    return translated.strip(), model_name
                else:
                    logger.warning(f"翻译返回为空 (尝试 {attempt + 1}/{max_retries})")
            except Exception as e:
                logger.error(f"翻译失败 (尝试 {attempt + 1}/{max_retries}): {e}")

            if attempt < max_retries - 1:
                await asyncio.sleep(1)

        logger.warning("翻译全部重试失败，使用原文")
        return text, None

    async def _build_tweet_chain(
        self,
        username: str,
        tweet_info: dict,
        sub_config: dict | None = None,
        translated_text: str | None = None,
        translate_model: str | None = None,
    ) -> list:
        """构建推文消息链

        参数:
            translated_text: 翻译后的文本，若提供则替换原文
            translate_model: 执行翻译的模型名称，用于末尾标注
        """
        if sub_config is None:
            sub_config = {"r18": True, "media": False, "status": True}

        text = str(translated_text or tweet_info.get("text") or "")
        images = tweet_info.get("images") or []
        quote = tweet_info.get("quote")
        tweet_id = str(tweet_info.get("tweet_id") or "")
        author_username = str(tweet_info.get("username") or username)
        screen_name = str(tweet_info.get("screen_name") or author_username)
        retweet = tweet_info.get("retweet") or None

        chain = []
        text_sections: list[str] = []

        def append_text_section(value: str) -> None:
            value = str(value or "").strip()
            if value:
                text_sections.append(value)

        # 头部信息
        nickname = self._build_author_display(author_username, screen_name)
        if retweet:
            retweeter_username = str(retweet.get("retweeter_username") or username)
            retweeter_screen_name = str(
                retweet.get("retweeter_screen_name") or retweeter_username
            )
            retweeter = self._build_author_display(
                retweeter_username, retweeter_screen_name
            )
            append_text_section(f"{retweeter} 转发了 {nickname} 的帖子")
        else:
            append_text_section(nickname)

        # 推文正文
        has_media = self._tweet_has_media(tweet_info)
        if not (self.no_text and has_media):
            if text:
                append_text_section(text)

        # 引用推文
        if quote:
            quote_author_username = str(quote.get("username") or "")
            quote_author = str(quote.get("author") or quote_author_username)
            quote_text = str(quote.get("translated_text") or quote.get("text") or "")
            quote_display = self._build_author_display(
                quote_author_username, quote_author
            )
            append_text_section(f"{nickname} 引用了 {quote_display} 的帖子")
            if quote_text:
                append_text_section(quote_text)

        # 推文链接
        if tweet_id and self.include_tweet_link:
            append_text_section(f"https://x.com/{author_username}/status/{tweet_id}")

        # 翻译说明标注
        quote_translated = bool((quote or {}).get("translated_text"))
        if translate_model and (translated_text is not None or quote_translated):
            append_text_section(f"（由 {translate_model} 翻译自原文）")

        if text_sections:
            self._append_to_last_plain(chain, "\n\n".join(text_sections))

        # 引用媒体
        if quote:
            await self._append_media_components(
                chain,
                quote.get("images") or [],
                quote.get("videos") or [],
                context_label="引用推文",
            )

        # 主贴媒体
        await self._append_media_components(
            chain, images, tweet_info.get("videos") or [], context_label="推文"
        )

        # 过滤 None 值，防止类型验证错误
        chain = [c for c in chain if c is not None]
        return chain

    def _tweet_link_component(
        self, tweet_info: dict, fallback_username: str
    ) -> Comp.Plain | None:
        """构建可选的推文链接组件。"""
        tweet_id = str(tweet_info.get("tweet_id") or "")
        author_username = str(tweet_info.get("username") or fallback_username)
        if not (tweet_id and self.include_tweet_link):
            return None
        return Comp.Plain(
            str(f"https://x.com/{author_username}/status/{tweet_id}")
        )

    @staticmethod
    def _append_to_last_plain(chain: list, text: str) -> None:
        """尽量把文本追加到最后一个纯文本组件，避免适配器拼接组件时吞换行。"""
        if chain and isinstance(chain[-1], Comp.Plain):
            current_text = getattr(chain[-1], "text", None)
            if isinstance(current_text, str):
                chain[-1].text = current_text + text
                return
        chain.append(Comp.Plain(str(text)))

    @staticmethod
    def _rendered_image_component(rendered_url: str):
        """把 html_render 的输出转换为图片组件。"""
        rendered_url = str(rendered_url or "").strip()
        if not rendered_url:
            return None
        if rendered_url.startswith(("http://", "https://")):
            return Comp.Image.fromURL(rendered_url)
        return Comp.Image.fromFileSystem(rendered_url)

    async def _build_tweet_message_chain(
        self,
        username: str,
        tweet_info: dict,
        sub_config: dict | None = None,
        translated_text: str | None = None,
        translate_model: str | None = None,
    ) -> list:
        """按当前文本渲染模式构建推文消息链。"""
        if self.text_render_mode != "screenshot":
            return await self._build_tweet_chain(
                username,
                tweet_info,
                sub_config,
                translated_text=translated_text,
                translate_model=translate_model,
            )

        try:
            return await self._build_screenshot_tweet_chain(
                username,
                tweet_info,
                sub_config,
                translated_text=translated_text,
                translate_model=translate_model,
            )
        except Exception as e:
            logger.warning(f"推文截图渲染失败，已回退为文本消息: {e}")
            return await self._build_tweet_chain(
                username,
                tweet_info,
                sub_config,
                translated_text=translated_text,
                translate_model=translate_model,
            )

    async def _build_screenshot_tweet_chain(
        self,
        username: str,
        tweet_info: dict,
        sub_config: dict | None = None,
        translated_text: str | None = None,
        translate_model: str | None = None,
    ) -> list:
        """构建正文以 X 风格卡片截图展示的消息链。"""
        if sub_config is None:
            sub_config = {"r18": True, "media": False, "status": True}

        chain: list = []
        has_media = self._tweet_has_media(tweet_info)
        render_text_card = not (self.no_text and has_media)

        if render_text_card:
            context = build_tweet_card_context(
                username,
                tweet_info,
                translated_text=translated_text,
                translate_model=translate_model,
                theme=self.screenshot_theme,
            )
            rendered_url = await self.html_render(
                load_tweet_card_template(),
                context,
                options=tweet_card_render_options(context),
            )
            image_comp = self._rendered_image_component(rendered_url)
            if image_comp is None:
                raise RuntimeError("html_render returned an empty image result")
            chain.append(image_comp)

        link_comp = self._tweet_link_component(tweet_info, username)
        if link_comp is not None:
            if chain:
                chain.append(Comp.Plain("\n"))
            chain.append(link_comp)

        quote = tweet_info.get("quote") or None
        if quote:
            await self._append_media_components(
                chain,
                quote.get("images") or [],
                quote.get("videos") or [],
                context_label="引用推文",
            )

        await self._append_media_components(
            chain,
            tweet_info.get("images") or [],
            tweet_info.get("videos") or [],
            context_label="推文",
        )

        return [c for c in chain if c is not None]

    def _split_chain_for_nodes(
        self, chain: list, nickname: str
    ) -> tuple[list[Node], list[Comp.Video]]:
        """将消息链分离为 Node 列表和待独立发送的视频列表

        视频不能放在 Node 中，否则下载+上传会超出 WebSocket API 超时时间，
        需要作为独立消息发送。

        参数:
            chain: _build_tweet_chain 生成的消息链
            nickname: Node 显示的昵称

        返回:
            (Node 列表, 待独立发送的视频组件列表)
        """
        nodes: list[Node] = []
        video_parts: list[Comp.Video] = []
        text_parts: list = []

        def flush_text_parts():
            nonlocal text_parts
            if text_parts:
                nodes.append(Node(content=text_parts, name=nickname))
                text_parts = []

        for comp in chain:
            if isinstance(comp, Comp.Video):
                video_parts.append(comp)
            elif isinstance(comp, Comp.Image):
                flush_text_parts()
                nodes.append(Node(content=[comp], name=nickname))
            else:
                text_parts.append(comp)

        # 文本节点
        if text_parts:
            nodes.append(Node(content=text_parts, name=nickname))

        return nodes, video_parts

    @staticmethod
    def _build_plain_chain(chain: list) -> list:
        """构建普通消息链，保留图片并把视频转换为链接文本。"""
        plain_chain = []
        for comp in chain:
            if isinstance(comp, Comp.Video):
                vid_url = getattr(comp, "file", "") or getattr(comp, "url", "")
                if vid_url:
                    plain_chain.append(Comp.Plain(str(f"\n视频: {vid_url}")))
            else:
                plain_chain.append(comp)
        return plain_chain

    @staticmethod
    def _split_plain_chain_and_videos(
        chain: list,
    ) -> tuple[list, list[Comp.Video]]:
        """构建普通消息链，并分离需要独立发送的视频组件。"""
        plain_chain = []
        video_parts: list[Comp.Video] = []
        for comp in chain:
            if isinstance(comp, Comp.Video):
                video_parts.append(comp)
            else:
                plain_chain.append(comp)
        return plain_chain, video_parts

    async def _send_video_or_fallback(self, umo: str, vid_comp: Comp.Video):
        """发送视频组件，失败时回退为链接

        参数:
            umo: 目标会话标识
            vid_comp: 视频组件
        """
        try:
            vid_chain = MessageChain(chain=[vid_comp])
            await self.context.send_message(umo, vid_chain)
        except Exception as vid_err:
            logger.warning(f"视频发送失败，回退为链接: {vid_err}")
            vid_url = getattr(vid_comp, "file", "") or getattr(
                vid_comp, "url", ""
            )
            if vid_url:
                await self.context.send_message(
                    umo,
                    MessageChain(
                        chain=[Comp.Plain(str(f"视频: {vid_url}"))]
                    ),
                )

    async def _push_tweet_to_subscribers(
        self, username: str, tweet_info: dict, user_info: dict
    ):
        """将推文推送给所有订阅者（或缓存到集体转发队列）

        注意：每次推送时实时从 KV 存储读取最新订阅数据，
        而非使用轮询开始时的快照，确保订阅状态的变更（取关/新订阅）能即时生效。
        """
        # 实时读取最新订阅数据，避免因订阅状态变更导致的推送错误
        latest_subs = await self._get_subs()
        if username not in latest_subs:
            return  # 该推主已无任何订阅者（可能已被全部取关并删除）
        latest_user_info = latest_subs[username]
        subscribers = latest_user_info.get("subscribers") or {}
        screen_name = str(
            latest_user_info.get("screen_name")
            or tweet_info.get("screen_name")
            or username
        )
        retweet = tweet_info.get("retweet") or {}
        if retweet:
            nickname = self._build_author_display(
                str(retweet.get("retweeter_username") or username),
                str(retweet.get("retweeter_screen_name") or screen_name),
            )
        else:
            nickname = self._build_nickname(username, screen_name)

        # 翻译推文（如果开启），同一推文只翻译一次
        first_umo = next(iter(subscribers), "")
        translated_text, translate_model = await self._maybe_translate(
            tweet_info, first_umo
        )
        if translate_model:
            original_text = str(tweet_info.get("text") or "")
            quote_text = str((tweet_info.get("quote") or {}).get("text") or "")
            logger.info(
                f"推文翻译完成 @{username}: "
                f"模型={translate_model}, "
                f"原文长度={len(original_text) + len(quote_text)}, "
                f"译文长度={len(translated_text or '')}"
            )

        for umo, sub_config in subscribers.items():
            if not sub_config.get("status", True):
                continue

            # R18 过滤
            is_r18 = tweet_info.get("is_r18", False)
            if is_r18 and not sub_config.get("r18", False):
                continue

            # 媒体过滤
            if sub_config.get("media", False) and not self._tweet_has_media(
                tweet_info
            ):
                continue

            # 集体转发模式：缓存推文，轮询结束后统一发送
            if self.collective_forward and self.use_node:
                if umo not in self._collected_tweets:
                    self._collected_tweets[umo] = []
                self._collected_tweets[umo].append(
                    CachedTweet(
                        username=username,
                        tweet_info=tweet_info,
                        sub_config=sub_config,
                        nickname=nickname,
                        translated_text=translated_text,
                        translate_model=translate_model,
                    )
                )
                continue

            # 即时推送模式
            await self._send_tweet_to_subscriber(
                umo,
                username,
                tweet_info,
                sub_config,
                nickname,
                translated_text=translated_text,
                translate_model=translate_model,
            )

    async def _send_tweet_to_subscriber(
        self,
        umo: str,
        username: str,
        tweet_info: dict,
        sub_config: dict,
        nickname: str,
        translated_text: str | None = None,
        translate_model: str | None = None,
    ):
        """向单个订阅者发送推文消息"""
        try:
            chain = await self._build_tweet_message_chain(
                username,
                tweet_info,
                sub_config,
                translated_text=translated_text,
                translate_model=translate_model,
            )
            if not chain:
                return

            if self.use_node:
                # 合并转发模式：使用 Node/Nodes 构建合并转发消息
                try:
                    nodes, video_parts = self._split_chain_for_nodes(chain, nickname)

                    # 发送合并转发消息（文本+图片）
                    if nodes:
                        message_chain = MessageChain(chain=[Nodes(nodes)])
                        await self.context.send_message(umo, message_chain)

                    # 视频作为独立消息逐条发送
                    for vid_comp in video_parts:
                        await self._send_video_or_fallback(umo, vid_comp)

                except Exception as node_err:
                    # 合并转发失败，回退到普通消息链（视频改为链接）
                    logger.warning(f"合并转发失败，回退到普通消息: {node_err}")
                    fallback_chain = self._build_plain_chain(chain)
                    if fallback_chain:
                        message_chain = MessageChain(chain=fallback_chain)
                        await self.context.send_message(umo, message_chain)
            else:
                # 普通消息模式：正文和图片先发，视频独立发送，避免混入普通链导致文本异常
                plain_chain, video_parts = self._split_plain_chain_and_videos(chain)
                if plain_chain:
                    message_chain = MessageChain(chain=plain_chain)
                    await self.context.send_message(umo, message_chain)
                for vid_comp in video_parts:
                    await self._send_video_or_fallback(umo, vid_comp)

            logger.info(f"推文已推送至 {umo}")
        except Exception as e:
            logger.error(f"推送推文至 {umo} 失败: {e}")

    async def _flush_collected_tweets(self):
        """将缓存的推文按推主分组打包为合并转发消息发送"""
        if not self._collected_tweets:
            return

        collected = self._collected_tweets
        self._collected_tweets = {}

        # 实时读取最新订阅数据，用于校验每个 UMO 是否仍为有效订阅者
        latest_subs = await self._get_subs()

        for umo, cached_list in collected.items():
            if not cached_list:
                continue

            # 校验该 UMO 是否仍是至少一个推主的订阅者
            valid_tweets: list[CachedTweet] = []
            for ct in cached_list:
                user_info = latest_subs.get(ct.username)
                if user_info and umo in user_info.get("subscribers", {}):
                    sub_cfg = user_info["subscribers"][umo]
                    # 检查推送状态
                    if sub_cfg.get("status", True):
                        valid_tweets.append(ct)
                    else:
                        logger.debug(
                            f"集体转发跳过已暂停的订阅: {umo} -> @{ct.username}"
                        )
                else:
                    logger.debug(
                        f"集体转发跳过已取关的订阅: {umo} -> @{ct.username}"
                    )

            if not valid_tweets:
                continue

            try:
                # 按推主分组，保持原始顺序（先到的推主排前面）
                seen_authors: dict[str, list[CachedTweet]] = {}
                author_order: list[str] = []
                for ct in valid_tweets:
                    if ct.username not in seen_authors:
                        seen_authors[ct.username] = []
                        author_order.append(ct.username)
                    seen_authors[ct.username].append(ct)

                # 按最大推主数分批
                max_authors = self.collective_max_authors
                author_batches: list[list[str]] = []
                for i in range(0, len(author_order), max_authors):
                    author_batches.append(author_order[i : i + max_authors])

                for batch_idx, batch_authors in enumerate(author_batches):
                    nodes: list[Node] = []
                    video_queue: list[Comp.Video] = []

                    for author in batch_authors:
                        for ct in seen_authors[author]:
                            chain = await self._build_tweet_message_chain(
                                ct.username,
                                ct.tweet_info,
                                ct.sub_config,
                                translated_text=ct.translated_text,
                                translate_model=ct.translate_model,
                            )
                            if not chain:
                                continue

                            ct_nodes, ct_videos = self._split_chain_for_nodes(
                                chain, ct.nickname
                            )
                            nodes.extend(ct_nodes)
                            video_queue.extend(ct_videos)

                    # 发送合并转发消息
                    if nodes:
                        batch_label = ""
                        if len(author_batches) > 1:
                            batch_label = (
                                f"（第{batch_idx + 1}/{len(author_batches)}批）"
                            )
                        try:
                            message_chain = MessageChain(chain=[Nodes(nodes)])
                            await self.context.send_message(umo, message_chain)
                            logger.info(
                                f"集体转发已推送至 {umo} "
                                f"{batch_label}共 {len(nodes)} 个节点"
                            )
                        except Exception as node_err:
                            logger.warning(
                                f"集体合并转发失败，回退逐条发送: {node_err}"
                            )
                            # 回退：逐条发送
                            for ct in [
                                ct
                                for a in batch_authors
                                for ct in seen_authors[a]
                            ]:
                                await self._send_tweet_to_subscriber(
                                    umo,
                                    ct.username,
                                    ct.tweet_info,
                                    ct.sub_config,
                                    ct.nickname,
                                    translated_text=ct.translated_text,
                                    translate_model=ct.translate_model,
                                )
                            # 回退模式下跳过独立视频发送（已在逐条发送中处理）
                            video_queue.clear()

                    # 逐条发送视频（独立消息，避免超时）
                    for vid_comp in video_queue:
                        await self._send_video_or_fallback(umo, vid_comp)

            except Exception as e:
                logger.error(f"集体转发推送至 {umo} 失败: {e}")
                # 回退：逐条发送该订阅者的缓存推文
                for ct in valid_tweets:
                    try:
                        await self._send_tweet_to_subscriber(
                            umo,
                            ct.username,
                            ct.tweet_info,
                            ct.sub_config,
                            ct.nickname,
                            translated_text=ct.translated_text,
                            translate_model=ct.translate_model,
                        )
                    except Exception as fallback_err:
                        logger.error(
                            f"集体转发回退逐条发送也失败: {fallback_err}"
                        )

    # ========== 定时轮询 ==========

    async def _poll_tweets(self):
        """定时轮询推文"""
        logger.debug("推文轮询任务已启动（后台协程）")
        while self._running:
            loop_start = datetime.datetime.now()
            try:
                # 检查是否在休眠时间段
                if self._is_in_sleep_period():
                    logger.debug(
                        f"轮询唤醒，但处于休眠时段 "
                        f"({self.sleep_start}:00 - {self.sleep_end}:00)，跳过"
                    )
                else:
                    logger.debug(
                        f"轮询触发 @ {loop_start.strftime('%H:%M:%S')}，"
                        f"间隔={self.poll_interval}min"
                    )
                    await self._check_all_subscriptions()
            except Exception as e:
                logger.error(f"推文轮询出错: {e}", exc_info=True)
            await asyncio.sleep(self.poll_interval * 60)

    async def _check_all_subscriptions(self):
        """检查所有订阅的新推文"""
        subscribe_list = await self._get_subs()
        if not subscribe_list:
            logger.debug("轮询: 无订阅，跳过")
            return

        usernames = list(subscribe_list.keys())
        logger.debug(
            f"轮询开始: {len(usernames)} 个推主 "
            f"({', '.join(f'@{u}' for u in usernames)})"
        )

        results: list[bool] = []
        for username, info in subscribe_list.items():
            try:
                result = await self._check_user_tweets(username, info)
                results.append(result)
                await asyncio.sleep(3)  # 避免频繁请求
            except Exception as e:
                logger.error(f"检查 {username} 推文失败: {e}")
                results.append(False)

        success_count = sum(1 for r in results if r)
        logger.debug(
            f"轮询结束: {success_count}/{len(results)} 个推主检查成功"
        )

        # 集体转发模式：轮询结束后统一发送缓存的推文
        if self.collective_forward and self._collected_tweets:
            await self._flush_collected_tweets()

    async def _check_user_tweets(self, username: str, info: dict) -> bool:
        """检查某个用户的新推文，使用 advanced_search + since_time 过滤。

        通过 /twitter/tweet/advanced_search 接口，以 since_time（上次轮询时间）
        作为过滤条件，支持游标分页，确保两次轮询之间的所有推文都被获取，
        避免 /twitter/user/last_tweets 返回固定数量导致的推文遗漏。
        """
        try:
            # 获取上次成功轮询时间，处理旧订阅的向后兼容
            last_poll_time = info.get("last_poll_time", 0.0)
            if not last_poll_time:
                # 旧订阅（仅有 since_id，无 last_poll_time）→ 初始化为当前时间
                # 避免首次使用新逻辑时推送大量历史推文
                last_poll_time = datetime.datetime.now().timestamp()
                logger.debug(
                    f"@{username}: 无 last_poll_time（旧订阅），"
                    f"初始化为当前时间"
                )

            logger.debug(
                f"检查 @{username}: since_time={last_poll_time:.0f} "
                f"({datetime.datetime.fromtimestamp(last_poll_time).strftime('%m-%d %H:%M:%S')})"
            )

            # 在 API 调用前记录时间，成功后用作新的 last_poll_time
            # 确保不会遗漏在 API 调用和推送期间发布的新推文
            poll_start = datetime.datetime.now().timestamp()
            new_tweets = await self.twitter_api.get_user_tweets_since_time(
                username, last_poll_time, include_retweets=self.include_retweets
            )

            if not new_tweets:
                logger.debug(f"  @{username}: 无新推文")
                # 无新推文 → 不更新 last_poll_time，下次仍以同一时间点查询
                return True

            logger.debug(
                f"  @{username}: 发现 {len(new_tweets)} 条新推文 "
                f"(ID: {new_tweets[0].get('tweet_id','')[:12]}..."
                f" ~ {new_tweets[-1].get('tweet_id','')[:12]}...)"
            )

            # 再次确认该推主仍有订阅者（可能在获取推文期间被取关）
            latest_subs = await self._get_subs()
            if username not in latest_subs:
                logger.info(f"@{username} 已无订阅者，跳过推送")
                # 无订阅者但仍更新 last_poll_time，避免重复拉取已取关用户的推文
                subs = await self._get_subs()
                if username in subs:
                    subs[username]["last_poll_time"] = poll_start
                    await self._save_subs(subs)
                return True

            # 按时间正序（最旧在前）逐条推送
            for tweet_info in new_tweets:
                await self._push_tweet_to_subscribers(username, tweet_info, info)

            # 推送成功 → 更新 last_poll_time 为 API 调用前的时间
            # 仅当获取到新推文且成功推送后才更新，确保不会因失败而跳过推文
            subs = await self._get_subs()
            if username in subs:
                subs[username]["last_poll_time"] = poll_start
                await self._save_subs(subs)

            # 用最新一条非转帖推文更新缓存（优先非转帖；全转帖时用最后一条）
            latest_tweet = new_tweets[-1]
            for t in reversed(new_tweets):
                if not t.get("retweet"):
                    latest_tweet = t
                    break
            await self._cache_set_tweet(username, latest_tweet)

            logger.debug(
                f"  @{username}: last_poll_time 已更新为 "
                f"{datetime.datetime.fromtimestamp(poll_start).strftime('%m-%d %H:%M:%S')}，"
                f"缓存已刷新"
            )
            return True
        except Exception as e:
            logger.error(f"获取 {username} 推文异常: {e}", exc_info=True)
            # 异常时绝不更新 last_poll_time，下次重试仍使用同一时间点
            return False

    # ========== 指令处理 ==========

    @filter.command("推特关注", alias={"twitter_follow"})
    async def follow_twitter(self, event: AstrMessageEvent, username: str = ""):
        """订阅推主，格式: /推特关注 <推主id> [r18] [媒体]"""
        if not self.api_key:
            yield event.plain_result(
                "未配置 twitterapi.io API Key，请先在配置中设置"
            )
            return

        if not username:
            yield event.plain_result(
                "请提供推主ID，用法: /推特关注 <推主ID> [r18] [媒体]"
            )
            return

        username = username.strip("@").strip()

        # 解析可选参数
        msg_str = event.message_str
        extra_args = msg_str.strip().split()[2:]  # 跳过指令名和用户名
        r18 = "r18" in extra_args
        media_only = "媒体" in extra_args

        # 获取用户信息
        user_info = await self.twitter_api.get_user_info(username)
        if not user_info["status"]:
            yield event.plain_result(f"未找到用户: {username}")
            return

        # 初始化 last_poll_time 为当前时间（首次轮询从此时间点开始）
        last_poll_time = datetime.datetime.now().timestamp()

        umo = event.unified_msg_origin

        # 添加订阅
        subs = await self._get_subs()
        session_config = {
            "status": True,
            "r18": r18,
            "media": media_only,
        }

        if username not in subs:
            subs[username] = {
                "screen_name": user_info["screen_name"],
                "last_poll_time": last_poll_time,
                "subscribers": {},
            }

        subs[username]["subscribers"][umo] = session_config
        subs[username]["screen_name"] = user_info["screen_name"]
        # 已存在订阅：保留原有 last_poll_time，新订阅者从当前时间开始不会收到历史推文
        if username in subs and "last_poll_time" not in subs[username]:
            subs[username]["last_poll_time"] = last_poll_time

        await self._save_subs(subs)

        r18_str = " | R18" if r18 else ""
        media_str = " | 仅媒体" if media_only else ""
        bio = user_info["bio"][:100] + ("..." if len(user_info["bio"]) > 100 else "")
        result = (
            f"订阅成功!\n"
            f"ID: {username}\n"
            f"昵称: {user_info['screen_name']}\n"
            f"简介: {bio}\n"
            f"选项: {r18_str}{media_str}"
        )
        yield event.plain_result(result)

    @filter.command("推特批量关注", alias={"twitter_batch_follow"})
    async def batch_follow_twitter(self, event: AstrMessageEvent):
        """批量订阅推主，格式: /推特批量关注 <推主id1> <推主id2> ... [r18] [媒体]"""
        if not self.api_key:
            yield event.plain_result(
                "未配置 twitterapi.io API Key，请先在配置中设置"
            )
            return

        # 解析消息：提取用户名和选项
        msg_str = event.message_str.strip()
        tokens = msg_str.split()[1:]  # 跳过指令名
        if not tokens:
            yield event.plain_result(
                "请提供推主ID，用法: /推特批量关注 <推主ID1> <推主ID2> ... [r18] [媒体]"
            )
            return

        r18 = "r18" in tokens
        media_only = "媒体" in tokens
        usernames = [t.strip("@").strip() for t in tokens if t not in ("r18", "媒体")]

        if not usernames:
            yield event.plain_result("请提供至少一个推主ID")
            return

        yield event.plain_result(f"正在批量订阅 {len(usernames)} 个推主，请稍候...")

        umo = event.unified_msg_origin
        subs = await self._get_subs()
        results: list[str] = []
        success_count = 0

        for username in usernames:
            try:
                # 获取用户信息
                user_info = await self.twitter_api.get_user_info(username)
                if not user_info["status"]:
                    results.append(f"❌ @{username} - 未找到用户")
                    continue

                # 初始化 last_poll_time 为当前时间（首次轮询从此时间点开始）
                last_poll_time = datetime.datetime.now().timestamp()

                # 添加订阅
                session_config = {
                    "status": True,
                    "r18": r18,
                    "media": media_only,
                }

                if username not in subs:
                    subs[username] = {
                        "screen_name": user_info["screen_name"],
                        "last_poll_time": last_poll_time,
                        "subscribers": {},
                    }

                subs[username]["subscribers"][umo] = session_config
                subs[username]["screen_name"] = user_info["screen_name"]
                if "last_poll_time" not in subs[username]:
                    subs[username]["last_poll_time"] = last_poll_time

                success_count += 1
                r18_str = " | R18" if r18 else ""
                media_str = " | 仅媒体" if media_only else ""
                results.append(
                    f"✅ @{username} ({user_info['screen_name']}){r18_str}{media_str}"
                )

            except Exception as e:
                results.append(f"❌ @{username} - 订阅失败: {e}")

        # 一次性保存所有变更
        await self._save_subs(subs)

        # 汇总结果
        summary = (
            f"批量订阅完成: 成功 {success_count}/{len(usernames)}\n"
            + "\n".join(results)
        )
        yield event.plain_result(summary)

    @filter.command("推特取关", alias={"twitter_unfollow"})
    async def unfollow_twitter(self, event: AstrMessageEvent, username: str = ""):
        """取关推主，格式: /推特取关 <推主id>"""
        if not username:
            yield event.plain_result("请提供推主ID，用法: /推特取关 <推主ID>")
            return

        username = username.strip("@").strip()
        umo = event.unified_msg_origin

        subs = await self._get_subs()
        if username not in subs:
            yield event.plain_result(f"未订阅推主: {username}")
            return

        if umo not in subs[username].get("subscribers", {}):
            yield event.plain_result(f"当前会话未订阅 {username}")
            return

        subs[username]["subscribers"].pop(umo)

        # 如果该推主没有任何订阅者了，删除该推主
        if not subs[username].get("subscribers", {}):
            subs.pop(username)

        await self._save_subs(subs)
        yield event.plain_result(f"已取关 {username}")

    @filter.command("推特批量取关", alias={"twitter_batch_unfollow"})
    async def batch_unfollow_twitter(self, event: AstrMessageEvent):
        """批量取关推主，格式: /推特批量取关 <推主id1> <推主id2> ..."""
        msg_str = event.message_str.strip()
        tokens = msg_str.split()[1:]  # 跳过指令名
        if not tokens:
            yield event.plain_result(
                "请提供推主ID，用法: /推特批量取关 <推主ID1> <推主ID2> ..."
            )
            return

        usernames = [t.strip("@").strip() for t in tokens]
        umo = event.unified_msg_origin
        subs = await self._get_subs()

        results: list[str] = []
        success_count = 0

        for username in usernames:
            if username not in subs:
                results.append(f"❌ @{username} - 未订阅此推主")
                continue

            if umo not in subs[username].get("subscribers", {}):
                results.append(f"❌ @{username} - 当前会话未订阅")
                continue

            subs[username]["subscribers"].pop(umo)

            # 如果该推主没有任何订阅者了，删除该推主
            if not subs[username].get("subscribers", {}):
                subs.pop(username)

            success_count += 1
            results.append(f"✅ @{username} - 已取关")

        # 一次性保存所有变更
        await self._save_subs(subs)

        # 汇总结果
        summary = (
            f"批量取关完成: 成功 {success_count}/{len(usernames)}\n"
            + "\n".join(results)
        )
        yield event.plain_result(summary)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("推特清空订阅", alias={"twitter_clear_all"})
    async def clear_all_subscriptions(self, event: AstrMessageEvent):
        """清空所有推文订阅（仅管理员），格式: /推特清空订阅"""
        subs = await self._get_subs()
        if not subs:
            yield event.plain_result("当前没有任何订阅")
            return

        total_authors = len(subs)
        total_subscribers = sum(
            len(info.get("subscribers", {})) for info in subs.values()
        )

        await self._save_subs({})
        # 清空集体转发缓存
        self._collected_tweets.clear()

        yield event.plain_result(
            f"已清空所有订阅: 共 {total_authors} 个推主, "
            f"{total_subscribers} 个订阅关系"
        )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("推特清空缓存", alias={"twitter_clear_cache"})
    async def clear_cache(self, event: AstrMessageEvent):
        """清空推文缓存（仅管理员），格式: /推特清空缓存"""
        cleared = 0
        try:
            if self._redis:
                # 清空 Redis 中所有 twitter:tweet: 前缀的键
                keys = self._redis.keys(f"{REDIS_KEY_PREFIX}*")
                if keys:
                    cleared = len(keys)
                    self._redis.delete(*keys)
            else:
                # 清空内存缓存
                keys = [
                    k for k in self._memory_cache
                    if k.startswith(REDIS_KEY_PREFIX)
                ]
                cleared = len(keys)
                for k in keys:
                    del self._memory_cache[k]
        except Exception as e:
            logger.error(f"清空缓存失败: {e}")
            yield event.plain_result(f"清空缓存失败: {e}")
            return

        # 同时清空媒体文件缓存
        media_cleared = self.twitter_api.clear_media_cache()

        logger.info(f"已清空推文缓存: {cleared} 条, 媒体缓存: {media_cleared} 个文件")
        yield event.plain_result(
            f"已清空推文缓存: {cleared} 条\n"
            f"已清空媒体缓存: {media_cleared} 个文件"
        )

    @filter.command("推特列表", alias={"twitter_list"})
    async def list_follows(self, event: AstrMessageEvent):
        """查看当前订阅的推主列表"""
        umo = event.unified_msg_origin
        subs = await self._get_subs()

        lines = []
        for username, info in subs.items():
            subscribers = info.get("subscribers", {})
            if umo in subscribers:
                sub = subscribers[umo]
                status_icon = "🟢" if sub.get("status", True) else "🔴"
                r18_str = " | R18" if sub.get("r18") else ""
                media_str = " | 仅媒体" if sub.get("media") else ""
                screen_name = info.get("screen_name", username)
                lines.append(
                    f"{status_icon} @{username} ({screen_name}){r18_str}{media_str}"
                )

        if not lines:
            yield event.plain_result("当前没有订阅任何推主")
            return

        yield event.plain_result(
            "当前订阅列表:\n"
            + "\n".join(f"{i}. {line}" for i, line in enumerate(lines, 1))
        )

    @filter.command("推特推送", alias={"twitter_push"})
    async def toggle_push(self, event: AstrMessageEvent, action: str = ""):
        """开启/关闭推文推送，格式: /推特推送 开启 或 /推特推送 关闭"""
        if action not in ("开启", "关闭"):
            yield event.plain_result("用法: /推特推送 开启 或 /推特推送 关闭")
            return

        enabled = action == "开启"
        umo = event.unified_msg_origin

        subs = await self._get_subs()
        count = 0
        for username in subs:
            subscribers = subs[username].get("subscribers", {})
            if umo in subscribers:
                subs[username]["subscribers"][umo]["status"] = enabled
                count += 1

        if count > 0:
            await self._save_subs(subs)
            status_text = "开启" if enabled else "关闭"
            yield event.plain_result(f"推文推送已{status_text} (影响 {count} 个订阅)")
        else:
            yield event.plain_result("当前没有订阅任何推主")

    @filter.command("推特测试", alias={"twitter_test"})
    async def test_tweet(self, event: AstrMessageEvent, username: str = ""):
        """立即获取并推送指定推主的最新一条推文，格式: /推特测试 <推主id>

        优先从 Redis 缓存获取，缓存未命中时调用 twitterapi.io。
        """
        if not self.api_key:
            yield event.plain_result(
                "未配置 twitterapi.io API Key，请先在配置中设置"
            )
            return

        if not username:
            yield event.plain_result("请提供推主ID，用法: /推特测试 <推主ID>")
            return

        username = username.strip("@").strip()
        umo = event.unified_msg_origin

        yield event.plain_result(f"正在获取 @{username} 的最新推文，请稍候...")

        tweet_info: dict | None = None

        # 优先从 Redis 缓存获取
        cached = await self._cache_get_tweet(username)
        if cached:
            logger.info(f"命中缓存: {username}")
            tweet_info = cached
        else:
            logger.info(f"缓存未命中: {username}，调用 twitterapi.io")

        # 缓存未命中，从 API 获取最新 1 条推文（仅 1 次 API 调用）
        if not tweet_info:
            new_tweets = await self.twitter_api.get_user_new_tweets_parsed(
                username, include_retweets=self.include_retweets, limit=1
            )
            if not new_tweets:
                yield event.plain_result(f"未找到 @{username} 的推文")
                return

            tweet_info = new_tweets[0]

            # 更新缓存
            await self._cache_set_tweet(username, tweet_info)

        # 翻译推文（如果翻译已缓存在 tweet_info 中则跳过 LLM 调用）
        translated_text, translate_model = await self._maybe_translate(
            tweet_info, umo
        )

        # 将翻译结果持久化到缓存（下次命中时跳过 API + LLM + 媒体下载）
        await self._cache_set_tweet(username, tweet_info)

        # 构建并返回消息
        chain = await self._build_tweet_message_chain(
            username,
            tweet_info,
            translated_text=translated_text,
            translate_model=translate_model,
        )
        if not chain:
            yield event.plain_result(f"未找到 @{username} 的推文内容")
            return

        if self.use_node:
            # 合并转发模式
            author_username = str(tweet_info.get("username") or username)
            screen_name = str(tweet_info.get("screen_name") or author_username)
            nickname = self._build_author_display(author_username, screen_name)
            try:
                nodes, video_parts = self._split_chain_for_nodes(chain, nickname)
                if nodes:
                    yield event.chain_result([Nodes(nodes)])
                else:
                    yield event.plain_result(f"未找到 @{username} 的推文内容")
                # 视频无法通过 yield 发送，作为独立消息发送
                for vid_comp in video_parts:
                    await self._send_video_or_fallback(umo, vid_comp)
            except Exception:
                # 合并转发失败，回退到普通消息链
                plain_chain, video_parts = self._split_plain_chain_and_videos(chain)
                if plain_chain:
                    yield event.chain_result(plain_chain)
                for vid_comp in video_parts:
                    await self._send_video_or_fallback(umo, vid_comp)
        else:
            plain_chain, video_parts = self._split_plain_chain_and_videos(chain)
            if plain_chain:
                yield event.chain_result(plain_chain)
            for vid_comp in video_parts:
                await self._send_video_or_fallback(umo, vid_comp)

    # ========== 链接识别 ==========

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_message(self, event: AstrMessageEvent):
        """监听所有消息，检测 Twitter/X 链接并解析推文"""
        # 全局链接识别开关检查
        if not self.link_recognition_enabled:
            return

        umo = event.unified_msg_origin
        msg_str = event.message_str

        # 检测是否包含 Twitter/X 链接
        match = TWITTER_LINK_PATTERN.search(msg_str)
        if not match:
            return

        link = match.group(1)
        username = match.group(2)
        tweet_id = match.group(3)

        logger.info(f"检测到推文链接: {link}")

        if not self.api_key:
            return

        try:
            tweet_info = await self.twitter_api.get_tweet(username, tweet_id)

            # 翻译推文
            translated_text, translate_model = await self._maybe_translate(
                tweet_info, umo
            )

            # 构建推文消息链
            chain = await self._build_tweet_message_chain(
                username,
                tweet_info,
                {"r18": True, "media": False, "status": True},
                translated_text=translated_text,
                translate_model=translate_model,
            )
            if not chain:
                return

            if self.use_node:
                # 合并转发模式
                author_username = str(tweet_info.get("username") or username)
                screen_name = str(tweet_info.get("screen_name") or author_username)
                nickname = self._build_author_display(author_username, screen_name)
                try:
                    nodes, video_parts = self._split_chain_for_nodes(
                        chain, nickname
                    )
                    if nodes:
                        yield event.chain_result([Nodes(nodes)])
                    # 视频无法通过 yield 发送，作为独立消息发送
                    for vid_comp in video_parts:
                        await self._send_video_or_fallback(umo, vid_comp)
                except Exception:
                    plain_chain, video_parts = self._split_plain_chain_and_videos(
                        chain
                    )
                    if plain_chain:
                        yield event.chain_result(plain_chain)
                    for vid_comp in video_parts:
                        await self._send_video_or_fallback(umo, vid_comp)
            else:
                plain_chain, video_parts = self._split_plain_chain_and_videos(chain)
                if plain_chain:
                    yield event.chain_result(plain_chain)
                for vid_comp in video_parts:
                    await self._send_video_or_fallback(umo, vid_comp)

        except Exception as e:
            logger.error(f"解析推文链接失败: {e}")
