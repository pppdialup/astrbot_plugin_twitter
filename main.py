"""
AstrBot Twitter 推文转发插件
基于 Nitter 镜像站，支持订阅推主、定时推送、链接识别、合并转发消息、推文翻译

指令列表:
  /推特关注 <推主id> [r18] [媒体]          - 订阅推主
  /推特批量关注 <推主id1> <推主id2> ... [r18] [媒体]  - 批量订阅推主
  /推特取关 <推主id>                        - 取关推主
  /推特批量取关 <推主id1> <推主id2> ...     - 批量取关推主
  /推特清空订阅                             - 清空所有订阅（仅管理员）
  /推特列表                                 - 查看当前订阅列表
  /推特推送 开启/关闭                       - 开启/关闭推送
  /推特测试 <推主id>                        - 立即获取并推送指定推主最新一条推文

配置项:
  推文内容翻译开关 (twitter_translate_enabled)   - 开启后推文正文自动翻译
  翻译目标语言 (twitter_translate_target_lang)    - 如：简体中文、日语、英语
  翻译 LLM Provider ID (twitter_translate_provider_id) - 留空则自动选择

当消息中包含 twitter.com 或 x.com 的推文链接时，自动解析并发送推文内容。
"""

import asyncio
import re
from dataclasses import dataclass, field

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Node, Nodes
from astrbot.api.star import Context, Star
import astrbot.api.message_components as Comp

from .twitter_api import TwitterAPI, WEBSITE_LIST, get_next_website

# Twitter/X 链接正则
TWITTER_LINK_PATTERN = re.compile(
    r"(https?://(?:twitter\.com|x\.com)/([a-zA-Z0-9_]+)/status/(\d+))"
)

# KV 存储键名
KV_SUBS_KEY = "twitter_subs"


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

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config

        # 读取配置
        self.proxy = str(config.get("twitter_proxy", "") or "") or None
        self.use_node = bool(config.get("twitter_use_node", True))
        self.no_text = bool(config.get("twitter_no_text", False))
        self.link_recognition_enabled = bool(
            config.get("twitter_link_recognition_enabled", True)
        )
        self.poll_interval = max(1, int(config.get("twitter_poll_interval", 5)))
        self.collective_forward = bool(
            config.get("twitter_collective_forward", False)
        )
        self.collective_max_authors = max(
            1, int(config.get("twitter_collective_max_authors", 5))
        )
        self.translate_enabled = bool(config.get("twitter_translate_enabled", False))
        self.translate_target_lang = str(
            config.get("twitter_translate_target_lang", "简体中文") or "简体中文"
        )
        self.translate_provider_id = str(
            config.get("twitter_translate_provider_id", "") or ""
        ).strip()
        custom_nitter_url = str(config.get("twitter_nitter_url", "") or "").strip()

        # 构建镜像站列表
        self.website_list: list[str] = []
        if custom_nitter_url:
            self.website_list.append(custom_nitter_url)
        self.website_list.extend(WEBSITE_LIST)

        # 初始化 Twitter API
        self.twitter_api = TwitterAPI(proxy=self.proxy, nitter_url="")

        # 定时任务句柄
        self._poll_task: asyncio.Task | None = None
        self._running = False

        # 集体转发推文缓存：{umo: [CachedTweet, ...]}
        self._collected_tweets: dict[str, list[CachedTweet]] = {}

    async def initialize(self):
        """插件初始化"""
        logger.info("Twitter 推文转发插件初始化中...")

        # 集体转发模式与合并转发消息的兼容性校验
        if self.collective_forward and not self.use_node:
            logger.warning(
                "集体转发模式已开启但合并转发消息未开启，集体转发功能不会生效。"
                "请同时开启「使用合并转发消息」配置项。"
            )

        # 检测可用镜像站
        available = await self.twitter_api.check_website_available(self.website_list)
        if available:
            logger.info(f"当前使用 Nitter 镜像站: {available}")
        else:
            logger.warning("未找到可用 Nitter 镜像站，推文轮询功能暂不可用")

        # 启动定时轮询任务
        if self.twitter_api.nitter_url:
            self._running = True
            self._poll_task = asyncio.create_task(self._poll_tweets())
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

        Args:
            username: 推主用户名（如 elonmusk）
            screen_name: 显示昵称（如 Elon Musk）

        Returns:
            格式化后的昵称，如 "@elonmusk (Elon Musk)" 或 "@elonmusk"
        """
        nickname = f"@{username}"
        if screen_name and screen_name != username:
            nickname += f" ({screen_name})"
        return nickname

    async def _maybe_translate(
        self, tweet_info: dict, umo: str
    ) -> tuple[str | None, str | None]:
        """根据翻译配置，翻译推文文本

        Args:
            tweet_info: 推文信息字典
            umo: 会话标识，用于获取 Provider

        Returns:
            (翻译后的文本, 翻译模型名称)；未开启翻译或翻译失败时返回 (None, None)
        """
        if not self.translate_enabled:
            return None, None

        original_text = str(tweet_info.get("text") or "")
        if not original_text.strip():
            return None, None

        translated_text, translate_model = await self._translate_text(
            original_text, umo
        )
        if translate_model:
            return translated_text, translate_model
        return None, None

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
                logger.debug(f"翻译使用配置指定的 Provider: {self.translate_provider_id}")
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

        Args:
            text: 原始文本
            umo: 订阅者的会话标识，用于获取 Provider

        Returns:
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

        max_retries = 2
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

    def _build_tweet_chain(
        self,
        username: str,
        tweet_info: dict,
        sub_config: dict | None = None,
        translated_text: str | None = None,
        translate_model: str | None = None,
    ) -> list:
        """构建推文消息链

        Args:
            translated_text: 翻译后的文本，若提供则替换原文
            translate_model: 执行翻译的模型名称，用于末尾标注
        """
        if sub_config is None:
            sub_config = {"r18": True, "media": False, "status": True}

        text = str(translated_text or tweet_info.get("text") or "")
        images = tweet_info.get("images") or []
        quote = tweet_info.get("quote")
        tweet_id = str(tweet_info.get("tweet_id") or "")
        screen_name = str(tweet_info.get("screen_name") or username)

        chain = []

        # 头部信息
        nickname = self._build_nickname(username, screen_name)
        chain.append(Comp.Plain(str(nickname) + "\n"))

        # 推文正文
        has_media = bool(images) or bool(tweet_info.get("videos"))
        if not (self.no_text and has_media):
            if text:
                chain.append(Comp.Plain(str(text) + "\n"))

        # 引用推文
        if quote:
            quote_author = str(quote.get("author") or "")
            quote_text = str(quote.get("text") or "")
            chain.append(Comp.Plain(str(f"\n引用 @{quote_author}:\n{quote_text}\n")))

        # 图片
        for img_url in images:
            try:
                img_comp = Comp.Image.fromURL(str(img_url))
                if img_comp is not None:
                    chain.append(img_comp)
            except Exception as e:
                logger.warning(f"添加图片失败: {img_url}, {e}")

        # 视频
        videos = tweet_info.get("videos") or []
        for v_url in videos:
            try:
                video_comp = Comp.Video.fromURL(str(v_url))
                if video_comp is not None:
                    chain.append(video_comp)
            except Exception as e:
                logger.warning(f"添加视频失败，回退为链接: {v_url}, {e}")
                chain.append(Comp.Plain(str(f"\n视频: {v_url}")))

        # 推文链接
        if tweet_id:
            link = f"\nhttps://x.com/{username}/status/{tweet_id}"
            chain.append(Comp.Plain(str(link)))

        # 翻译说明标注
        if translate_model and translated_text is not None:
            chain.append(
                Comp.Plain(str(f"\n（由 {translate_model} 翻译自原文）"))
            )

        # 过滤 None 值，防止类型验证错误
        chain = [c for c in chain if c is not None]
        return chain

    def _split_chain_for_nodes(
        self, chain: list, nickname: str
    ) -> tuple[list[Node], list[Comp.Video]]:
        """将消息链分离为 Node 列表和待独立发送的视频列表

        视频不能放在 Node 中，否则下载+上传会超出 WebSocket API 超时时间，
        需要作为独立消息发送。

        Args:
            chain: _build_tweet_chain 生成的消息链
            nickname: Node 显示的昵称

        Returns:
            (Node 列表, 待独立发送的视频组件列表)
        """
        nodes: list[Node] = []
        video_parts: list[Comp.Video] = []
        text_parts: list = []
        image_parts: list[Comp.Image] = []

        for comp in chain:
            if isinstance(comp, Comp.Video):
                video_parts.append(comp)
            elif isinstance(comp, Comp.Image):
                image_parts.append(comp)
            else:
                text_parts.append(comp)

        # 文本节点
        if text_parts:
            nodes.append(Node(content=text_parts, name=nickname))
        # 每张图片一个节点
        for img_comp in image_parts:
            nodes.append(Node(content=[img_comp], name=nickname))

        return nodes, video_parts

    async def _send_video_or_fallback(self, umo: str, vid_comp: Comp.Video):
        """发送视频组件，失败时回退为链接

        Args:
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
        nickname = self._build_nickname(username, screen_name)

        # 翻译推文（如果开启），同一推文只翻译一次
        first_umo = next(iter(subscribers), "")
        translated_text, translate_model = await self._maybe_translate(
            tweet_info, first_umo
        )
        if translate_model:
            original_text = str(tweet_info.get("text") or "")
            logger.info(
                f"推文翻译完成 @{username}: "
                f"模型={translate_model}, "
                f"原文长度={len(original_text)}, "
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
            images = tweet_info.get("images") or []
            if sub_config.get("media", False) and not images:
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
                umo, username, tweet_info, sub_config, nickname,
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
            chain = self._build_tweet_chain(
                username, tweet_info, sub_config,
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
                        message_chain = MessageChain(
                            chain=[Nodes(nodes)]
                        )
                        await self.context.send_message(umo, message_chain)

                    # 视频作为独立消息逐条发送
                    for vid_comp in video_parts:
                        await self._send_video_or_fallback(umo, vid_comp)

                except Exception as node_err:
                    # 合并转发失败，回退到普通消息链（视频改为链接）
                    logger.warning(
                        f"合并转发失败，回退到普通消息: {node_err}"
                    )
                    fallback_chain = []
                    for comp in chain:
                        if isinstance(comp, Comp.Video):
                            vid_url = getattr(comp, "file", "") or getattr(
                                comp, "url", ""
                            )
                            if vid_url:
                                fallback_chain.append(
                                    Comp.Plain(str(f"\n视频: {vid_url}"))
                                )
                        else:
                            fallback_chain.append(comp)
                    if fallback_chain:
                        message_chain = MessageChain(chain=fallback_chain)
                        await self.context.send_message(umo, message_chain)
            else:
                # 纯文本模式：将消息链中非文本组件转为文本描述
                plain_chain = []
                for comp in chain:
                    if isinstance(comp, Comp.Image):
                        # 图片无法在纯文本模式下展示，跳过
                        pass
                    elif isinstance(comp, Comp.Video):
                        vid_url = getattr(comp, "file", "") or getattr(
                            comp, "url", ""
                        )
                        if vid_url:
                            plain_chain.append(
                                Comp.Plain(str(f"\n视频: {vid_url}"))
                            )
                    else:
                        plain_chain.append(comp)
                if plain_chain:
                    message_chain = MessageChain(chain=plain_chain)
                    await self.context.send_message(umo, message_chain)

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
                            chain = self._build_tweet_chain(
                                ct.username, ct.tweet_info, ct.sub_config,
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

    async def _poll_tweets(self):
        """定时轮询推文"""
        while self._running:
            try:
                await self._check_all_subscriptions()
            except Exception as e:
                logger.error(f"推文轮询出错: {e}")
            await asyncio.sleep(self.poll_interval * 60)

    async def _check_all_subscriptions(self):
        """检查所有订阅的新推文"""
        subscribe_list = await self._get_subs()
        if not subscribe_list:
            return

        results: list[bool] = []
        for username, info in subscribe_list.items():
            try:
                result = await self._check_user_tweets(username, info)
                results.append(result)
                await asyncio.sleep(3)  # 避免频繁请求
            except Exception as e:
                logger.error(f"检查 {username} 推文失败: {e}")
                results.append(False)

        # 集体转发模式：轮询结束后统一发送缓存的推文
        if self.collective_forward and self._collected_tweets:
            await self._flush_collected_tweets()

        # 自动切换镜像站
        if not self.config.get("twitter_nitter_url", "") and results:
            success_count = sum(1 for r in results if r)
            if success_count < len(results) / 2 and self.website_list:
                new_url = get_next_website(
                    self.website_list, self.twitter_api.nitter_url
                )
                if new_url and new_url != self.twitter_api.nitter_url:
                    logger.info(f"当前镜像站出错过多，切换至: {new_url}")
                    self.twitter_api.nitter_url = new_url

    async def _check_user_tweets(self, username: str, info: dict) -> bool:
        """检查某个用户的新推文，返回是否成功获取"""
        try:
            since_id = info.get("since_id", "")
            new_tweet_ids = await self.twitter_api.get_user_newtimeline(
                username, since_id
            )

            if not new_tweet_ids:
                return True

            # 再次确认该推主仍有订阅者（可能在获取推文期间被取关）
            latest_subs = await self._get_subs()
            if username not in latest_subs:
                logger.info(f"@{username} 已无订阅者，跳过推送")
                return True

            # 按时间正序（最旧在前）逐条处理
            # （集体转发模式下缓存，即时模式下直接推送）
            for tweet_id in new_tweet_ids:
                tweet_info = await self.twitter_api.get_tweet(username, tweet_id)
                await self._push_tweet_to_subscribers(username, tweet_info, info)

            # 更新 since_id 为最新一条
            subs = await self._get_subs()
            if username in subs:
                subs[username]["since_id"] = new_tweet_ids[-1]
                await self._save_subs(subs)

            return True
        except Exception as e:
            logger.error(f"获取 {username} 推文异常: {e}")
            return False

    # ========== 指令处理 ==========

    @filter.command("推特关注", alias={"twitter_follow"})
    async def follow_twitter(self, event: AstrMessageEvent, username: str = ""):
        """订阅推主，格式: /推特关注 <推主id> [r18] [媒体]"""
        if not self.twitter_api.nitter_url:
            yield event.plain_result("镜像站不可用，请检查配置或网络")
            return

        if not username:
            yield event.plain_result("请提供推主ID，用法: /推特关注 <推主ID> [r18] [媒体]")
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

        # 获取最新推文 ID 作为 since_id
        latest_ids = await self.twitter_api.get_user_newtimeline(username)
        since_id = latest_ids[-1] if latest_ids else ""

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
                "since_id": since_id,
                "subscribers": {},
            }

        subs[username]["subscribers"][umo] = session_config
        subs[username]["screen_name"] = user_info["screen_name"]
        if since_id:
            subs[username]["since_id"] = since_id

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
        if not self.twitter_api.nitter_url:
            yield event.plain_result("镜像站不可用，请检查配置或网络")
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

                # 获取最新推文 ID
                latest_ids = await self.twitter_api.get_user_newtimeline(username)
                since_id = latest_ids[-1] if latest_ids else ""

                # 添加订阅
                session_config = {
                    "status": True,
                    "r18": r18,
                    "media": media_only,
                }

                if username not in subs:
                    subs[username] = {
                        "screen_name": user_info["screen_name"],
                        "since_id": since_id,
                        "subscribers": {},
                    }

                subs[username]["subscribers"][umo] = session_config
                subs[username]["screen_name"] = user_info["screen_name"]
                if since_id:
                    subs[username]["since_id"] = since_id

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

        yield event.plain_result("当前订阅列表:\n" + "\n".join(
            f"{i}. {line}" for i, line in enumerate(lines, 1)
        ))

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
        """立即获取并推送指定推主的最新一条推文，格式: /推特测试 <推主id>"""
        if not self.twitter_api.nitter_url:
            yield event.plain_result("镜像站不可用，请检查配置或网络")
            return

        if not username:
            yield event.plain_result("请提供推主ID，用法: /推特测试 <推主ID>")
            return

        username = username.strip("@").strip()

        umo = event.unified_msg_origin

        yield event.plain_result(f"正在获取 @{username} 的最新推文，请稍候...")

        # 获取最新推文 ID
        latest_ids = await self.twitter_api.get_user_newtimeline(username)
        if not latest_ids:
            yield event.plain_result(f"未找到 @{username} 的推文")
            return

        tweet_id = latest_ids[-1]

        # 获取推文详情
        tweet_info = await self.twitter_api.get_tweet(username, tweet_id)

        # 翻译推文
        translated_text, translate_model = await self._maybe_translate(
            tweet_info, umo
        )

        # 构建并返回消息
        chain = self._build_tweet_chain(
            username, tweet_info,
            translated_text=translated_text,
            translate_model=translate_model,
        )
        if not chain:
            yield event.plain_result(f"未找到 @{username} 的推文内容")
            return

        if self.use_node:
            # 合并转发模式
            screen_name = str(tweet_info.get("screen_name") or username)
            nickname = self._build_nickname(username, screen_name)
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
                yield event.chain_result(chain)
        else:
            yield event.chain_result(chain)

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

        if not self.twitter_api.nitter_url:
            return

        try:
            tweet_info = await self.twitter_api.get_tweet(username, tweet_id)

            # 翻译推文
            translated_text, translate_model = await self._maybe_translate(
                tweet_info, umo
            )

            # 构建推文消息链
            chain = self._build_tweet_chain(
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
                screen_name = str(tweet_info.get("screen_name") or username)
                nickname = self._build_nickname(username, screen_name)
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
                    yield event.chain_result(chain)
            else:
                yield event.chain_result(chain)

        except Exception as e:
            logger.error(f"解析推文链接失败: {e}")
