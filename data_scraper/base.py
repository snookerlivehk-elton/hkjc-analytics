import time
import random
import asyncio
from typing import Optional
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
from utils.logger import logger
from utils.config import config

class BaseScraper:
    """基礎爬蟲類，提供瀏覽器管理、限速與錯誤處理"""
    
    def __init__(self):
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        
        # 設定參數
        self.headless = config.get("scraping.headless", True)
        self.user_agent = config.get("scraping.user_agent")
        self.timeout = config.get("scraping.timeout", 30000)
        self.delay = config.get("scraping.rate_limit_delay", 2.0)

    async def start(self):
        """啟動 Playwright 瀏覽器 (強化版)"""
        if not self.playwright:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=self.headless,
                args=["--disable-blink-features=AutomationControlled"] # 隱藏自動化特徵
            )
            self.context = await self.browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080}
            )
            self.page = await self.context.new_page()
            logger.info("Playwright 瀏覽器已啟動 (隱身模式)")

    async def stop(self):
        """關閉瀏覽器並釋放資源"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("Playwright 瀏覽器已關閉")

    async def navigate_with_retry(self, url: str, retries: int = 3) -> bool:
        """導向 URL 並包含重試機制 (務實版)"""
        for i in range(retries):
            try:
                actual_delay = self.delay * (0.8 + random.random() * 0.4)
                await asyncio.sleep(actual_delay)
                
                logger.info(f"正在導向: {url} (嘗試 {i+1}/{retries})")
                # 使用 domcontentloaded 以快速進入，後續再等待特定元素
                await self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
                return True
            except Exception as e:
                logger.warning(f"導向失敗 {url}: {e}")
                if i == retries - 1:
                    return False
        return False

    async def get_content(self) -> str:
        """獲取目前頁面的 HTML 內容"""
        return await self.page.content()

    def parse_float(self, val: str) -> float:
        """工具函數：解析浮點數，失敗回傳 0.0"""
        try:
            return float(val.replace(',', '').strip())
        except (ValueError, AttributeError):
            return 0.0

    def parse_int(self, val: str) -> int:
        """工具函數：解析整數，失敗回傳 0"""
        try:
            return int(val.replace(',', '').strip())
        except (ValueError, AttributeError):
            return 0
