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
        """啟動 Playwright 瀏覽器"""
        if not self.playwright:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(headless=self.headless)
            self.context = await self.browser.new_context(user_agent=self.user_agent)
            self.page = await self.context.new_page()
            logger.info("Playwright 瀏覽器已啟動")

    async def stop(self):
        """關閉瀏覽器並釋放資源"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("Playwright 瀏覽器已關閉")

    async def navigate_with_retry(self, url: str, retries: int = 3) -> bool:
        """導向 URL 並包含重試機制"""
        for i in range(retries):
            try:
                # 隨機延遲以避免被封鎖
                actual_delay = self.delay * (0.8 + random.random() * 0.4)
                await asyncio.sleep(actual_delay)
                
                logger.info(f"正在導向: {url} (嘗試 {i+1}/{retries})")
                await self.page.goto(url, wait_until="domcontentloaded", timeout=self.timeout)
                return True
            except Exception as e:
                logger.warning(f"導向失敗 {url}: {e}")
                if i == retries - 1:
                    logger.error(f"達到最大重試次數: {url}")
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
