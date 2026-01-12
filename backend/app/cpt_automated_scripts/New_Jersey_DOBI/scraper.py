from pathlib import Path
from playwright.sync_api import sync_playwright
import logging

logger = logging.getLogger(__name__)

class NJMedicalScraper:
    def __init__(self, download_dir: Path = None):
        self.download_dir = download_dir or Path("downloads")
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.url = "https://www.nj.gov/dobi/pipinfo/aicrapg.htm"
    
    def setup_browser(self):
        """Initialize Playwright browser (headless=False)"""
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.set_default_timeout(60000)
        return playwright, browser, context, page
    
    def download_excel_file(self) -> Path:
        """
        Download the Excel file from NJ medical fee schedule page
        Returns: Path to downloaded file
        """
        logger.info(f" Download directory: {self.download_dir}")
        
        playwright, browser, context, page = self.setup_browser()
        
        try:
            logger.info(f" Navigating to: {self.url}")
            page.goto(self.url, wait_until="networkidle")
            
            # Locate the MS Excel link
            logger.info("🔍 Searching for MS Excel link...")
            excel_link = page.locator("a[href$='ex1_130104.xls']")
            excel_link.wait_for(state="visible", timeout=15000)
            
            logger.info(" Found MS Excel link. Starting download...")
            
            # Download the file
            with page.expect_download(timeout=60000) as download_info:
                excel_link.click()
            
            download = download_info.value
            filename = download.suggested_filename
            file_path = self.download_dir / filename
            download.save_as(file_path)
            
            logger.info(f" Download complete: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f" ERROR during download: {str(e)}")
            raise
        finally:
            context.close()
            browser.close()
            playwright.stop()