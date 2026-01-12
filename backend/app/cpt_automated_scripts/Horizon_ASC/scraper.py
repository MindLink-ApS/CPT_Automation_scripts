import logging
from pathlib import Path
from playwright.sync_api import sync_playwright, Page
import time
from typing import Optional

logger = logging.getLogger(__name__)

class HorizonASCScraper:
    """Scraper for Horizon Blue Cross/Blue Shield ASC fee schedule"""

    BASE_URL = "https://www.horizonblue.com/providers/resources/fee-schedules"
    SOURCE_ID = "Horizon_ASC"

    def __init__(self, download_dir: Path = Path.cwd() / "downloads_horizon_asc"):
        self.download_dir = download_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Download directory set to: {self.download_dir}")

    def download_file(self, headless: bool = True) -> Optional[Path]:
        """
        Navigate to Horizon BCBS fee schedule page and download ASC fee schedule Excel file.
        Returns path to downloaded file.
        """
        logger.info("=" * 50)
        logger.info("HORIZON ASC FEE SCHEDULE DOWNLOAD")
        logger.info("=" * 50)
        logger.info(f"Target URL: {self.BASE_URL}")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(
                accept_downloads=True,
                viewport={"width": 1920, "height": 1080}
            )
            page = context.new_page()

            try:
                # Navigate to fee schedule page
                logger.info("Navigating to Horizon BCBS fee schedule page...")
                page.goto(self.BASE_URL, wait_until="networkidle", timeout=60000)
                logger.info("Page loaded successfully")

                # Wait for page to fully load
                page.wait_for_load_state("domcontentloaded")
                time.sleep(2)

                # Look for ASC fee schedule download link
                # Common patterns: "ASC", "Ambulatory Surgical Center", "Fee Schedule"
                logger.info("Searching for ASC fee schedule download link...")
                
                # Try multiple selectors for the download link
                download_link = None
                selectors = [
                    'a[href*="ASC"]',
                    'a[href*="asc"]',
                    'a:has-text("ASC")',
                    'a:has-text("Ambulatory Surgical Center")',
                    'a[href*="fee-schedule"]',
                    'a[href*="Fee Schedule"]',
                    'a[href$=".xlsx"]',
                    'a[href$=".xls"]',
                ]

                for selector in selectors:
                    try:
                        links = page.locator(selector).all()
                        for link in links:
                            link_text = link.inner_text().lower()
                            href = link.get_attribute('href') or ''
                            if 'asc' in link_text or 'asc' in href.lower():
                                download_link = link
                                logger.info(f"Found ASC link with selector: {selector}")
                                logger.info(f"Link text: {link.inner_text()}")
                                logger.info(f"Link href: {href}")
                                break
                        if download_link:
                            break
                    except Exception as e:
                        logger.debug(f"Selector {selector} failed: {e}")
                        continue

                if not download_link:
                    # Fallback: look for any Excel file link
                    logger.warning("ASC-specific link not found, searching for any Excel file...")
                    excel_links = page.locator('a[href$=".xlsx"], a[href$=".xls"]').all()
                    if excel_links:
                        download_link = excel_links[0]
                        logger.info(f"Using first Excel file found: {download_link.inner_text()}")

                if not download_link:
                    raise Exception("Could not find ASC fee schedule download link on page")

                # Click the download link
                logger.info("Clicking download link...")
                with page.expect_download(timeout=60000) as download_info:
                    download_link.click()

                download = download_info.value
                filename = download.suggested_filename or "horizon_asc_fee_schedule.xlsx"
                file_path = self.download_dir / filename

                logger.info(f"Downloading: {filename}")
                download.save_as(file_path)
                logger.info(f"✅ Download complete: {file_path}")

                return file_path

            except Exception as e:
                logger.error(f"❌ Error during download: {str(e)}")
                # Take screenshot for debugging
                screenshot_path = self.download_dir / "error_screenshot.png"
                page.screenshot(path=str(screenshot_path))
                logger.info(f"Screenshot saved to: {screenshot_path}")
                raise

            finally:
                browser.close()
