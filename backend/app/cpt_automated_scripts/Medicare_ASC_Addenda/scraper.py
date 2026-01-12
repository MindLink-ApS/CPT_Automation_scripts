import logging
from pathlib import Path
from playwright.sync_api import sync_playwright, Page
import zipfile
import time
import shutil
from typing import List

logger = logging.getLogger(__name__)

class ASCScraper:
    """Scrapes the CMS website for the latest ASC Payment Rates Addenda."""

    BASE_URL = "https://www.cms.gov"
    ASC_PAGE_URL = f"{BASE_URL}/medicare/payment/prospective-payment-systems/ambulatory-surgical-center-asc/asc-payment-rates-addenda"

    def __init__(self, download_dir: Path = Path.cwd() / "downloads"):
        self.download_dir = download_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f" Download directory set to: {self.download_dir}")

    def _find_and_download_latest_file(self, page: Page) -> Path:
        """
        Finds the latest file by expanding the first accordion,
        clicking the first link, accepting the terms, and downloading.
        """
        logger.info(" Searching for latest file link in accordion...")
        
        # Find the first accordion block (which is the latest year)
        first_accordion = page.locator(".cms-accordion--block").first
        
        # Click the button to expand it
        logger.info("Expanding first accordion (latest year)...")
        first_accordion.locator("button.ds-u-display--flex").click()
        
        # Wait for the content area to become visible
        content_area = first_accordion.locator(".accordion--content")
        content_area.wait_for(state="visible", timeout=10000)
        
        # Find the very first link in that content area
        target_link = content_area.locator("ul li a").first
        link_text = target_link.inner_text()
        
        logger.info(f" Found target link: {link_text}")
        logger.info("Navigating to license page by clicking link...")
        
        # Click the link, which navigates the current page
        target_link.click()
        page.wait_for_load_state("domcontentloaded", timeout=60000)
        logger.info("License page loaded.")
        
        # Now, handle the "Accept" button and download
        try:
            accept_button = page.locator('input[type="submit"][value="Accept"]')
            accept_button.wait_for(state="visible", timeout=10000)
            logger.info("Accept button found!")

            with page.expect_download(timeout=60000) as download_info:
                accept_button.click()

            download = download_info.value
            filename = download.suggested_filename
            file_path = self.download_dir / filename

            logger.info(f" Downloading: {filename}")
            download.save_as(file_path)
            logger.info(f" Download complete: {file_path}")
            return file_path

        except Exception as e:
            logger.error(f"ERROR during download: {str(e)}")
            raise

    def _extract_xlsx_from_zip(self, zip_path: Path, keyword="Addenda") -> List[Path]:
        """
        Extracts XLSX files matching the keyword into the download directory.
        """
        logger.info("\n Extracting ZIP file...")
        extracted_paths = []
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                all_files = zip_ref.namelist()
                logger.info(f" Files in ZIP: {len(all_files)}")

                xlsx_files = []
                for file_name in all_files:
                    if file_name.lower().endswith(".xlsx") and keyword.lower() in file_name.lower():
                        logger.info(f"Found matching XLSX: {file_name}")
                        xlsx_files.append(file_name)
                        
                        # Extract to the download_dir, not cwd
                        extracted_path = self.download_dir / Path(file_name).name
                        with zip_ref.open(file_name) as src, open(extracted_path, "wb") as dst:
                            shutil.copyfileobj(src, dst)
                        logger.info(f" Saved to: {extracted_path}")
                        extracted_paths.append(extracted_path)

                if not xlsx_files:
                    logger.warning(f" No XLSX files found matching keyword: '{keyword}'")

                return extracted_paths

        except Exception as e:
            logger.error(f" Error extracting ZIP: {str(e)}")
            raise

    def download_and_extract_file(self) -> List[Path]:
        """
        Main method to run the scraping pipeline.
        Returns a list of Paths to the extracted data files.
        """
        with sync_playwright() as p:
            # Running headless=False as in your script
            browser = p.chromium.launch(headless=True) 
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()

            try:
                logger.info(" Loading ASC Payment Rates page...")
                page.goto(self.ASC_PAGE_URL, wait_until="domcontentloaded", timeout=60000)
                time.sleep(2) 
                logger.info(" Page loaded successfully!")
                
        
                zip_file_path = self._find_and_download_latest_file(page)
                
                extracted_file_paths = self._extract_xlsx_from_zip(zip_file_path, keyword="Addenda")
                
                return extracted_file_paths

            except Exception as e:
                logger.error(f" Scraping failed: {e}")
                raise
            finally:
                context.close()
                browser.close()


