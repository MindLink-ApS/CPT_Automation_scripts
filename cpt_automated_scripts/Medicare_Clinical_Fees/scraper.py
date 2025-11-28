import asyncio
import zipfile
import shutil
from pathlib import Path
from playwright.async_api import async_playwright, Page, Browser
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CLFSDownloader:
    """Handles downloading and extracting CLFS files from CMS website"""
    
    FILE_URL = "https://www.cms.gov/medicare/medicare-fee-service-payment/clinicallabfeesched/clinical-laboratory-fee-schedule-files/25clabq1"
    DOWNLOAD_DIR = Path("./downloads")
    
    def __init__(self):
        self.browser: Browser | None = None
        self.page: Page | None = None
        self.playwright = None
        self.download_dir = self.DOWNLOAD_DIR
        self.download_dir.mkdir(exist_ok=True)
        
    async def initialize_browser(self) -> None:
        """Initialize Playwright browser instance"""
        logger.info("Initializing browser...")
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=['--start-maximized']
        )
        context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080}
        )
        self.page = await context.new_page()
        logger.info("Browser initialized successfully")
        
    async def navigate_to_file_page(self) -> None:
        """Navigate directly to the file detail page"""
        logger.info(f"Navigating to {self.FILE_URL}")
        await self.page.goto(self.FILE_URL, wait_until="networkidle")
        logger.info("File page loaded successfully")
        
    async def download_zip_file(self) -> Path:
        """
        Clicks link, accepts terms on the same page, and downloads the ZIP file.
        
        Returns:
            Path: Path to downloaded ZIP file
        """
        logger.info("Looking for download link in 'Related Links' section...")
        
        await self.page.wait_for_selector(
            '.field--name-field-related-links', 
            timeout=10000
        )
        

        download_link = self.page.locator(
            '.field--name-field-related-links .field__items li a'
        ).first
        

        await download_link.wait_for(state='visible', timeout=5000)
        
        link_text = await download_link.inner_text()
        logger.info(f"Found download link: {link_text}. Clicking it...")
        
    
        await download_link.click()
        logger.info("Page navigating to license agreement...")
        await self.page.wait_for_load_state("networkidle")
        logger.info("License agreement page loaded.")

        # 2. On the same page (now on the license page), expect the download
        async with self.page.expect_download() as download_info:
            logger.info("Locating and clicking 'Accept' button...")
            
            # Use the DOM info to locate the accept button
            accept_button = self.page.locator(
                'input[type="submit"][value="Accept"][name="next"]'
            )
            await accept_button.wait_for(state='visible', timeout=5000)
            
            # Click the "Accept" button
            await accept_button.click() 
            
        download = await download_info.value
        logger.info("Download initiated after accepting terms.")
        
        # 3. Save the file
        zip_filename = download.suggested_filename
        zip_path = self.download_dir / zip_filename
        await download.save_as(zip_path)
        
        logger.info(f"Downloaded: {zip_path}")
        # --- END MODIFICATION ---
        
        return zip_path
    
    def extract_xlsx_from_zip(self, zip_path: Path) -> Path | None:
        """
        Extract XLSX file from ZIP archive to current directory
        
        Args:
            zip_path: Path to the ZIP file
            
        Returns:
            Path: Path to extracted XLSX file, or None if not found
        """
        logger.info(f"Extracting XLSX from {zip_path.name}...")
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # List all files in the ZIP
                file_list = zip_ref.namelist()
                logger.info(f"Files in ZIP: {file_list}")
                
                # Find XLSX file(s)
                xlsx_files = [f for f in file_list if f.lower().endswith(('.xlsx', '.xls'))]
                
                if not xlsx_files:
                    logger.warning("No XLSX files found in ZIP archive")
                    return None
                
                # Extract the first XLSX file to current directory
                xlsx_filename = xlsx_files[0]
                logger.info(f"Extracting {xlsx_filename}...")
                
                # Extract to current directory
                zip_ref.extract(xlsx_filename, '.')
                xlsx_path = Path(xlsx_filename)
                
                logger.info(f"XLSX file extracted to: {xlsx_path.absolute()}")
                return xlsx_path
                
        except zipfile.BadZipFile:
            logger.error(f"Invalid ZIP file: {zip_path}")
            return None
        except Exception as e:
            logger.error(f"Error extracting ZIP: {e}")
            return None
    
    def cleanup_downloads(self) -> None:
        """Remove the downloads directory"""
        if self.download_dir.exists():
            logger.info("Cleaning up downloads directory...")
            shutil.rmtree(self.download_dir)
            logger.info("Cleanup complete")
    
    async def close(self) -> None:
        """Close browser and cleanup"""
        if self.browser:
            await self.browser.close()
            logger.info("Browser closed")
        if self.playwright:
            await self.playwright.stop()
            logger.info("Playwright stopped")
    
    async def run(self) -> Path | None:
        """
        Main execution flow
        
        Returns:
            Path: Path to extracted XLSX file, or None if failed
        """
        try:
            await self.initialize_browser()
            await self.navigate_to_file_page()
            zip_path = await self.download_zip_file()
            xlsx_path = self.extract_xlsx_from_zip(zip_path)
            
            return xlsx_path
            
        except Exception as e:
            logger.error(f"Error during execution: {e}", exc_info=True)
            return None
            
        finally:
            await self.close()
            self.cleanup_downloads()


async def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("CLFS Automation Script Started")
    logger.info("=" * 60)
    
    downloader = CLFSDownloader()
    xlsx_path = await downloader.run()
    
    if xlsx_path and xlsx_path.exists():
        logger.info("=" * 60)
        logger.info(f"SUCCESS! XLSX file saved to: {xlsx_path.absolute()}")
        logger.info("=" * 60)
    else:
        logger.error("=" * 60)
        logger.error("FAILED! Could not download and extract XLSX file")
        logger.error("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

