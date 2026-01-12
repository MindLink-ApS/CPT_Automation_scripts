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
    
    # NOTE: If this URL returns 404, the CMS website structure has changed.
    # To find the correct URL:
    # 1. Go to https://www.cms.gov
    # 2. Search for "Clinical Laboratory Fee Schedule" or "CLFS"
    # 3. Navigate to the files/downloads section
    # 4. Find the latest quarterly file (e.g., "25clabq1" for 2025 Q1)
    # 5. Update FILE_URL with the correct path
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
        
        # Check if we got a 404 or Page Not Found error
        page_title = await self.page.title()
        page_content = await self.page.content()
        
        # Check for common error indicators
        if "page not found" in page_content.lower() or "404" in page_content.lower() or "error" in page_title.lower():
            logger.error("❌ Page Not Found (404) - The URL may have changed!")
            logger.error(f"   Page title: {page_title}")
            
            # Take screenshot for debugging
            try:
                error_screenshot = self.download_dir / "404_error_screenshot.png"
                await self.page.screenshot(path=error_screenshot, full_page=True)
                logger.error(f"📸 404 error screenshot saved: {error_screenshot}")
            except:
                pass
            
            # Try alternative URLs
            alternative_urls = [
                "https://www.cms.gov/medicare/medicare-fee-service-payment/clinicallabfeesched/clinical-laboratory-fee-schedule-files",
                "https://www.cms.gov/medicare/payment/clinical-diagnostic-laboratory-fees/clinical-laboratory-fee-schedule-files",
                "https://www.cms.gov/medicare/medicare-fee-service-payment/clinicallabfeesched",
            ]
            
            logger.warning("⚠️ Trying alternative URLs...")
            for alt_url in alternative_urls:
                try:
                    logger.info(f"   Trying: {alt_url}")
                    await self.page.goto(alt_url, wait_until="networkidle", timeout=30000)
                    await asyncio.sleep(2)
                    
                    # Check if this page is valid
                    new_content = await self.page.content()
                    if "page not found" not in new_content.lower() and "404" not in new_content.lower():
                        logger.info(f"✅ Found valid page at: {alt_url}")
                        self.FILE_URL = alt_url  # Update the URL for future reference
                        return
                except Exception as e:
                    logger.warning(f"   Failed: {e}")
                    continue
            
            raise Exception(f"Page Not Found at {self.FILE_URL}. The CMS website structure may have changed. Please check the CMS website manually for the correct CLFS file URL.")
        
        logger.info("File page loaded successfully")
        
    async def download_zip_file(self) -> Path:
        """
        Clicks link, accepts terms on the same page, and downloads the ZIP file.
        
        Returns:
            Path: Path to downloaded ZIP file
        """
        logger.info("Looking for download link...")
        
        # Wait for page to fully load
        await self.page.wait_for_load_state("networkidle")
        await asyncio.sleep(2)  # Additional wait for dynamic content
        
        # Take screenshot for debugging
        try:
            screenshot_path = self.download_dir / "page_screenshot.png"
            await self.page.screenshot(path=screenshot_path)
            logger.info(f"📸 Screenshot saved: {screenshot_path}")
        except:
            pass
        
        # Try multiple selectors to find download link
        download_link = None
        link_text = None
        
        # Strategy 1: Look for Related Links section
        try:
            logger.info("  ↳ Trying selector: .field--name-field-related-links")
            await self.page.wait_for_selector(
                '.field--name-field-related-links', 
                timeout=15000,
                state='visible'
            )
            download_link = self.page.locator(
                '.field--name-field-related-links .field__items li a'
            ).first
            await download_link.wait_for(state='visible', timeout=5000)
            link_text = await download_link.inner_text()
            logger.info(f"✅ Found download link (method 1): {link_text}")
        except Exception as e:
            logger.warning(f"  ↳ Method 1 failed: {e}")
        
        # Strategy 2: Look for any link containing "download" or "zip" or "CLFS"
        if not download_link or not link_text:
            try:
                logger.info("  ↳ Trying alternative: links with 'download', 'zip', or 'CLFS'")
                # Try to find links with download-related text
                all_links = self.page.locator('a')
                link_count = await all_links.count()
                logger.info(f"  ↳ Found {link_count} links on page")
                
                # Log all links for debugging
                logger.info("  ↳ Scanning all links on page...")
                all_link_info = []
                for i in range(min(link_count, 100)):  # Check first 100 links
                    link = all_links.nth(i)
                    try:
                        text = await link.inner_text()
                        href = await link.get_attribute('href')
                        text_lower = (text or "").strip().lower()
                        href_lower = (href or "").lower()
                        
                        # Log all links for debugging
                        if text or href:
                            all_link_info.append(f"Link {i}: text='{text[:50]}' href='{href[:100] if href else 'None'}'")
                        
                        # Look for download indicators - be more flexible
                        keywords = ['download', 'zip', 'clfs', 'clinical', 'laboratory', 'fee', 'schedule', 'file', 'data']
                        if any(keyword in text_lower or keyword in href_lower for keyword in keywords):
                            # Prioritize links with zip or download
                            if 'zip' in href_lower or 'download' in text_lower or 'download' in href_lower:
                                download_link = link
                                link_text = text or href or "Download link"
                                logger.info(f"✅ Found download link (method 2): {link_text} (href: {href})")
                                break
                            # Also check for CLFS-related links
                            elif 'clfs' in text_lower or 'clfs' in href_lower:
                                download_link = link
                                link_text = text or href or "CLFS link"
                                logger.info(f"✅ Found CLFS link (method 2): {link_text} (href: {href})")
                                break
                    except Exception as link_error:
                        continue
                
                # If we found links but none matched, log them
                if not download_link and all_link_info:
                    logger.warning(f"  ↳ All {len(all_link_info)} links found (showing first 10):")
                    for link_info in all_link_info[:10]:
                        logger.warning(f"     {link_info}")
                        
            except Exception as e:
                logger.warning(f"  ↳ Method 2 failed: {e}")
        
        # Strategy 3: Look for direct download buttons or file links
        if not download_link or not link_text:
            try:
                logger.info("  ↳ Trying alternative: direct file links")
                # Look for links that end with .zip or contain .zip
                zip_links = self.page.locator('a[href$=".zip"], a[href*=".zip"], a[href*="/download"], a[href*="/file"]')
                count = await zip_links.count()
                logger.info(f"  ↳ Found {count} potential file/download links")
                if count > 0:
                    download_link = zip_links.first
                    link_text = await download_link.inner_text()
                    href = await download_link.get_attribute('href')
                    logger.info(f"✅ Found file link (method 3): {link_text} (href: {href})")
            except Exception as e:
                logger.warning(f"  ↳ Method 3 failed: {e}")
        
        # Strategy 4: Look for buttons or any clickable element with download-related text
        if not download_link or not link_text:
            try:
                logger.info("  ↳ Trying alternative: buttons and clickable elements")
                # Look for buttons or divs with download text
                download_elements = self.page.locator(
                    'button:has-text("Download"), button:has-text("download"), '
                    'a:has-text("Download"), a:has-text("download"), '
                    '[role="button"]:has-text("Download"), [role="button"]:has-text("download")'
                )
                count = await download_elements.count()
                logger.info(f"  ↳ Found {count} download buttons/elements")
                if count > 0:
                    download_link = download_elements.first
                    link_text = await download_link.inner_text()
                    logger.info(f"✅ Found download button (method 4): {link_text}")
            except Exception as e:
                logger.warning(f"  ↳ Method 4 failed: {e}")
        
        # Strategy 5: Look for any link in common CMS sections
        if not download_link or not link_text:
            try:
                logger.info("  ↳ Trying alternative: CMS-specific sections")
                # Try common CMS Drupal sections
                sections = [
                    '.field--name-field-related-links',
                    '.field--name-field-downloads',
                    '.field--name-field-files',
                    '[class*="download"]',
                    '[class*="file"]',
                    '[class*="related"]',
                    'section[class*="download"]',
                    'div[class*="download"]'
                ]
                
                for selector in sections:
                    try:
                        section = self.page.locator(selector)
                        count = await section.count()
                        if count > 0:
                            logger.info(f"  ↳ Found section with selector: {selector}")
                            links_in_section = section.locator('a')
                            link_count = await links_in_section.count()
                            if link_count > 0:
                                # Take the first link in this section
                                download_link = links_in_section.first
                                link_text = await download_link.inner_text()
                                href = await download_link.get_attribute('href')
                                logger.info(f"✅ Found link in section {selector} (method 5): {link_text} (href: {href})")
                                break
                    except:
                        continue
            except Exception as e:
                logger.warning(f"  ↳ Method 5 failed: {e}")
        
        # Strategy 6: Last resort - try any link that's visible and might be relevant
        if not download_link or not link_text:
            try:
                logger.info("  ↳ Trying last resort: any visible link on page")
                all_links = self.page.locator('a[href]:visible')
                link_count = await all_links.count()
                logger.info(f"  ↳ Found {link_count} visible links")
                
                # Try to find any link that's not a navigation link
                for i in range(min(link_count, 20)):  # Check first 20 visible links
                    link = all_links.nth(i)
                    try:
                        text = await link.inner_text()
                        href = await link.get_attribute('href')
                        
                        # Skip navigation links
                        if href and any(skip in href.lower() for skip in ['#', 'javascript:', 'mailto:', '/node/', '/user/']):
                            continue
                        
                        # Skip if it's clearly a navigation link
                        if text and any(nav in text.lower() for nav in ['home', 'about', 'contact', 'search', 'menu']):
                            continue
                        
                        # If we have a link with href, try it
                        if href and href.strip():
                            download_link = link
                            link_text = text or href or "Link"
                            logger.warning(f"⚠️ Using fallback link (method 6): {link_text} (href: {href})")
                            logger.warning(f"   This might not be the correct download link. Please verify.")
                            break
                    except:
                        continue
            except Exception as e:
                logger.warning(f"  ↳ Method 6 failed: {e}")
        
        # If still no link found, raise error
        if not download_link:
            # Take final screenshot for debugging
            try:
                error_screenshot = self.download_dir / "error_screenshot.png"
                await self.page.screenshot(path=error_screenshot, full_page=True)
                logger.error(f"📸 Error screenshot saved: {error_screenshot}")
            except:
                pass
            
            # Log all links found for debugging
            try:
                all_links = self.page.locator('a')
                link_count = await all_links.count()
                logger.error(f"📋 Total links on page: {link_count}")
                logger.error("📋 First 10 links found:")
                for i in range(min(link_count, 10)):
                    try:
                        link = all_links.nth(i)
                        text = await link.inner_text()
                        href = await link.get_attribute('href')
                        logger.error(f"   Link {i}: text='{text[:60]}' href='{href[:100] if href else 'None'}'")
                    except:
                        pass
            except:
                pass
            
            # Log page content for debugging
            try:
                page_content = await self.page.content()
                logger.error(f"Page HTML (first 2000 chars): {page_content[:2000]}")
            except:
                pass
            
            raise Exception("Could not find download link on page. Page structure may have changed. Check screenshots and logs for details.")
        
        # Click the download link
        logger.info(f"Clicking download link: {link_text}")
        await download_link.click()
        logger.info("Page navigating to license agreement...")
        await self.page.wait_for_load_state("networkidle")
        await asyncio.sleep(2)  # Wait for license page to load
        logger.info("License agreement page loaded.")

        # 2. On the same page (now on the license page), expect the download
        logger.info("Locating and clicking 'Accept' button...")
        
        # Try multiple selectors for accept button
        accept_button = None
        
        # Strategy 1: Original selector
        try:
            accept_button = self.page.locator(
                'input[type="submit"][value="Accept"][name="next"]'
            )
            await accept_button.wait_for(state='visible', timeout=10000)
            logger.info("✅ Found Accept button (method 1)")
        except Exception as e:
            logger.warning(f"  ↳ Method 1 failed: {e}")
        
        # Strategy 2: Alternative selectors
        if not accept_button:
            try:
                # Try button with "Accept" text
                accept_button = self.page.locator(
                    'button:has-text("Accept"), input[value*="Accept" i], input[value*="I Accept" i]'
                ).first
                await accept_button.wait_for(state='visible', timeout=10000)
                logger.info("✅ Found Accept button (method 2)")
            except Exception as e:
                logger.warning(f"  ↳ Method 2 failed: {e}")
        
        # Strategy 3: Look for any submit button
        if not accept_button:
            try:
                submit_buttons = self.page.locator('input[type="submit"], button[type="submit"]')
                count = await submit_buttons.count()
                logger.info(f"  ↳ Found {count} submit buttons, trying first one")
                if count > 0:
                    accept_button = submit_buttons.first
                    await accept_button.wait_for(state='visible', timeout=5000)
                    button_text = await accept_button.get_attribute('value') or await accept_button.inner_text()
                    logger.info(f"✅ Found submit button: {button_text}")
            except Exception as e:
                logger.warning(f"  ↳ Method 3 failed: {e}")
        
        if not accept_button:
            # Take screenshot before error
            try:
                error_screenshot = self.download_dir / "accept_button_error.png"
                await self.page.screenshot(path=error_screenshot, full_page=True)
                logger.error(f"📸 Error screenshot saved: {error_screenshot}")
            except:
                pass
            raise Exception("Could not find Accept button on license page")
        
        # Click the accept button and wait for download
        async with self.page.expect_download(timeout=60000) as download_info:
            await accept_button.click()
            logger.info("✅ Clicked Accept button") 
            
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

