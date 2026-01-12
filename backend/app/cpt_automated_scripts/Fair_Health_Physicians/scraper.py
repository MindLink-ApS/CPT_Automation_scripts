import logging
from pathlib import Path
from playwright.sync_api import sync_playwright, Page
import time
import sys

logger = logging.getLogger(__name__)

class FairHealthPhysicianScraper:
    """
    Scrapes Fair Health website for Physician (Charge Medical) data
    by logging in, selecting filters, and downloading CSV data.
    """

    def __init__(self, 
                 fairhealth_url: str,
                 email: str,
                 password: str,
                 proxy_server: str = None,
                 proxy_username: str = None,
                 proxy_password: str = None,
                 download_dir: Path = Path.cwd() / "downloads_physicians"):
        
        self.fairhealth_url = fairhealth_url
        self.email = email
        self.password = password
        self.proxy_server = proxy_server
        self.proxy_username = proxy_username
        self.proxy_password = proxy_password
        self.download_dir = download_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Download directory set to: {self.download_dir}")

    def _setup_browser(self, playwright, headless=False):
        """Start Playwright and launch Chromium with proxy configuration."""
        logger.info(f"🚀 Launching browser (Headless: {headless})")
        launch_args = {"headless": headless}

        if self.proxy_server:
            launch_args["proxy"] = {"server": self.proxy_server}
            if self.proxy_username and self.proxy_password:
                launch_args["proxy"]["username"] = self.proxy_username
                launch_args["proxy"]["password"] = self.proxy_password
            logger.info(f"🔌 Using proxy: {self.proxy_server}")
        
        browser = playwright.chromium.launch(**launch_args)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.set_default_timeout(60000)
        return browser, context, page

    def _safe_goto(self, page, url, timeout=60000, attempts=2):
        """Navigate with graceful retry using DOMContentLoaded."""
        last_exc = None
        for i in range(attempts):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                time.sleep(2)
                return
            except Exception as e:
                logger.warning(f"⚠️ Navigation attempt {i+1} failed: {e}")
                last_exc = e
                time.sleep(3)
        raise last_exc

    def _login_to_fairhealth(self, page):
        """Handle login flow."""
        logger.info("🔐 Starting login process...")
        try:
            email_input = page.locator('input#Email')
            email_input.wait_for(state="visible", timeout=20000)
            email_input.fill(self.email)
            logger.info(f"✅ Filled email")
            time.sleep(0.5)
            
            password_input = page.locator('input#Password')
            password_input.wait_for(state="visible", timeout=20000)
            password_input.fill(self.password)
            logger.info("✅ Filled password")
            time.sleep(0.5)
            
            signin_button = page.locator('button#signin')
            signin_button.wait_for(state="visible", timeout=20000)
            signin_button.click()
            logger.info("✅ Clicked Sign In button")
            
            time.sleep(5)
            
            product_category_label = page.locator('label:has-text("Product Category")')
            product_category_label.wait_for(state="visible", timeout=30000)
            logger.info("✅ Login successful - Dashboard loaded")
            
        except Exception as e:
            logger.error(f"❌ Login failed: {e}")
            raise

    def _select_react_dropdown(self, page, container_id, option_text, dropdown_type="product"):
        """Handle React-Select dropdown selection."""
        logger.info(f"📽 Selecting '{option_text}' from {dropdown_type} dropdown...")
        try:
            dropdown_control = page.locator(f'#{container_id} .select__control')
            dropdown_control.wait_for(state="visible", timeout=20000)
            dropdown_control.click()
            logger.info(f"  ↳ Opened {dropdown_type} dropdown")
            time.sleep(1)
            
            option = page.locator(f'.select__option:has-text("{option_text}")')
            option.wait_for(state="visible", timeout=20000)
            option.click()
            logger.info(f"✅ Selected '{option_text}' from {dropdown_type}")
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"❌ Failed to select from {dropdown_type} dropdown: {e}")
            raise
  
    def _select_release_date(self, page):
        """Select the latest release date from the dropdown."""
        logger.info("📽 Selecting latest Release Date...")
        try:
            # Wait a bit longer after Product selection for Release dropdown to appear
            time.sleep(2)
            
            # Use the same robust selector as Facility scraper - find by label text
            release_container = page.locator('div:has(> label:has-text("Release"))').first
            
            # Wait for the container to be visible
            release_container.wait_for(state="visible", timeout=30000)
            logger.info("  ↳ Found Release Date container")
            
            # Find the dropdown control within the container
            dropdown_control = release_container.locator('.select__control')
            dropdown_control.wait_for(state="visible", timeout=30000)
            dropdown_control.click()
            logger.info("  ↳ Opened Release Date dropdown")
            time.sleep(2)  # Wait for options to load
            
            # Get all options - select FIRST option (most recent), not last
            # The Facility scraper selects first option which is the most recent
            options = page.locator('.select__option')
            options.first.wait_for(state="visible", timeout=20000)
            count = options.count()
            
            if count == 0:
                # Try alternative selector
                logger.warning("  ↳ No options found with .select__option, trying alternative...")
                options = page.locator('[id*="react-select"][role="option"]')
                count = options.count()
            
            if count == 0:
                raise Exception("No options found in Release Date dropdown")
            
            # Select the FIRST option (most recent release date), skip "All" if it exists
            # Filter out "All" option and select the first actual date
            selected_option = None
            selected_text = None
            
            for i in range(count):
                option = options.nth(i)
                option_text = option.inner_text().strip()
                # Skip "All" option, select first actual date
                if option_text.lower() != "all" and option_text:
                    selected_option = option
                    selected_text = option_text
                    break
            
            # If no date found (only "All"), use first option anyway
            if selected_option is None:
                selected_option = options.first
                selected_text = selected_option.inner_text()
                logger.warning(f"  ↳ Only 'All' option found, selecting first option: '{selected_text}'")
            
            selected_option.click()
            logger.info(f"✅ Selected Release Date: '{selected_text}'")
            time.sleep(2)  # Wait for selection to register
            
        except Exception as e:
            logger.error(f"❌ Failed to select Release Date: {e}")
            # Take screenshot for debugging
            try:
                screenshot_path = self.download_dir / "release_date_error.png"
                page.screenshot(path=screenshot_path)
                logger.info(f"📸 Screenshot saved: {screenshot_path}")
            except:
                pass
            raise
 

    def _enter_geozips(self, page, geozips):
        """Enter geozip values directly into the input field."""
        geozip_string = ",".join(geozips)
        logger.info(f"📽 Entering Geozips: {geozip_string}...")
        try:
            geozip_input = page.locator('input#geozip')
            geozip_input.wait_for(state="visible", timeout=20000)
            
            # Clear and fill
            geozip_input.click()
            page.keyboard.press('Control+A')
            page.keyboard.press('Backspace')
            time.sleep(0.5)
            
            geozip_input.fill(geozip_string)
            logger.info(f"✅ Entered Geozips: {geozip_string}")
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"❌ Failed to enter Geozips: {e}")
            try:
                page.screenshot(path=self.download_dir / "geozip_error.png")
            except: pass
            raise

    def _click_search_and_confirm(self, page):
        """Click the Search button and handle the confirmation popup."""
        logger.info("🔍 Clicking Search button...")
        try:
            search_button = page.locator('button#product-search-button')
            search_button.wait_for(state="visible", timeout=20000)
            search_button.click()
            logger.info("✅ Clicked Search button")
            time.sleep(2)
            
            logger.info("  ↳ Handling confirmation popup...")
            popup = page.locator('.react-confirm-alert-body')
            popup.wait_for(state="visible", timeout=20000)
            
            yes_button = popup.locator('button:has-text("Yes")')
            yes_button.wait_for(state="visible", timeout=10000)
            yes_button.click()
            logger.info("✅ Clicked 'Yes' on confirmation popup")
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"❌ Failed during search/confirmation: {e}")
            raise

    def _wait_for_data_to_load(self, page):
        """Wait for the data grid to be populated with results."""
        logger.info("⏳ Waiting for data to load...")
        try:
            # Wait for grid to appear
            grid = page.locator('div#productsGrid')
            grid.wait_for(state="visible", timeout=60000)
            logger.info("  ↳ Grid is visible")
            time.sleep(5)  # Initial wait for data to start loading
            
            rows = page.locator('.ag-row')
            
            # Wait for at least some rows to appear (data is loading)
            for i in range(30):  # Check for 30 seconds
                count = rows.count()
                if count > 0:
                    logger.info(f"✅ Data loaded - Found {count} rows")
                    # Also wait for export button to be ready
                    try:
                        export_btn = page.locator('div.search-header-content a:has(label:has-text("Export as Excel"))')
                        export_btn.wait_for(state="visible", timeout=5000)
                        logger.info("  ↳ Export button is ready")
                    except:
                        logger.warning("  ↳ Export button not yet ready")
                    break
                time.sleep(1)
                if i % 5 == 0 and i > 0:
                    logger.info(f"  ↳ Still waiting... ({i}s)")
            else:
                logger.warning("⚠️ Timeout waiting for data rows, proceeding anyway...")
            
            # Final wait for data to fully render
            time.sleep(3)
            
        except Exception as e:
            logger.warning(f"⚠️ Warning during data load wait: {e}")
            time.sleep(5)

    def _export_to_csv(self, page, batch_name: str = "") -> Path:
        """Click the Export as Excel button and save the downloaded file."""
        logger.info("📥 Exporting data to CSV...")
        try:
            # Wait for any loading overlays to disappear
            logger.info("  ↳ Waiting for overlays to clear...")
            try:
                # Wait for loading spinner/overlay to disappear
                overlay = page.locator('.search-header-right-panel, .loading-overlay, [class*="loading"]')
                overlay.wait_for(state="hidden", timeout=10000)
            except:
                # Overlay might not exist, that's okay
                pass
            
            time.sleep(2)  # Additional wait for UI to stabilize
            
            # Find export button
            export_button = page.locator('div.search-header-content a:has(label:has-text("Export as Excel"))')
            export_button.wait_for(state="visible", timeout=20000)
            logger.info("  ↳ Export button found")
            
            # Scroll the button into view to ensure it's clickable
            export_button.scroll_into_view_if_needed()
            time.sleep(1)
            
            # Try multiple click strategies if regular click fails
            try:
                with page.expect_download(timeout=120000) as download_info:
                    # First try regular click
                    export_button.click(timeout=10000)
                    logger.info("✅ Clicked Export button")
            except Exception as click_error:
                if "intercepts pointer events" in str(click_error).lower():
                    logger.warning("  ↳ Regular click blocked, trying force click...")
                    # Try force click (ignores actionability checks)
                    with page.expect_download(timeout=120000) as download_info:
                        export_button.click(force=True, timeout=10000)
                        logger.info("✅ Clicked Export button (force)")
                else:
                    # Try JavaScript click as last resort
                    logger.warning("  ↳ Force click failed, trying JavaScript click...")
                    with page.expect_download(timeout=120000) as download_info:
                        export_button.evaluate("element => element.click()")
                        logger.info("✅ Clicked Export button (JavaScript)")
            
            download = download_info.value
            
            suggested = download.suggested_filename or "fairhealth_physicians.csv"
            
            # Add batch prefix to filename
            if batch_name:
                name_parts = suggested.rsplit('.', 1)
                suggested = f"{batch_name}_{name_parts[0]}.{name_parts[1]}"
            
            file_path = self.download_dir / suggested
            download.save_as(file_path)
            
            size_mb = file_path.stat().st_size / (1024 * 1024)
            logger.info("-" * 60)
            logger.info("✅ Export complete")
            logger.info(f"📄 {file_path.name}")
            logger.info(f"📂 {file_path}")
            logger.info(f"📊 {size_mb:.2f} MB")
            
            return file_path
            
        except Exception as e:
            logger.error(f"❌ Failed to export CSV: {e}")
            raise

    def download_file(self, geozips, product_category="FH Benchmarks", 
                     product_name="Charge Medical", headless=False, 
                     batch_name="") -> Path:
        """
        Main automation flow for Fair Health Physician data download.
        
        Args:
            geozips: List of geozip codes (e.g., ["070"] or ["USA"])
            product_category: Product category to select
            product_name: Product to select
            headless: Run browser in headless mode
            batch_name: Prefix for downloaded file
            
        Returns:
            Path to saved file
        """
        geozip_str = ",".join(geozips)
        logger.info("=" * 60)
        logger.info("FAIR HEALTH PHYSICIAN DATA DOWNLOAD")
        logger.info("=" * 60)
        logger.info(f"🌐 Target URL: {self.fairhealth_url}")
        logger.info(f"📍 Geozips: {geozip_str}")
        logger.info(f"📦 Product: {product_name}")
        logger.info("-" * 60)

        playwright = sync_playwright().start()
        browser, context, page = self._setup_browser(playwright, headless=headless)

        try:
            logger.info("\n🌐 STEP 1: Navigating to Fair Health...")
            self._safe_goto(page, self.fairhealth_url)
            time.sleep(2)

            logger.info("\n🔐 STEP 2: Logging in...")
            self._login_to_fairhealth(page)
            time.sleep(3)

            logger.info("\n📋 STEP 3: Selecting Product Category...")
            self._select_react_dropdown(page, "ProductId", product_category, "Product Category")
            time.sleep(2)

            logger.info("\n📋 STEP 4: Selecting Product...")
            self._select_react_dropdown(page, "ModuleId", product_name, "Product")
            time.sleep(3)  # Increased wait for Release Date dropdown to appear

            logger.info("\n📅 STEP 5: Selecting Release date...")
            # Wait for Release Date field to be ready before trying to select
            try:
                release_label = page.locator('label:has-text("Release")')
                release_label.wait_for(state="visible", timeout=30000)
                logger.info("  ↳ Release Date field is ready")
            except Exception as e:
                logger.warning(f"  ↳ Release Date label not found immediately: {e}")
            
            self._select_release_date(page)
            time.sleep(2)

            logger.info("\n📍 STEP 6: Entering Geozips...")
            self._enter_geozips(page, geozips)
            time.sleep(2)

            logger.info("\n🔍 STEP 7: Searching for data...")
            self._click_search_and_confirm(page)

            logger.info("\n⏳ STEP 8: Waiting for data to load...")
            self._wait_for_data_to_load(page)

            logger.info("\n📥 STEP 9: Exporting data...")
            file_path = self._export_to_csv(page, batch_name=batch_name)

            logger.info("\n" + "=" * 60)
            logger.info("✅ DOWNLOAD COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
            return file_path

        except Exception as e:
            logger.error("\n" + "=" * 60)
            logger.error(f"❌ DOWNLOAD FAILED: {e}")
            logger.error("=" * 60)
            logger.exception("Full traceback:")
            
            try:
                screenshot_path = self.download_dir / f"error_screenshot_{batch_name}.png"
                page.screenshot(path=str(screenshot_path))
                logger.info(f"📸 Screenshot saved: {screenshot_path}")
            except: pass
            
            raise
        
        finally:
            try:
                context.close()
                browser.close()
                playwright.stop()
                logger.info("🛑 Browser closed and Playwright stopped.")
            except Exception:
                pass


