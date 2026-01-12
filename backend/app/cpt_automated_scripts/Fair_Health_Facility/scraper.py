import logging
from pathlib import Path
from playwright.sync_api import sync_playwright, Page
import time
import sys

logger = logging.getLogger(__name__)

class FairHealthScraper:
    """
    Scrapes Fair Health website by logging in, selecting filters,
    and downloading the resulting CSV data.
    """

    # ----- CONFIG -----
    FAIRHEALTH_URL = "https://fhonline.fairhealth.org/login"

    # Credentials
    EMAIL = "david.delvecchio@premier-surgical.com"
    PASSWORD = "Clifton999!!"

    # Proxy details (US exit)
    PROXY_SERVER = "http://142.111.48.253:7030"
    PROXY_USERNAME = "eqiwjzzo"
    PROXY_PASSWORD = "c3doqndordj6"
    # -------------------

    def __init__(self, download_dir: Path = Path.cwd() / "downloads_fairhealth"):
        self.download_dir = download_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Download directory set to: {self.download_dir}")

    def _setup_browser(self, playwright, headless=True):
        """
        Start Playwright and launch Chromium with proxy configuration.
        Returns (browser, context, page).
        """
        logger.info(f"🚀 Launching browser (Headless: {headless})")
        launch_args = {"headless": headless}

        if self.PROXY_SERVER:
            launch_args["proxy"] = {"server": self.PROXY_SERVER}
            if self.PROXY_USERNAME and self.PROXY_PASSWORD:
                launch_args["proxy"]["username"] = self.PROXY_USERNAME
                launch_args["proxy"]["password"] = self.PROXY_PASSWORD
            logger.info(f"🔌 Using proxy: {self.PROXY_SERVER}")
        
        browser = playwright.chromium.launch(**launch_args)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.set_default_timeout(60000)
        return browser, context, page

    def _safe_goto(self, page, url, timeout=60000, attempts=2):
        """
        Navigate with graceful retry using DOMContentLoaded.
        """
        last_exc = None
        for i in range(attempts):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                time.sleep(2)  # Allow dynamic content to render
                return
            except Exception as e:
                logger.warning(f"⚠️ Navigation attempt {i+1} failed: {e}")
                last_exc = e
                time.sleep(3)
        raise last_exc

    def _login_to_fairhealth(self, page):
        """
        Handle login flow:
        - Fill email and password
        - Click sign in button
        - Wait for successful login
        """
        logger.info("🔐 Starting login process...")
        try:
            email_input = page.locator('input#Email')
            email_input.wait_for(state="visible", timeout=20000)
            email_input.fill(self.EMAIL)
            logger.info(f"✅ Filled email")
            time.sleep(0.5)
            
            password_input = page.locator('input#Password')
            password_input.wait_for(state="visible", timeout=20000)
            password_input.fill(self.PASSWORD)
            logger.info("✅ Filled password")
            time.sleep(0.5)
            
            signin_button = page.locator('button#signin')
            signin_button.wait_for(state="visible", timeout=20000)
            signin_button.click()
            logger.info("✅ Clicked Sign In button")
            
            time.sleep(5)  # Give time for redirect
            
            product_category_label = page.locator('label:has-text("Product Category")')
            product_category_label.wait_for(state="visible", timeout=30000)
            logger.info("✅ Login successful - Dashboard loaded")
            
        except Exception as e:
            logger.error(f"❌ Login failed: {e}")
            raise

    def _select_react_dropdown(self, page, container_id, option_text, dropdown_type="product"):
        """ Handle React-Select dropdown selection """
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
        """ Select the most recent release date (top option) """
        logger.info("📽 Selecting most recent Release date...")
        try:
            release_container = page.locator('div:has(> label:has-text("Release"))').first
            
            dropdown_control = release_container.locator('.select__control')
            dropdown_control.wait_for(state="visible", timeout=20000)
            dropdown_control.click()
            logger.info("  ↳ Opened Release dropdown")
            time.sleep(1)
            
            first_option = page.locator('.select__option').first
            first_option.wait_for(state="visible", timeout=20000)
            option_text = first_option.inner_text()
            first_option.click()
            logger.info(f"✅ Selected Release: {option_text}")
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"❌ Failed to select Release date: {e}")
            raise

    def _enter_geozips(self, page):
        """ Enter geozip values directly into the input field """
        logger.info("📽 Entering Geozips...")
        try:
            geozip_input = page.locator('input#geozip')
            geozip_input.wait_for(state="visible", timeout=20000)
            
            # Clear any existing value
            geozip_input.click()
            page.keyboard.press('Control+A')
            page.keyboard.press('Backspace')
            time.sleep(0.5)
            
            # Type the geozip values
            geozip_values = "070,074,USA"
            geozip_input.fill(geozip_values)
            logger.info(f"✅ Entered Geozips: {geozip_values}")
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"❌ Failed to enter Geozips: {e}")
            try:
                page.screenshot(path=self.download_dir / "geozip_error.png")
                logger.info(f"📸 Screenshot saved to: {self.download_dir / 'geozip_error.png'}")
            except: pass
            raise

    def _click_search_and_confirm(self, page):
        """ Click the Search button and handle the confirmation popup. """
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
        """ Wait for the data grid to be populated with results. """
        logger.info("⏳ Waiting for data to load...")
        try:
            grid = page.locator('div#productsGrid')
            grid.wait_for(state="visible", timeout=60000)
            time.sleep(10) # Give substantial time for data loading
            
            rows = page.locator('.ag-row')
            
            for i in range(60):
                count = rows.count()
                if count > 0:
                    logger.info(f"✅ Data loaded - Found {count} rows")
                    break
                time.sleep(1)
                if i % 10 == 0:
                    logger.info(f"  ↳ Still waiting... ({i}s)")
            else:
                logger.warning("⚠️ Timeout waiting for data rows, proceeding anyway...")
            
            time.sleep(2)
            
        except Exception as e:
            logger.warning(f"⚠️ Warning during data load wait: {e}")
            time.sleep(5)

    def _export_to_csv(self, page) -> Path:
        """ Click the Export as Excel button and save the downloaded file. """
        logger.info("📥 Exporting data to CSV...")
        try:
            export_button = page.locator('div.search-header-content a:has(label:has-text("Export as Excel"))')
            export_button.wait_for(state="visible", timeout=20000)
            
            with page.expect_download(timeout=120000) as download_info:
                export_button.click()
                logger.info("✅ Clicked Export button")
            
            download = download_info.value
            
            suggested = download.suggested_filename or "fairhealth_facility.csv"
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

    def download_file(self, headless=True) -> Path:
        """
        Main automation flow for Fair Health data download.
        Returns Path to saved file.
        """
        logger.info("=" * 60)
        logger.info("FAIR HEALTH DATA DOWNLOAD AUTOMATION")
        logger.info("=" * 60)
        logger.info(f"🌐 Target URL: {self.FAIRHEALTH_URL}")
        logger.info("-" * 60)

        playwright = sync_playwright().start()
        browser, context, page = self._setup_browser(
            playwright,
            headless=headless,
        )

        try:
            logger.info("\n🌐 STEP 1: Navigating to Fair Health...")
            self._safe_goto(page, self.FAIRHEALTH_URL)
            time.sleep(2)

            logger.info("\n🔐 STEP 2: Logging in...")
            self._login_to_fairhealth(page)
            time.sleep(3)

            logger.info("\n📋 STEP 3: Selecting Product Category...")
            self._select_react_dropdown(page, "ProductId", "FH Benchmarks", "Product Category")
            time.sleep(2)

            logger.info("\n📋 STEP 4: Selecting Product...")
            self._select_react_dropdown(page, "ModuleId", "Allowed ASC Facility", "Product")
            time.sleep(2)

            logger.info("\n📅 STEP 5: Selecting Release date...")
            self._select_release_date(page)
            time.sleep(2)

            logger.info("\n📍 STEP 6: Entering Geozips...")
            self._enter_geozips(page)
            time.sleep(2)

            logger.info("\n🔍 STEP 7: Searching for data...")
            self._click_search_and_confirm(page)

            logger.info("\n⏳ STEP 8: Waiting for data to load...")
            self._wait_for_data_to_load(page)

            logger.info("\n📥 STEP 9: Exporting data...")
            file_path = self._export_to_csv(page)

            logger.info("\n" + "=" * 60)
            logger.info("✅ AUTOMATION COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
            return file_path

        except Exception as e:
            logger.error("\n" + "=" * 60)
            logger.error(f"❌ AUTOMATION FAILED: {e}")
            logger.error("=" * 60)
            logger.exception("Full traceback:")
            
            try:
                screenshot_path = self.download_dir / "error_screenshot.png"
                page.screenshot(path=str(screenshot_path))
                logger.info(f"📸 Screenshot saved: {screenshot_path}")
            except: pass
            
            raise # Re-raise the exception to fail the pipeline
        
        finally:
            try:
                context.close()
                browser.close()
                playwright.stop()
                logger.info("🛑 Browser closed and Playwright stopped.")
            except Exception:
                pass
