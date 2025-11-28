from pathlib import Path
from playwright.sync_api import sync_playwright
import time
import sys
import logging

logger = logging.getLogger(__name__)

# ----- CONFIG -----
NOVITAS_URL = "https://www.novitas-solutions.com/webcenter/portal/MedicareJL/FeeLookup"

# Proxy details (US exit)
PROXY_SERVER = "http://142.111.48.253:7030"
PROXY_USERNAME = "eqiwjzzo"
PROXY_PASSWORD = "c3doqndordj6"
# -------------------

def setup_browser(headless=False, proxy_server=None, proxy_user=None, proxy_pass=None):
    """
    Start Playwright and launch Chromium. Optionally uses proxy.
    Returns (playwright, browser, context, page).
    """
    playwright = sync_playwright().start()
    launch_args = {"headless": headless}

    if proxy_server:
        launch_args["proxy"] = {"server": proxy_server}
        if proxy_user and proxy_pass:
            launch_args["proxy"]["username"] = proxy_user
            launch_args["proxy"]["password"] = proxy_pass

    browser = playwright.chromium.launch(**launch_args)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()
    page.set_default_timeout(60000)
    return playwright, browser, context, page


def safe_goto(page, url, timeout=60000, attempts=2, pause=4):
    """
    Navigate with graceful retry. Uses DOMContentLoaded to avoid 'networkidle' hangs.
    """
    last_exc = None
    for i in range(attempts):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            # extra tiny wait so dynamic UI renders
            time.sleep(2)
            return
        except Exception as e:
            print(f"⚠️ Navigation attempt {i+1} failed: {e}")
            last_exc = e
            try:
                page.reload(timeout=timeout, wait_until="domcontentloaded")
                time.sleep(pause)
            except Exception as e2:
                print(f"⚠️ Reload failed: {e2}")
                time.sleep(pause)
    raise last_exc


def handle_preferences_and_terms(page):
    """
    Handle the preferences and terms & conditions page that appears first.
    Clicks through the required radio buttons, checkbox, and submit button.
    """
    print("🔧 Handling preferences and terms page...")
    
    try:
        # 1. Click "Health care professional" radio button (already checked by default, but ensure it)
        healthcare_radio = page.locator('input[id="T:pt_sor3:_0"]')
        healthcare_radio.wait_for(state="visible", timeout=20000)
        if not healthcare_radio.is_checked():
            healthcare_radio.click()
        print("✅ Selected: Health care professional")
        time.sleep(0.5)
        
        # 2. Click "Part B: Physicians & other health care professionals" radio button
        part_b_radio = page.locator('input[id="T:pt_sor1:_1"]')
        part_b_radio.wait_for(state="visible", timeout=20000)
        part_b_radio.click()
        print("✅ Selected: Part B - Physicians & other health care professionals")
        time.sleep(0.5)
        
        # 3. Check "Accept Terms and Conditions" checkbox
        terms_checkbox = page.locator('input[id="T:pt_cpt_disclaimer::content"]')
        terms_checkbox.wait_for(state="visible", timeout=20000)
        terms_checkbox.check()
        print("✅ Accepted Terms and Conditions")
        time.sleep(0.5)
        
        # 4. Click "Set Preference" button
        set_pref_button = page.locator('button[id="T:pt_cb1"]')
        set_pref_button.wait_for(state="visible", timeout=20000)
        set_pref_button.click()
        print("✅ Clicked 'Set Preference' button")
        
        # Wait for page to load after clicking Set Preference
        time.sleep(3)
        print("✅ Preferences page completed successfully")
        
    except Exception as e:
        print(f"⚠️ Error handling preferences page: {e}")
        raise


def select_dropdown_by_label(page, selector, visible_text):
    """
    Wait for <select> to be visible and select by label (visible text).
    """
    locator = page.locator(selector)
    locator.wait_for(state="visible", timeout=20000)
    # select_option supports label argument
    page.select_option(selector, label=visible_text)
    print(f"✅ Selected '{visible_text}' for {selector}")


def click_download_button(page, download_button_selector, expect_download=True):
    """
    Clicks the download UI element. 
    If expect_download=True, waits for and returns Playwright download object.
    If expect_download=False, just clicks without waiting for download.
    """
    # Wait until download button is visible & enabled
    btn = page.locator(download_button_selector)
    btn.wait_for(state="visible", timeout=20000)

    if expect_download:
        with page.expect_download(timeout=120000) as dl_info:
            btn.click()
        return dl_info.value
    else:
        btn.click()
        return None


def handle_error_popup(page):
    """
    Handle the error popup that appears with message "one or more required fields are missing"
    Click the OK button to close it.
    """
    print("⚠️ Handling error popup...")
    
    try:
        # Wait for the OK button in the error dialog to appear
        ok_button = page.locator('button[id="docrt::msgDlg::cancel"]')
        ok_button.wait_for(state="visible", timeout=10000)
        
        # Click OK button
        ok_button.click()
        print("✅ Clicked OK button in error popup")
        time.sleep(1)
        
    except Exception as e:
        print(f"⚠️ Error handling popup (might not have appeared): {e}")
        # Continue anyway - popup might not always appear


def download_novitas_fee_schedule(output_dir=None, headless=False,
                                  proxy_server=None, proxy_user=None, proxy_pass=None):
    """
    Main flow (mimicking human behavior):
      - open page (via proxy if provided)
      - handle preferences and terms page
      - Select Year, State, File Type
      - Click Download button FIRST time
      - Click Download button SECOND time (triggers error popup)
      - Handle error popup by clicking OK
      - Select Locality
      - Click Download button THIRD time (actual download)
      - save file into output_dir (cwd by default)
    Returns Path to saved file.
    """
    download_dir = Path(output_dir) if output_dir else Path.cwd()
    download_dir.mkdir(parents=True, exist_ok=True)

    print(f"📁 Download directory: {download_dir}")
    print(f"🌐 Opening page: {NOVITAS_URL}")
    print("-" * 60)

    playwright, browser, context, page = setup_browser(
        headless=headless,
        proxy_server=proxy_server,
        proxy_user=proxy_user,
        proxy_pass=proxy_pass,
    )

    try:
        # Reliable navigation with retry
        safe_goto(page, NOVITAS_URL)

        # small pause to let any client JS run/render widgets
        time.sleep(3)

        # Handle preferences and terms & conditions page
        handle_preferences_and_terms(page)
        
        # Additional wait for the fee lookup page to fully load
        time.sleep(5)

        # Selectors from provided DOM (using attribute selector so colons in id are fine)
        year_select = 'select[id="T:dclay:oc_9513666413roialH1:s3:soc4::content"]'
        state_select = 'select[id="T:dclay:oc_9513666413roialH1:s3:soc8::content"]'
        locality_select = 'select[id="T:dclay:oc_9513666413roialH1:s3:soc5::content"]'
        filetype_select = 'select[id="T:dclay:oc_9513666413roialH1:s3:soc10::content"]'

        # Download button: using the anchor/button wrapper id - escaped selector when using CSS id with colons
        download_button_selector = '#T\\:dclay\\:oc_9513666413roialH1\\:s3\\:cb3'

        # === STEP 1: Select Year, State, and File Type ===
        print("🔽 STEP 1: Selecting Year, State, and File Type...")
        select_dropdown_by_label(page, year_select, "2026")
        time.sleep(0.8)
        
        select_dropdown_by_label(page, state_select, "New Jersey")
        time.sleep(0.8)
        
        select_dropdown_by_label(page, filetype_select, "Excel")
        time.sleep(3)
        print("✅ Year, State, and File Type selected")

        # === STEP 2: Click Download button FIRST time ===
        print("🔘 STEP 2: Clicking Download button (1st time)...")
        click_download_button(page, download_button_selector, expect_download=False)
        time.sleep(2)
        print("✅ First download click completed")

        # === STEP 3: Click Download button SECOND time (this triggers error popup) ===
        print("🔘 STEP 3: Clicking Download button (2nd time)...")
        click_download_button(page, download_button_selector, expect_download=False)
        time.sleep(2)
        print("✅ Second download click completed")

        # === STEP 4: Handle error popup by clicking OK ===
        print("🔘 STEP 4: Handling error popup...")
        handle_error_popup(page)
        time.sleep(1)

        # === STEP 5: Now select Locality ===
        print("🔽 STEP 5: Selecting Locality...")
        select_dropdown_by_label(page, locality_select, "Northern New Jersey (01)")
        time.sleep(1)
        print("✅ Locality selected")

        # === STEP 6: Click Download button THIRD time (actual download) ===
        print("📥 STEP 6: Clicking Download button (3rd time) for actual download...")
        download = click_download_button(page, download_button_selector, expect_download=True)

        # Save downloaded file to desired folder
        suggested = download.suggested_filename or "downloaded_file"
        file_path = download_dir / suggested
        download.save_as(file_path)

        size_mb = file_path.stat().st_size / (1024 * 1024)
        print("-" * 60)
        print("✅ Download complete")
        print(f"📄 {file_path.name}")
        print(f"📂 {file_path}")
        print(f"📊 {size_mb:.2f} MB")

        return file_path

    except Exception as e:
        print("-" * 60)
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        try:
            context.close()
            browser.close()
            playwright.stop()
        except Exception:
            pass


class NovitasScraper:
    """Wrapper class for the scraper functionality"""
    
    def __init__(self, output_dir=None):
        self.output_dir = Path(output_dir) if output_dir else Path.cwd()
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def download_excel_file(self, headless=False) -> Path:
        """
        Download the Excel file using the scraper module
        Returns Path to downloaded file
        """
        logger.info("🌐 Starting file download...")
        
        try:
            file_path = download_novitas_fee_schedule(
                output_dir=str(self.output_dir),
                headless=headless,
                proxy_server=PROXY_SERVER,
                proxy_user=PROXY_USERNAME,
                proxy_pass=PROXY_PASSWORD,
            )
            
            logger.info(f"✅ File downloaded successfully: {file_path}")
            return Path(file_path)
            
        except Exception as e:
            logger.error(f"❌ Error downloading file: {e}")
            raise


if __name__ == "__main__":
    # set headless False to watch the browser; True for invisible runs
    downloaded = download_novitas_fee_schedule(
        output_dir=None,
        headless=False,
        proxy_server=PROXY_SERVER,
        proxy_user=PROXY_USERNAME,
        proxy_pass=PROXY_PASSWORD,
    )
    print("Done. Saved:", downloaded)