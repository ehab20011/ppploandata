import os
import logging
from pathlib import Path
from playwright.sync_api import sync_playwright

#Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger: logging.Logger = logging.getLogger(__name__)

#Constants
BASE_URL = "https://data.sba.gov/organization/"
DOWNLOAD_DIR = Path(__file__).resolve().parent / "ppp_csvs"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)  # ensure the folder exists before using it
CUSTOM_FILE_NAME = "PPP_loan_dataset.csv"

def scrape_ppp_loan_data():
    #Start the playwright context
    with sync_playwright() as p:
        #Launch a chromium browser (set headless=True so it can run in the background)
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        #Go to the organization index page
        page.goto(BASE_URL)
        logger.info(f"Navigated to {BASE_URL}")

        #Click on the PPP FOIA dataset link from the navbar
        page.wait_for_selector('a[href="/dataset/"]', timeout=5000)
        page.locator('a[href="/dataset/"]').click()
        logger.info("Clicked on the Dataset link from the Navbar")

        #From the dataset listings, click the "PPP FOIA" link
        page.wait_for_selector('h2.dataset-heading a[href="/dataset/ppp-foia"]', timeout=5000)
        page.locator('h2.dataset-heading a[href="/dataset/ppp-foia"]').click()
        logger.info("Navigated to the PPP FOIA dataset page.")

        #Click a link for a CSV file
        page.wait_for_selector('a[href="/dataset/ppp-foia/resource/cff06664-1f75-4969-ab3d-6fa7d6b4c41e"]', timeout=5000)
        page.locator('a[href="/dataset/ppp-foia/resource/cff06664-1f75-4969-ab3d-6fa7d6b4c41e"]').nth(0).click()
        logger.info("Clicked the link for the CSV file page")
        
        #Click the Download link
        page.wait_for_selector('a.btn.btn-primary.resource-url-analytics', timeout=5000)
        download_button = page.locator('a.btn.btn-primary.resource-url-analytics')
        
        with page.expect_download() as download_info:
            download_button.click()
        download = download_info.value # get the download object

        #Download the file to the specified directory with the specified name
        download_path = DOWNLOAD_DIR / CUSTOM_FILE_NAME
        download.save_as(str(download_path))
        logger.info(f"File Downloaded to: {download_path}")
        
        logger.info(f"Test Successful. Closing browser.")
        browser.close()
        return download_path
        
if __name__ == "__main__":
    scrape_ppp_loan_data()
