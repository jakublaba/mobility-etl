import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
from airflow.decorators import dag, task
from azure.storage.blob import BlobServiceClient
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


# Airflow DAG definition
@dag(
    dag_id="scrape_warsaw_traffic_data",
    schedule="@hourly",
    start_date=datetime(2024, 12, 1),
    end_date=datetime(2025, 1, 2),
    catchup=False,
)
def scrape_and_upload_data():
    @task()
    def scrape_data():

        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Enable headless mode
        chrome_options.add_argument("--no-sandbox")  # Necessary for some environments (like Docker)
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("lang=pl-PL")

        # Initialize the WebDriver
        driver = webdriver.Remote("http://chrome-driver:4444", options=chrome_options)

        # Open the target page
        driver.get("https://czynaczas.pl/warsaw/opoznienia-autobusy-tramwaje")
        driver.maximize_window()

        # Zoom out the page to ensure the button is always visible
        driver.execute_script("document.body.style.zoom='0.7';")

        # Handle cookie consent
        button_xpath = '//p[contains(text(), "Zgadzam się")]'
        wait = WebDriverWait(driver, 10)
        agree_button = wait.until(EC.element_to_be_clickable((By.XPATH, button_xpath)))
        agree_button.click()

        # Initial scroll to load content
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 4.5);")

        # Locate the first row cell to focus on the table
        first_row_cell_xpath = '(//div[@aria-colcount="9"]//div[@role="row"]//div[@role="cell"])[1]'  # First cell of the first row in the correct table
        first_cell = wait.until(EC.element_to_be_clickable((By.XPATH, first_row_cell_xpath)))
        first_cell.click()

        # Prepare to store all row data
        all_rows = []

        # Variables to track the current page and row index
        current_page = 0

        # Function to click the "Next page" button
        def click_next_page():
            next_page_button_xpath = '//button[contains(@aria-label, "Przejdź do następnej strony")]'
            try:
                # Wait for the "Next page" button to be clickable
                next_page_button = wait.until(EC.element_to_be_clickable((By.XPATH, next_page_button_xpath)))
                # Click the "Next page" button
                next_page_button.click()
                time.sleep(2)  # Wait for new rows to load
                first_row_cell_xpath = '(//div[@aria-colcount="9"]//div[@role="row"]//div[@role="cell"])[1]'  # First cell of the first row in the correct table
                first_cell = wait.until(EC.element_to_be_clickable((By.XPATH, first_row_cell_xpath)))
                first_cell.click()
                return True
            except Exception as e:
                print(f"Error clicking next page button: {e}")
                return False

        # Function to check stop condition in pagination text
        def check_stop_condition():
            try:
                pagination_text_xpath = '//p[@class="MuiTablePagination-displayedRows css-1chpzqh"]'
                pagination_text_element = wait.until(EC.presence_of_element_located((By.XPATH, pagination_text_xpath)))
                pagination_text = pagination_text_element.text

                # Extract numbers from the pagination text
                numbers = [int(s) for s in pagination_text.split() if s.isdigit()]
                if len(numbers) == 2 and numbers[0] == numbers[1]:
                    print(f"Stop condition met: {pagination_text}. Ending data scraping.")
                    return True
                return False
            except Exception as e:
                print(f"Error checking stop condition: {e}")
                return False

        # Function to get row indexes with retry mechanism
        def get_row_indexes(driver, rows_xpath):
            attempts = 3
            for attempt in range(attempts):
                try:
                    rows = driver.find_elements(By.XPATH, rows_xpath)
                    return [row.get_attribute("data-rowindex") for row in rows if row.get_attribute("data-rowindex")]
                except StaleElementReferenceException:
                    if attempt < attempts - 1:
                        time.sleep(1)  # Wait before retrying
                    else:
                        raise

        # Main scraping loop
        while True:
            # Wait for the page to settle after pressing the "Page Down" key
            time.sleep(1)

            # Simulate pressing the "Page Down" key
            body = driver.find_element(By.TAG_NAME, "body")
            body.send_keys(Keys.PAGE_DOWN)

            # Wait for a short time to allow the new rows to be rendered
            time.sleep(2)

            # Collect all rows from the table with 9 columns (aria-colcount="9")
            rows_xpath = '//div[@aria-colcount="9"]//div[@role="row"]'  # XPath for rows inside the table with 9 columns
            row_indexes = get_row_indexes(driver, rows_xpath)

            # Scrape data for each row
            for index in row_indexes:
                try:
                    row_xpath = f'//div[@aria-colcount="9"]//div[@data-rowindex="{index}"]'
                    row = driver.find_element(By.XPATH, row_xpath)

                    # Extract cells in the row
                    cells_xpath = './/div[@role="cell"]'
                    cells = row.find_elements(By.XPATH, cells_xpath)

                    row_values = []
                    for i, cell in enumerate(cells):
                        if i == 0:  # Skip the first cell (checkbox)
                            continue

                        # Check for specific content types in the cell
                        chip_element = cell.find_elements(By.CLASS_NAME, 'MuiChip-label')
                        if chip_element:
                            row_values.append(chip_element[0].text.strip())  # Chip content
                        elif cell.find_elements(By.TAG_NAME, 'a'):  # Link
                            row_values.append(cell.find_element(By.TAG_NAME, 'a').text.strip())
                        elif cell.find_elements(By.TAG_NAME, 'span'):  # Span content
                            span_element = cell.find_element(By.TAG_NAME, 'span')
                            row_values.append(span_element.text.strip())
                        else:  # Plain text
                            row_values.append(cell.text.strip())

                    if row_values:  # Only append non-empty rows
                        all_rows.append(row_values)

                except NoSuchElementException:
                    print(f"No such element found for row {index}. Retrying...")
                    row_indexes = get_row_indexes(driver, rows_xpath)
                except StaleElementReferenceException:
                    print(f"Stale element reference for row {index}. Retrying...")
                    row_indexes = get_row_indexes(driver, rows_xpath)

            # Check if we've reached the end of the current page
            current_row_count = len(all_rows)
            if current_row_count >= (current_page + 1) * 100:
                print(f"Reached end of page {current_page}. Clicking next page.")
                if not click_next_page():
                    print("No more pages available or button not clickable.")
                    break

                # Update for the next page
                current_page += 1

            # Check the stop condition based on the pagination text
            if check_stop_condition():
                break

        # Define column names
        column_names = ['Type', 'Vehicle No', 'Brigade', 'Route', 'Trip Headsign', 'Delay', 'Stop Name', 'Outside']

        # Create DataFrame and save to CSV
        df = pd.DataFrame(all_rows, columns=column_names)
        df['Timestamp'] = datetime.now()
        return df

    @task()
    def upload_csv_to_blob_storage(df: pd.DataFrame):
        # Upload the parquet file to Azure Blob Storage
        azure_storage_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
        path = datetime.now(tz=ZoneInfo("Europe/Warsaw")).strftime("%Y/%m/%d/delays-%H.csv")
        blob_client = blob_service_client.get_blob_client(container="traffic", blob=path)
        blob_client.upload_blob(df.to_csv(index=False))

    # Define task dependencies
    df = scrape_data()
    upload_csv_to_blob_storage(df)


scrape_and_upload_data()
