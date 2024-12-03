import os
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from airflow.decorators import dag, task
from azure.storage.blob import BlobServiceClient

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

        # Initialize the WebDriver
        download_service = Service()  # Add path to chromedriver if necessary
        driver = webdriver.Chrome(service=download_service, options=chrome_options)

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

        # Handle "Rozumiem" button
        button_xpath_understand = '//button[contains(text(), "Rozumiem")]'
        understand_button = wait.until(EC.element_to_be_clickable((By.XPATH, button_xpath_understand)))
        understand_button.click()

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
        current_row_index = 0  # Start with row 0 for the first page

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
            rows = driver.find_elements(By.XPATH, rows_xpath)

            # Filter rows and collect data
            row_indexes = [row.get_attribute("data-rowindex") for row in rows if row.get_attribute("data-rowindex")]

            # Scrape data for each row
            for row in rows:
                try:
                    row_cells = row.find_elements(By.XPATH, './/div[@role="cell"]')
                    row_values = []
                    for i, cell in enumerate(row_cells):
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

                except Exception as e:
                    print(f"Error processing row: {e}")

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
        return df

    @task()
    def save_data_as_parquet(df):
        # Save DataFrame to Parquet
        parquet_file = "/tmp/delay_data.parquet"
        df.to_parquet(parquet_file, index=False)
        return parquet_file

    @task()
    def upload_to_blob_storage(parquet_file):
        # Upload the parquet file to Azure Blob Storage
        abs_conn_str = os.getenv("ABS_CONNECTION_STRING")
        blob_service_client = BlobServiceClient.from_connection_string(abs_conn_str)
        container_name = "traffic-data"
        blob_name = f"delay_data_{datetime.today().strftime('%Y_%m_%d')}.parquet"
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        with open(parquet_file, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

    # Define task dependencies
    df = scrape_data()
    parquet_file = save_data_as_parquet(df)
    upload_to_blob_storage(parquet_file)

    scrape_data() >> save_data_as_parquet(df) >> upload_to_blob_storage(parquet_file)

scrape_and_upload_data()
