import unittest
import random
import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

class DownloadCsvTests(unittest.TestCase):
    def setUp(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": os.getcwd(),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })
        service = Service(executable_path="./webdriver/chromedriver")
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

    def test_download_csv(self):
        self.driver.get("https://scholar.ipeirotis.org/")
        search_terms = ["Panos Ipeirotis", "Adam Heller", "Juliana Freire", "Claudio T. Silva"]
        search_query = random.choice(search_terms)

        search_box = self.driver.find_element(By.NAME, "author_name")
        search_box.clear()
        search_box.send_keys(search_query)
        search_box.send_keys(Keys.RETURN)


        self.driver.implicitly_wait(10) # Adjust time as needed for your application's response time

        download_link = self.driver.find_element(By.LINK_TEXT, "Download CSV")
        download_link.click()

        import time
        time.sleep(10) # Wait for the download to complete

        # Check if the file is downloaded
        expected_filename = search_query.replace(" ", "_") + "_results.csv"
        self.assertTrue(os.path.isfile(expected_filename))

    def tearDown(self):
        self.driver.quit()

if __name__ == "__main__":
    unittest.main()

