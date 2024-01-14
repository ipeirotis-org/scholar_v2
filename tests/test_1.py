import unittest
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

class FlaskAppTests(unittest.TestCase):

    def setUp(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        service = Service(executable_path="./webdriver/chromedriver")
        self.driver = webdriver.Chrome(service=service, options=chrome_options)



    def test_home_page(self):
        driver = self.driver
        driver.get("https://scholar.ipeirotis.org/") 
        assert "Google Scholar Productivity Search Engine" in driver.title

        elem = driver.find_element(By.NAME, "author_name")
        elem.clear()
        elem.send_keys("Albert Einstein")
        elem.send_keys(Keys.RETURN)
        assert "No author's data available. Please try again." not in driver.page_source

    def tearDown(self):
        self.driver.quit()

if __name__ == "__main__":
    unittest.main()

