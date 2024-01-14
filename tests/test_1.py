import unittest
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService

class FlaskAppTests(unittest.TestCase):

    def setUp(self):
        self.driver = webdriver.Chrome(executable_path="./webdriver/chromedriver")



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
        self.driver.close()

if __name__ == "__main__":
    unittest.main()

