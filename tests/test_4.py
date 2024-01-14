import unittest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

class ErrorRouteTests(unittest.TestCase):
    def setUp(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        service = Service(executable_path="./webdriver/chromedriver")
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

    def test_error_route(self):
        self.driver.get("https://scholar.ipeirotis.org/error")

        # Check if the header with error message is present
        header = self.driver.find_element(By.TAG_NAME, "h1")
        self.assertIsNotNone(header, "Header element should be present on the error page.")
        
        # Verify that the correct error header text is displayed
        expected_header_text = "An Error Occurred"
        self.assertEqual(header.text, expected_header_text, "The error page should display the correct header.")

        paragraph = self.driver.find_element(By.TAG_NAME, "p")
        self.assertIsNotNone(paragraph, "Paragraph element should be present on the error page.")
        
        expected_paragraph_text = "Sorry, something went wrong. Please try again later."
        self.assertEqual(paragraph.text, expected_paragraph_text, "The error page should display the correct details message.")

    def tearDown(self):
        self.driver.quit()

if __name__ == "__main__":
    unittest.main()

