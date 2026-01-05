from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import time

# 設定 ChromeDriver 的路徑
chromedriver_path = "C:/Users/hsi67063/Box/00-home-pigo.hsiao/VBA/chromedriver-win64/chromedriver.exe"  # 替換為你的 chromedriver 路徑

# 建立 ChromeDriver 服務
service = Service(chromedriver_path)

# 啟動瀏覽器
driver = webdriver.Chrome(service=service)

try:
    # 開啟指定的網頁
    url = "https://www.annualreviews.org/content/journals/10.1146/annurev-med-050710-134421"
    driver.get(url)

    # 等待幾秒以便網頁載入完成
    time.sleep(5)

    # 抓取網頁的 meta title
    meta_title = driver.title
    print("Meta Title:", meta_title)  # 列印 meta title

    # 抓取網頁的 meta description
    meta_description = driver.find_element(By.NAME, "description").get_attribute("content")
    print("Meta Description:", meta_description)  # 列印 meta description
finally:
    # 關閉瀏覽器
    driver.quit()