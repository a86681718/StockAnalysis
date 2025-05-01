import os
import time
import random
import logging
import requests
import pandas as pd
import undetected_chromedriver as uc
from datetime import datetime
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def parse_table(obj):
    """Extract table data from JSON object."""
    if not obj or 'tables' not in obj:
        return None
    table = None
    for t in obj['tables']:
        if len(t) != 0:
            table = t
    if table: 
        columns = [f.strip() for f in table['fields']]
        return pd.DataFrame(table['data'], columns=columns)
    return None

def fetch_json(url, payload):
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }
    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Request failed: {e}")
        return None

def get_stock_list(url, dt):
    stock_payload = {
        "date": dt,
        "type": "EW",
        "id": "",
        "response": "json",
    }
    all_stocks_pdf = parse_table(fetch_json(url, stock_payload))
    if all_stocks_pdf is not None:
        all_stocks_pdf = all_stocks_pdf[all_stocks_pdf['成交筆數'] != 0]
        return all_stocks_pdf[all_stocks_pdf['代號'].str.len() == 4]['代號'].to_list()
    return []

def get_warrant_list(url, dt):
    warrant_payload = {
        "date": dt,
        "type": "WW",
        "id": "",
        "response": "json",
    }
    all_warrant_pdf = parse_table(fetch_json(url, warrant_payload))
    if all_warrant_pdf is not None:
        all_warrant_pdf['成交股數'] = pd.to_numeric(all_warrant_pdf['成交股數'].str.replace(',', ''), errors='coerce')
        all_warrant_pdf['成交筆數'] = pd.to_numeric(all_warrant_pdf['成交筆數'].str.replace(',', ''), errors='coerce')
        all_warrant_pdf = all_warrant_pdf[all_warrant_pdf['成交筆數'] != 0].sort_values('成交股數', ascending=False)
        return all_warrant_pdf['代號'].to_list()
    return []

def get_chrome_options():
    """
    配置 Chrome 瀏覽器選項
    :return: 配置好的 ChromeOptions 物件
    """
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--profile-directory=Default")
    options.add_argument("--incognito")  # 無痕模式
    options.add_argument("--start-maximized")  # 瀏覽器最大化
    options.add_argument("--disable-gpu")
    options.add_argument("--lang=zh-TW")  # 語言和你目標網站對應
    return options    

def simulate_human_behavior(driver):
    """模擬人類操作行為"""
    # 隨機滾動頁面
    for _ in range(random.randint(3, 6)):
        scroll_distance = random.randint(100, 300)
        driver.execute_script(f"window.scrollBy(0, {scroll_distance});")
        time.sleep(random.uniform(0.5, 1.5))

    # 隨機移動鼠標
    action = ActionChains(driver)
    for _ in range(random.randint(5, 10)):
        x_offset = random.randint(-100, 100)
        y_offset = random.randint(-100, 100)
        try:
            action.move_by_offset(x_offset, y_offset).perform()
            time.sleep(random.uniform(0.1, 1))
        except Exception as e:
            pass
    driver.execute_script("window.scrollTo(0, 0);")

def wait_for_turnstile(driver, max_retries=10, wait_time=5):
    """
    等待 Turnstile 驗證完成，並返回驗證 Token。
    :param driver: Selenium WebDriver
    :param max_retries: 最大重試次數
    :param wait_time: 每次重試的等待時間（秒）
    :return: Turnstile Token 或 None
    """
    logging.info("等待 Turnstile 驗證完成...")
    retries = 0
    while retries < max_retries:
        try:
            # 嘗試獲取 Turnstile Token
            token = driver.execute_script("return document.querySelector('[name=cf-turnstile-response]')?.value")
            if token and token.strip():  # 如果 Token 存在且非空
                logging.info(f"取得的 Turnstile Token: {token}")
                return token
            else:
                retries += 1
                logging.warning(f"Turnstile Token 為空，重試第 {retries} 次...")
                time.sleep(wait_time)
        except Exception as e:
            retries += 1
            logging.error(f"嘗試獲取 Turnstile Token 時發生錯誤，重試第 {retries} 次: {e}")
            time.sleep(wait_time)

    logging.error("達到最大重試次數，無法完成 Turnstile 驗證。")
    return None

def crawl_stock_data(stock, token, output_path):
    """爬取股票數據並保存為 CSV"""
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/brokerBS"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.tpex.org.tw",
        "Referer": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/brokerBS.html",
        "User-Agent": "Mozilla/5.0"
    }

    payload = {
        "cf-turnstile-response": token,
        "code": stock
    }
    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()
        table = response.json()['tables'][1]
        pdf = pd.DataFrame(table['data'], columns=table['fields'])
        pdf['日期'] = datetime.now().strftime('%Y/%m/%d')
        pdf['券商'] = pdf['券商'].str.split().str[0]
        pdf.drop(columns=['序號'], inplace=True)
        pdf.to_csv(output_path, encoding='utf8', index=False)
        logging.info(f"成功爬取並保存股票 {stock} 的數據")
    except Exception as e:
        logging.error(f"Failed to crawl data for stock {stock}: {e}")

def main():
    # 設置環境變數
    os.environ['DISPLAY'] = ':99'

    # 股票列表
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc"
    today_dt = datetime.now().strftime('%Y/%m/%d')
    stock_list = get_stock_list(url, today_dt) + get_warrant_list(url, today_dt)
    output_dir = "./output/"
    os.makedirs(output_dir, exist_ok=True)

    for stock in stock_list:
        logging.info(f"開始爬取股票 {stock}")
        driver = uc.Chrome(options=get_chrome_options())
        try:
            # 打開目標頁面
            driver.get("https://www.tpex.org.tw/zh-tw/mainboard/trading/info/brokerBS.html")
            simulate_human_behavior(driver)

            # 等待 Turnstile 驗證完成
            token = wait_for_turnstile(driver)
            if not token:
                logging.error(f"無法獲取 Turnstile Token，跳過股票 {stock}")
                continue

            # 爬取數據
            output_path = os.path.join(output_dir, f"{stock}.csv")
            crawl_stock_data(stock, token, output_path)
        finally:
            driver.quit()

if __name__ == "__main__":
    main()