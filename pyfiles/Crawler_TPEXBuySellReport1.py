import os
import time
import random
import requests
import pandas as pd
import undetected_chromedriver as uc
from datetime import datetime
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

def parse_table(obj):
    """Extract table data from JSON object."""
    if not obj or 'tables' not in obj:
        return None
    table = None
    for t in obj['tables']:
        if len(t) != 0:
            table = t
    if table: 
        columns = []
        for f in table['fields']:
            columns.append(f.strip())
        return pd.DataFrame(table['data'], columns=columns)
    return None

def fetch_json(url, payload):
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }
    response = requests.post(url, headers=headers, data=payload)
    # 如果成功，回傳 JSON 結構
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print("Request failed:", response.status_code)

def get_stock_list(url, dt):
    stock_payload = {
        "date": dt,
        "type": "EW",
        "id": "",
        "response": "json",
    }
    all_stocks_pdf = parse_table(fetch_json(url, stock_payload))
    all_stocks_pdf = all_stocks_pdf[all_stocks_pdf['成交筆數'] != 0]
    return all_stocks_pdf[all_stocks_pdf['代號'].str.len() == 4]['代號'].to_list()
    
def get_warrant_list(url, dt):
    warrant_payload = {
        "date": dt,
        "type": "WW",
        "id": "",
        "response": "json",
    }
    all_warrant_pdf = parse_table(fetch_json(url, warrant_payload))
    all_warrant_pdf['成交股數'] = pd.to_numeric(all_warrant_pdf['成交股數'].str.replace(',', ''), errors='coerce')
    all_warrant_pdf['成交筆數'] = pd.to_numeric(all_warrant_pdf['成交筆數'].str.replace(',', ''), errors='coerce')
    all_warrant_pdf = all_warrant_pdf[all_warrant_pdf['成交筆數'] != 0].sort_values('成交股數', ascending=False)
    return all_warrant_pdf['代號'].to_list()
    
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

os.environ['DISPLAY'] = ':99' 

url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc"
stock_list = get_stock_list(url, '2025/04/29') + get_warrant_list(url, '2025/04/29')
stock_list[:10]

pdf_list = []
for s in stock_list:
    print(f'start to crawl {s}')
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

    driver = uc.Chrome(options=options)
    
    # Step 2: 前往目標頁面
    url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/brokerBS.html"
    driver.get(url)
    driver.save_screenshot('screenshot1.png')  
    # Step 3: 模擬人類操作行為
    time.sleep(1)
    simulate_human_behavior(driver)
    driver.save_screenshot('screenshot2.png') 
    # Step 4: 等待 Turnstile 載入與驗證完成（你可能要調整等待時間）
    print("等待 Turnstile 驗證完成…")
    token_element = None
    max_retries = 10
    success = False
    retries = 0
    while retries < max_retries and not success:
        try:
            driver.save_screenshot(f'retry-{retries}.png')
            time.sleep(10)
            token_element = driver.execute_script("return document.querySelector('[name=cf-turnstile-response]')?.value")
            if token_element == "":
                retries += 1
                # print(f'retry: {retries}')
            else:
                # print(f'verified successfully')
                success = True
        except:
            retries += 1
            print("Turnstile verification failed or timed out.")
    driver.quit()
    if not success:
        print(f"[s] 無法獲取 Turnstile Token，請檢查網頁或手動驗證。")
        continue
    
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/brokerBS"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.tpex.org.tw",
        "Referer": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/brokerBS.html",
        "User-Agent": "Mozilla/5.0"
    }
    
    # 要送的資料（範例）
    payload = {
        "cf-turnstile-response": token_element,
        "code": s
    }
    try:
        response = requests.post(url, data=payload, headers=headers)
        table = response.json()['tables'][1]
        pdf = pd.DataFrame(table['data'], columns=table['fields'])
        pdf['日期'] = datetime.now().strftime('%Y/%m/%d')
        pdf['券商'] = pdf['券商'].str.split().str[0]
        pdf.drop(columns=['序號'], inplace=True)
        pdf.to_csv(f'{s}.csv', encoding='utf8', index=False)
        print(f'finish crawl {s}')
    except Exception as e:
        print(e)