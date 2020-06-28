import os
import logging
import requests
import pandas as pd
from time import sleep
from io import StringIO
from random import randint
from datetime import timedelta, datetime
import speech_recognition as sr
import Xlib.display
from pyvirtualdisplay import Display
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.info('Start time: {}'.format(datetime.now()))

browser_url = 'https://www.tpex.org.tw/web/stock/aftertrading/broker_trading/brokerBS.php?l=zh-tw'
height = 800
width = 1280
whole_screen = (0, 0, width, height)
DOWNLOAD_LOCATION = '~/Downloads/'
IMG_PATH = '/userap/BuySellReport/img/'
os.environ['DISPLAY']=':2'

if len(sys.argv) > 1 and sys.argv[1] == 'skip':
    import pyautogui
    pyautogui._pyautogui_x11._display = Xlib.display.Display(os.environ['DISPLAY'])
else:
    os.system('pkill -f Xvfb')
    os.system('pkill -f chrome')
    os.system('Xvfb :2 -screen 0 1280x800x16 &')
    import pyautogui
    pyautogui._pyautogui_x11._display = Xlib.display.Display(os.environ['DISPLAY'])
    pyautogui.PAUSE = 0.05
    os.system('google-chrome-stable --no-sandbox --window-size=1280,800 --window-position=0,0 --app %s --incognito >> /dev/null 2>&1 &' % browser_url)

def random_move():
    pyautogui.moveTo(randint(0, width), randint(0, height), duration=0.25)

def refresh():
    logging.info('remove audio files')
    os.system('rm -rf ' + DOWNLOAD_LOCATION + "audio.mp3")
    os.system('rm -rf /userap/audio.wav')

    logging.info('refresh page')
    random_move()
    #pyautogui.moveTo(96, 63, duration=0.5)
    #pyautogui.click()
    pyautogui.press('F5')
    retry_count = 0
    while not pyautogui.locateOnScreen(IMG_PATH + 'logo.png', confidence=0.8):
        sleep(2)
        logging.info('still refreshing')
        retry_count += 1
        if retry_count > 5:
            os.system('pkill -f Xvfb')
            os.system('pkill -f chrome')
            exit()
            return
    pyautogui.screenshot(IMG_PATH + '0_after_refresh.png', region=whole_screen)

    logging.info('confirm message box')
    random_move()    
    pyautogui.press('enter')
    pyautogui.screenshot(IMG_PATH + '1_after_confirm.png', region=whole_screen)

    logging.info('click checkbox')
    random_move()
    pyautogui.moveTo(410, 447, duration=1)
    pyautogui.mouseDown()
    pyautogui.mouseUp()
    sleep(2)
    pyautogui.screenshot(IMG_PATH + '2_after_checkbox.png', region=whole_screen)

    if pyautogui.locateOnScreen(IMG_PATH + 'warning.png', confidence=0.8):
        logging.warning('automation is detected')
        os.system('pkill -f chrome')
        sleep(3)
        os.system('google-chrome-stable --no-sandbox --window-size=1280,800 --window-position=0,0 --app %s --incognito >> /dev/null 2>&1 &' % browser_url)
        logging.info('restart chrome')
        return
    elif pyautogui.locateOnScreen(IMG_PATH + 'voice.png', confidence=0.8):
        logging.info('click voice icon')
        random_move()
        pyautogui.moveTo(510, 696, duration=1)
        pyautogui.mouseDown()
        pyautogui.mouseUp()
        sleep(1)
        pyautogui.screenshot(IMG_PATH + '3_after_voice.png', region=whole_screen)
        if pyautogui.locateOnScreen(IMG_PATH + 'warning.png', confidence=0.8):
            logging.warning('automation is detected')
            os.system('pkill -f chrome')
            sleep(3)            
            os.system('google-chrome-stable --no-sandbox --window-size=1280,800 --window-position=0,0 --app %s --incognito >> /dev/null 2>&1 &' % browser_url)
            logging.info('restart chrome')
            return
        else:
            logging.info('go mp3 page')
            random_move()
            pyautogui.moveTo(568, 496, duration=0.5)
            pyautogui.click()
            sleep(3)
            pyautogui.screenshot(IMG_PATH + '31_mp3_page.png', region=whole_screen)

            logging.info('click more icon')
            random_move()
            pyautogui.moveTo(775, 494, duration=0.5)
            pyautogui.click()
            pyautogui.click()
            sleep(1)
            pyautogui.screenshot(IMG_PATH + '32_more_option.png', region=whole_screen)

            logging.info('download mp3')
            random_move()
            pyautogui.moveTo(609, 489, duration=0.5)
            pyautogui.click()
            sleep(1)
            pyautogui.screenshot(IMG_PATH + '33_download.png', region=whole_screen)

            logging.info('close download bar')
            random_move()
            pyautogui.moveTo(1270, 783, duration=0.5)
            pyautogui.click()
            sleep(0.2)
            pyautogui.screenshot(IMG_PATH + '34_close_downloads.png', region=whole_screen)

            logging.info('close download page')
            random_move()
            pyautogui.hotkey('ctrl', 'w')
            sleep(1)
            pyautogui.screenshot(IMG_PATH + '35_close_tab.png', region=whole_screen)

            logging.info('speech recognition') 
            os.system('/userap/anaconda3/bin/ffmpeg -i ' + DOWNLOAD_LOCATION + 'audio.mp3 /userap/audio.wav 2>/dev/null')
            r = sr.Recognizer()
            audio_path = '/userap/audio.wav'
            if os.path.exists(audio_path):
                with sr.AudioFile(audio_path) as source:
                    audio = r.record(source)
                try:
                    text = r.recognize_google(audio)
                    logging.info('Recognized text: {}'.format(text))
                    logging.info('type text')
                    random_move()
                    pyautogui.moveTo(560, 450, duration=0.5)
                    pyautogui.mouseDown()
                    pyautogui.mouseUp()
                    pyautogui.typewrite(text)

                    logging.info('verify')
                    random_move()
                    pyautogui.moveTo(650, 560, duration=0.5)
                    pyautogui.mouseDown()
                    pyautogui.mouseUp()
                    sleep(1)
                    pyautogui.screenshot(IMG_PATH + '4_after_verification.png', region=whole_screen)
                except Exception as e:
                    logging.error('Failed to recognize: '+ str(e))
                    return
            else:
                logging.info('audio file not existed.')
                return

    logging.info('type stock number')
    random_move()
    pyautogui.moveTo(730, 450, duration=0.5)
    pyautogui.mouseDown()
    pyautogui.mouseUp()
    pyautogui.typewrite('1240')

    logging.info('search')
    random_move()
    pyautogui.moveTo(830, 453, duration=0.5)
    pyautogui.mouseDown()
    pyautogui.mouseUp()          
    sleep(1)
    pyautogui.screenshot(IMG_PATH + '5_after_search.png', region=whole_screen)

    logging.info('back to main page')
    random_move()
    pyautogui.moveTo(613, 352, duration=0.5)
    pyautogui.mouseDown()
    pyautogui.mouseUp()
    pyautogui.screenshot(IMG_PATH + '6_after_back.png', region=whole_screen)

    logging.info('refresh finished')

def strToFloat(pdf, num_cols):
    for col in num_cols:
        pdf[col] = pdf[col].astype(str).str.replace(',', '').astype(float)
    return pdf

def getStockList(url):
    df = pd.read_html(url,encoding='big5hkscs',header=0)[0]
    raw_list = df['有價證券代號及名稱']
    code_list = []
    for code in raw_list:
        code = code.split('　')[0]
        if len(code) == 4:
            code_list.append(code)
    return code_list

today_dt = datetime.now()
hour = today_dt.hour
if hour < 9:
    today_dt = today_dt - timedelta(hours=9)
data_dt = today_dt.strftime('%Y%m%d')

otc_page = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=4'
stock_list = getStockList(otc_page)
url = 'https://www.tpex.org.tw/web/stock/aftertrading/broker_trading/download_ALLCSV.php'
root_path = '/userap/BuySellReport/bs_data/%s/' % data_dt
if not os.path.exists(root_path):
    os.mkdir(root_path)
if not os.path.exists(root_path + 'tpex/'):
    os.mkdir(root_path + 'tpex/')
root_path = root_path + 'tpex/'

file_list = [file.replace('.csv', '') for file in os.listdir(root_path) if '.csv' in file]
session = requests.Session()
consecutive_refresh_count = 0
logging.info('total stock: {}'.format(len(stock_list)))
for stock in stock_list:
    if stock in file_list:
        continue
    logging.info(stock)
    payload = {'stk_code': int(stock), 'charset': 'UTF-8'}
    logging.info(payload)
    retry_count = 0
    while True:
        try:
            response = session.post(url, data=payload)
            bs_df = pd.read_csv(StringIO(response.text), sep=',', skiprows=2)
            df_part1 = bs_df.iloc[:, :5]
            df_part2 = bs_df.iloc[:, 6:]
            df_part2.columns = df_part1.columns
            df_merged = df_part1.append(df_part2).sort_values('序號')
            df_merged['券商'] = df_merged['券商'].str[:4]
            df_merged['日期'] = today_dt.strftime('%Y/%m/%d')
            del df_merged['序號']
            df_merged = strToFloat(df_merged, ['買進股數', '賣出股數'])
            df_merged.to_csv(root_path + stock + '.csv', index=False)
            sleep(randint(3, 5))
            consecutive_refresh_count = 0
            break
        except Exception as e:
            logging.error(e)
            retry_count += 1
            if retry_count == 2:
                consecutive_refresh_count += 1
                logging.info('refresh: {}'.format(consecutive_refresh_count))
                refresh()
                retry_count = 0
            if consecutive_refresh_count >= 5:
                exit()

