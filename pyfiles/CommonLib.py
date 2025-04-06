import os
import platform
from configparser import RawConfigParser
from PIL import Image, ImageFont, ImageDraw


def strToFloat(pdf, num_cols):
    for col in num_cols:
        pdf.loc[:, col] = pdf[col].astype(str).str.replace(',', '').astype(float)
    return pdf


def toRocYear(date_str, sep='/'):
    YEAR_OFFSET = 1911
    split = date_str.split(sep)
    ori_year = split[0]
    new_year = str(int(ori_year) - YEAR_OFFSET)
    new_date_str = date_str.replace(ori_year, new_year)
    return new_date_str


def toAdDate(roc_dt):
    if roc_dt == '':
        return None
    else:
        roc_year = roc_dt.split('/')[0]
        ad_year = str(int(roc_year) + 1911)
        month = roc_dt.split('/')[1]
        day = roc_dt.split('/')[2]
        return '/'.join([ad_year, month, day])


def getConf(section=None):
    conf = RawConfigParser()
    conf.read('conf/default.properties')
    if section:
        return conf._sections[section]
    else:
        if platform.system() == 'Darwin':
            return conf._sections['MAC']
        elif platform.system() == 'Linux':
            return conf._sections['GCP']
        else:
            return conf.defaults()


def txt2Img(title_str, content_str, file_name, font_size=50):
    root_path = getConf().get("root.path")
    font_file_path = os.sep.join([root_path, 'conf', "NotoSansMonoCJKtc-Bold.otf"])
    font = ImageFont.truetype(font_file_path, size=font_size, encoding="unic")
    rgb_white = (255, 255, 255)
    rgb_black = (0, 0, 0)

    title_width = font.getsize(title_str)[0]
    title_height = font.getsize(title_str)[1]

    lines = content_str.split('\n')
    content_width = font.getsize_multiline(content_str)[0]
    content_height = font.getsize_multiline(content_str)[1] 

    max_width = content_width if content_width > title_width else title_width
    max_height = title_height + content_height

    img = Image.new("RGB", (max_width, max_height), rgb_black)
    draw = ImageDraw.Draw(img)
    draw.text(((max_width-title_width)/2,0), title_str, font=font, fill=rgb_white)
    draw.text(((max_width-content_width)/2,title_height), content_str, font=font, fill=rgb_white)
    img.save(file_name)
    
    
def getTick(price):
    if price < 10:
        return 0.01
    elif price < 50:
        return 0.05
    elif price < 100:
        return 0.1
    elif price < 500:
        return 0.5
    elif price < 1000:
        return 1
    elif price < 3000:
        return 5
