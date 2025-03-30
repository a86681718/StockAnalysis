from PIL import Image, ImageFont, ImageDraw

font_file_path = "NotoSansMonoCJKtc-Bold.otf"

rgb_white = (255, 255, 255)
rgb_black = (0, 0, 0)

def txt2Img(title_str, content_str, file_name, font_size=50):
    font = ImageFont.truetype(font_file_path, size=font_size, encoding="unic")

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