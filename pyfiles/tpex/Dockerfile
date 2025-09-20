FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /app

RUN apt-get update && apt-get install -y \
    wget curl unzip gnupg ca-certificates \
    libnss3 libatk-bridge2.0-0 libx11-xcb1 libxcomposite1 \
    libxdamage1 libxrandr2 libasound2 libxshmfence1 libgtk-3-0 \
    libgbm1 libdrm2 xvfb x11-xserver-utils mesa-utils \
    fonts-noto fonts-noto-cjk fonts-liberation fonts-dejavu \
    alsa-utils pulseaudio \
    && rm -rf /var/lib/apt/lists/*

# 安裝 Chrome v136
RUN wget https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/136.0.7103.113/linux64/chrome-linux64.zip && \
    unzip chrome-linux64.zip && mv chrome-linux64 /opt/chrome && \
    ln -s /opt/chrome/chrome /usr/bin/google-chrome

# 安裝 ChromeDriver
RUN wget https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/136.0.7103.113/linux64/chromedriver-linux64.zip && \
    unzip chromedriver-linux64.zip && mv chromedriver-linux64/chromedriver /usr/bin/chromedriver && \
    chmod +x /usr/bin/chromedriver

# 安裝 Python 套件
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# 複製 Python 檔案
COPY Crawler_TPEXBuySellReport.py /app/Crawler_TPEXBuySellReport.py
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]
