FROM node:20-bookworm-slim

# System packages: build tools + ไลบรารีที่ Python packages ต้องใช้
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-dev python3-venv \
    build-essential pkg-config cmake git \
    libfreetype6-dev libpng-dev libjpeg-dev zlib1g-dev \
    libsndfile1 ffmpeg libopenblas-dev liblapack-dev \
    libhdf5-dev libxml2-dev libxslt1-dev libffi-dev \
    libssl-dev libcurl4-openssl-dev libpq-dev libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir --break-system-packages --upgrade pip setuptools wheel

# ติดตั้งไลบรารีพื้นฐาน — บังคับ discord.py เวอร์ชันใหม่ที่รองรับ description_localizations
RUN pip3 install --no-cache-dir --break-system-packages --upgrade \
    "discord.py>=2.4.0" aiohttp requests Pillow numpy matplotlib librosa

WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY . .

RUN mkdir -p /data
ENV DATA_DIR=/data
EXPOSE 3000
CMD ["node", "server.js"]
