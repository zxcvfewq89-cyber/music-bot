FROM node:20-slim

# ติดตั้ง ffmpeg, curl และ python สำหรับดาวน์โหลดและแปลงเสียงเพลง
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

CMD ["node", "index.js"]
