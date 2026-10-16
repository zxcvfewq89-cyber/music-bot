FROM node:20-slim

# ติดตั้ง ffmpeg และ curl ไว้ในระบบคอนเทนเนอร์โดยตรง
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
