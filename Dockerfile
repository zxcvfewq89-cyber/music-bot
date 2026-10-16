FROM node:20-slim
WORKDIR /app
RUN apt-get update && apt-get install -y ffmpeg
COPY package*.json ./
RUN npm install
COPY . .
CMD ["node", "index.js"]
