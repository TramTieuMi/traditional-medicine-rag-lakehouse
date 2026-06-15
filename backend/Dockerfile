FROM node:20-alpine

# Cài curl để dùng healthcheck
RUN apk add --no-cache curl

WORKDIR /app

COPY package*.json ./

RUN npm install --quiet --production

COPY . .

EXPOSE 5000

# Healthcheck đảm bảo backend đã sẵn sàng trước khi các service phụ thuộc kết nối
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=5 \
  CMD curl -sf http://localhost:5000/health || exit 1

CMD ["node", "server.js"]

