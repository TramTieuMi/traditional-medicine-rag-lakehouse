FROM node:20-alpine

WORKDIR /app

COPY package*.json ./

RUN npm install --quiet --legacy-peer-deps

COPY . .

EXPOSE 3000

CMD ["npm", "run", "dev"]
