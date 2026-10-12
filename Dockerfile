FROM node:22-alpine

WORKDIR /app

COPY package.json ./
COPY server.js ./

ENV NODE_ENV=production
ENV CONFIG_DIR=/config

EXPOSE 8998

VOLUME ["/config"]

CMD ["node", "server.js"]
