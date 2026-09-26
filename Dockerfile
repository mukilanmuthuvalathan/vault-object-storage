FROM node:22-alpine
WORKDIR /app
COPY package.json ./
COPY src ./src
COPY public ./public
RUN mkdir -p /app/data && chown -R node:node /app
ENV NODE_ENV=production PORT=8080 VAULT_DATA_DIR=/app/data
USER node
EXPOSE 8080
CMD ["node", "src/server.js"]
