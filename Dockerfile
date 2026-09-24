FROM node:22-alpine
WORKDIR /app
COPY package.json server.js admin-auth.js ./
COPY public ./public
ENV DATA_DIR=/data
ENV BIND_HOST=0.0.0.0
VOLUME /data
EXPOSE 3123
CMD ["node", "server.js"]
