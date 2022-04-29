FROM node:15.14.0

COPY package*.json ./
RUN npm install

COPY server.js ./

CMD [ "node", "server.js" ]
