# pull official base image
FROM node:15

# set working directory
WORKDIR /app

# install app dependencies
COPY package.json ./
COPY package-lock.json ./
RUN npm install && npm update --force

EXPOSE 3000

# start app
CMD ["npm", "start"]