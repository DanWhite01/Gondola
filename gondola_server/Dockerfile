# set base image (host OS)
FROM python:3.8-slim

RUN useradd --create-home --shell /bin/bash app_user
WORKDIR /home/app_user

# copy the dependencies file to the working directory
COPY requirements.txt ./


RUN pip install -r requirements.txt

RUN mkdir -p ./app/logs \
    && chown -R app_user ./app/logs

USER app_user

COPY src/ ./app/src
COPY config/ ./app/config
COPY templates/ ./app/templates
COPY util/ ./app/util

COPY compare.py ./app/
COPY rest_main.py ./app/
COPY .env ./app/



CMD [ "python", "./app/rest_main.py" ]