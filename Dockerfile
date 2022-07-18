FROM python:3.8-alpine

WORKDIR /usr/src/app

RUN pip install --upgrade pip
COPY . .
RUN pip install psycopg2-binary
RUN pip install -r requirements.txt

COPY ./entrypoint.sh .

ENTRYPOINT [ "sh", "entrypoint.sh" ]
