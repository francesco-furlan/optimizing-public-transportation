FROM python:3.7

WORKDIR /ksql-app

COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT [ "python", "ksql.py" ]
