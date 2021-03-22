FROM python:3.7

WORKDIR /consumer-server-app

COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT [ "python", "server.py" ]
