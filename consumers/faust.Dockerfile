FROM python:3.7

WORKDIR /faust-app

COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT [ "faust", "-A", "faust_stream", "worker", "-l", "info" ]
