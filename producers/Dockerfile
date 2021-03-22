FROM python:3.7

WORKDIR /producer-app

COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT [ "python", "simulation.py" ]
