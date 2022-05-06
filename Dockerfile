# syntax=docker/dockerfile:1
FROM python:3.9
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV FLASK_APP=yo1k.qaws.qaws_app
ENV FLASK_RUN_HOST=0.0.0.0
WORKDIR /code
COPY requirements.txt /code/
RUN pip install -r requirements.txt
COPY ./yo1k/ /code/
COPY Dockerfile /code/
COPY docker-compose.yml /code/
CMD ["flask", "run"]
