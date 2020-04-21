FROM openjdk:8-jdk-slim as deployment
COPY --from=python:3.7 / /
ENV PYTHONPATH="/app/lib/src:/app/lib/test:$PYTHONPATH"
ENV JAVA_HOME="/usr/local/openjdk-8"

WORKDIR "/app"
COPY lib/ .

COPY . /app

RUN pip install -r /app/requirements.txt

EXPOSE 8000
CMD ["gunicorn", "-b",  "0.0.0.0:8000", "spark_validation.app"]
