FROM openjdk:8-jdk-slim as deployment
COPY --from=python:3.7 / /
ENV PYTHONPATH="/app/lib/src:/app/lib/test:$PYTHONPATH"
ENV JAVA_HOME="/usr/local/openjdk-8"

WORKDIR "/app"
COPY lib/ .

COPY . /app

# An explicit installation of GUnicorn is required for it to instantiate worker threads.
RUN pip install -r /app/requirements.txt && \
    pip install gunicorn==20.0.4

EXPOSE 8000
CMD ["gunicorn", "-b",  "0.0.0.0:8000", "--workers", "3", "spark_validation.app", "--timeout", "3000"]
