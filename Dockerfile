FROM openjdk:8-jdk-slim as deployment
COPY --from=python:3.7 / /
ENV PYTHONPATH="/app/lib/src:/app/lib/test:$PYTHONPATH"
ENV JAVA_HOME="/usr/local/openjdk-8"

WORKDIR "/app"
COPY lib/ .

COPY . /app

RUN pip install -r /app/requirements.txt

RUN python /app/lib/src/spark_validation/dataframe_validation/file_system_validator.py -c /app/lib/test/spark_validation_tests/common/mock_data/config_example_fs.json
#CMD ["sleep" , "50000"]
