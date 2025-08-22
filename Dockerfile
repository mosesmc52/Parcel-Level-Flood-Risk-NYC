FROM apache/airflow:2.9.1

# Copy Poetry files
COPY pyproject.toml poetry.lock /app/

# Install Poetry
RUN pip install poetry

# Install dependencies
WORKDIR /app
RUN poetry config virtualenvs.create false && poetry install --no-root

# Copy DAGs and plugins
COPY ./airflow/dags /opt/airflow/dags
COPY ./airflow/plugins /opt/airflow/plugins
