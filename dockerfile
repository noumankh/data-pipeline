# Use an official Python runtime as the base image
FROM python:3.10-slim

ENV PYTHONPATH=/app
# Set the working directory
WORKDIR /app

# Install Poetry
RUN pip install --no-cache-dir poetry

# Copy the Poetry configuration files
COPY pyproject.toml poetry.lock ./

# Install dependencies (without creating a virtual environment)
RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi

# Copy the rest of the application code
COPY . .

# Set Poetry as the default command handler
ENTRYPOINT ["poetry", "run"]
CMD ["python", "data_pipeline.py", "--root-dir", "/app/data"]
