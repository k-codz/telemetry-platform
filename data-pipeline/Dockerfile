FROM python:3.12-slim

# Install system dependencies required for psycopg2 (Postgres driver)
RUN apt-get update && apt-get install -y libpq-dev gcc

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy our ELT script
COPY elt_job.py .

# Run the script when the container starts
CMD ["python", "elt_job.py"]
