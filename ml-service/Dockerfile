FROM python:3.12-slim

WORKDIR /app

# Install dependencies first for Docker caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python code and the trained model!
COPY main.py .
COPY isolation_forest.joblib .

EXPOSE 8000

# Run the FastAPI server using Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]