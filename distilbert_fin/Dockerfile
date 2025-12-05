FROM python:3.10-slim

WORKDIR /app

# Update pip first
RUN pip install --upgrade pip

# Copy requirements and install
COPY distilbert_fin_requirement.txt .
RUN pip install --no-cache-dir -r distilbert_fin_requirement.txt

# Copy app code
COPY app.py .

# Expose port
EXPOSE 8000

# Run the FastAPI service
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
