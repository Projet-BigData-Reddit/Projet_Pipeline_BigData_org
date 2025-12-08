FROM python:3.10-slim

WORKDIR /app

# Upgrade pip
RUN pip install --upgrade pip

# 1. Install general requirements (Numpy, FastAPI, etc.)
COPY distilbert_fin_requirement.txt .
RUN pip install --no-cache-dir -r distilbert_fin_requirement.txt

# 2. Install PyTorch (CPU Version) explicitly
# This ignores the huge GPU version and downloads the small CPU one
RUN pip install --no-cache-dir torch==2.2.0 --index-url https://download.pytorch.org/whl/cpu

# Copy your app code
COPY app.py .

# Expose port
EXPOSE 8000

# Run the app
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]