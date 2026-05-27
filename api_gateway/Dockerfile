# Use an official lightweight Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Copy our requirements file first to take advantage of Docker's build cache
COPY requirements.txt .

# Install the required dependencies inside the container image
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy the rest of our application code into the container
COPY . .

# Expose the port that our FastAPI gateway runs on
EXPOSE 8000

# Run Uvicorn when the container launches
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]