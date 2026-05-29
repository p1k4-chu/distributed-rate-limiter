FROM python:3.12-slim

WORKDIR /app

# Copy the requirements file from the root context into the container
COPY requirements.txt .

# Install dependencies cleanly without caching temporary installation files
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire engine source code into the container work directory
COPY . .

EXPOSE 50051

CMD ["python", "main.py"]