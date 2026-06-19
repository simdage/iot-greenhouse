FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY subscriber.py detector.py ./

CMD ["python", "-u", "subscriber.py"]
