FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY proto /app/proto
COPY dfs_pb2.py /app/dfs_pb2.py
COPY dfs_pb2_grpc.py /app/dfs_pb2_grpc.py
COPY main.py /app/main.py

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
