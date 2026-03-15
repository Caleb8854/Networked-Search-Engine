FROM python:3.12-bookworm

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    ninja-build \
    librocksdb-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . /app

RUN python -m pip install --upgrade pip setuptools wheel pybind11

RUN if [ -f backend/requirements.txt ]; then pip install -r backend/requirements.txt; fi

RUN pip install fastapi uvicorn

RUN cmake -S . -B build -G Ninja -DPython3_EXECUTABLE=/usr/local/bin/python && \
    cmake --build build

ENV PYTHONPATH=/app:/app/build:/app/backend

WORKDIR /app/backend

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

