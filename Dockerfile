FROM nvidia/cuda:11.8.0-cudnn8-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Europe/Athens \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-dev python3-venv \
    git wget curl ca-certificates \
    tzdata \
    libgl1 libglib2.0-0 \
    build-essential gcc librdkafka-dev \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./requirements.txt

RUN python3 -m pip install --upgrade pip \
    && python3 -m pip install -r requirements.txt

COPY . .

# NEW: build Cython extension for compute_overlap
RUN python3 setup.py build_ext --inplace

ENV PYTHONPATH=/app

CMD ["python3", "reaction.py"]

