FROM nvidia/cuda:11.8.0-cudnn8-runtime-ubuntu20.04

# Install Python and dependencies
RUN apt-get update && apt-get install -y \
    python3.8 \
    python3-pip \
    python3.8-dev \
    libgl1 \
    libglib2.0-0 \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Use python3.8 as default python
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.8 1 && \
    update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install Cython numpy && \
    pip install -r requirements.txt

COPY . .

CMD ["python", "reaction_v2.py"]
