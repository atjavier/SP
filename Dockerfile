FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    build-essential \
    ca-certificates \
    curl \
    default-jre-headless \
    git \
    libbz2-dev \
    libdbd-mysql-perl \
    libdbi-perl \
    libhtml-tableextract-perl \
    libjson-perl \
    liblist-moreutils-perl \
    liblzma-dev \
    libmodule-build-perl \
    libperlio-gzip-perl \
    libset-intervaltree-perl \
    libwww-perl \
    perl \
    tabix \
    unzip \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 8000

CMD ["python", "src/serve.py"]
