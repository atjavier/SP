FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Optional evidence toggles (used by setup_evidence_runtime.sh in init containers)
ARG INSTALL_GNOMAD=0
ARG GNOMAD_VCF_BASE_URL=https://hgdownload.soe.ucsc.edu/gbdb/hg38/gnomAD/v4/exomes
ARG GNOMAD_FILE_PREFIX=gnomad.exomes.v4.0.sites.chr
ARG GNOMAD_FILE_SUFFIX=.vcf.bgz
ARG GNOMAD_CHROM_LIST="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 X Y"
ENV INSTALL_GNOMAD=${INSTALL_GNOMAD} \
    GNOMAD_VCF_BASE_URL=${GNOMAD_VCF_BASE_URL} \
    GNOMAD_FILE_PREFIX=${GNOMAD_FILE_PREFIX} \
    GNOMAD_FILE_SUFFIX=${GNOMAD_FILE_SUFFIX} \
    GNOMAD_CHROM_LIST=${GNOMAD_CHROM_LIST}

RUN apt-get update && apt-get install -y --no-install-recommends \
    aria2 \
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
