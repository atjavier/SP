#!/usr/bin/env bash
set -euo pipefail

API_FILE="/opt/vep/ensembl-vep/Bio/EnsEMBL/Variation/DBSQL/VariationFeatureAdaptor.pm"
ALPHA_FILE="/opt/vep/.vep/Plugins/AlphaMissense_hg38.tsv.gz"
ALPHA_TBI="${ALPHA_FILE}.tbi"
CACHE_DIR="/opt/vep/.vep/homo_sapiens/115_GRCh38"

echo "[check_vep_runtime] Checking required files..."

missing=0

if [ -f "${API_FILE}" ]; then
  echo "OK API: ${API_FILE}"
else
  echo "MISSING API: ${API_FILE}"
  missing=1
fi

if [ -f "${ALPHA_FILE}" ]; then
  echo "OK AlphaMissense: ${ALPHA_FILE}"
else
  echo "MISSING AlphaMissense: ${ALPHA_FILE}"
  missing=1
fi

if [ -f "${ALPHA_TBI}" ]; then
  echo "OK AlphaMissense index: ${ALPHA_TBI}"
else
  echo "MISSING AlphaMissense index: ${ALPHA_TBI}"
  missing=1
fi

if [ -d "${CACHE_DIR}" ]; then
  echo "OK Cache: ${CACHE_DIR}"
else
  echo "MISSING Cache: ${CACHE_DIR}"
  missing=1
fi

if perl \
  -I/opt/vep/ensembl-vep \
  -I/opt/vep/ensembl-vep/modules \
  -MBio::DB::HTS::Tabix \
  -MBio::EnsEMBL::Variation::DBSQL::VariationFeatureAdaptor \
  -MList::MoreUtils \
  -MLWP::Simple \
  -e 1 >/dev/null 2>&1; then
  echo "OK Perl runtime modules: Tabix + Ensembl Variation + List::MoreUtils + LWP::Simple"
else
  echo "MISSING Perl runtime module(s): Tabix and/or Ensembl Variation and/or List::MoreUtils and/or LWP::Simple"
  missing=1
fi

if [ "${missing}" -ne 0 ]; then
  echo "[check_vep_runtime] FAILED"
  exit 1
fi

echo "[check_vep_runtime] PASSED"
