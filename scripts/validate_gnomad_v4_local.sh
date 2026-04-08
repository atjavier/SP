#!/usr/bin/env bash
set -euo pipefail

GNOMAD_DIR="${1:-${SP_GNOMAD_LOCAL_VCF_PATH:-}}"
GNOMAD_FILE_PREFIX="${GNOMAD_FILE_PREFIX:-gnomad.exomes.v4.0.sites.chr}"
GNOMAD_FILE_SUFFIX="${GNOMAD_FILE_SUFFIX:-.vcf.bgz}"
GNOMAD_CHROM_LIST="${GNOMAD_CHROM_LIST:-1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 X Y}"

EXPECTED_TOTAL_GB="${EXPECTED_TOTAL_GB:-283.264}"
SIZE_TOLERANCE_PCT="${SIZE_TOLERANCE_PCT:-10}"

if [ -z "${GNOMAD_DIR}" ]; then
  echo "[validate_gnomad_v4_local] ERROR: Provide a directory argument or set SP_GNOMAD_LOCAL_VCF_PATH."
  exit 2
fi

if [ ! -d "${GNOMAD_DIR}" ]; then
  echo "[validate_gnomad_v4_local] ERROR: Directory not found: ${GNOMAD_DIR}"
  exit 2
fi

echo "[validate_gnomad_v4_local] Directory: ${GNOMAD_DIR}"
echo "[validate_gnomad_v4_local] Expected files: ${GNOMAD_FILE_PREFIX}<chrom>${GNOMAD_FILE_SUFFIX}"
echo "[validate_gnomad_v4_local] Chromosomes: ${GNOMAD_CHROM_LIST}"

missing_files=0
missing_indexes=0
total_bytes=0
total_index_bytes=0

for chrom in ${GNOMAD_CHROM_LIST}; do
  file_name="${GNOMAD_FILE_PREFIX}${chrom}${GNOMAD_FILE_SUFFIX}"
  file_path="${GNOMAD_DIR}/${file_name}"
  if [ ! -f "${file_path}" ]; then
    echo "[validate_gnomad_v4_local] MISSING: ${file_name}"
    missing_files=$((missing_files + 1))
    continue
  fi
  file_size=$(stat -c%s "${file_path}")
  total_bytes=$((total_bytes + file_size))

  if [ -f "${file_path}.tbi" ]; then
    index_size=$(stat -c%s "${file_path}.tbi")
    total_index_bytes=$((total_index_bytes + index_size))
  elif [ -f "${file_path}.csi" ]; then
    index_size=$(stat -c%s "${file_path}.csi")
    total_index_bytes=$((total_index_bytes + index_size))
  else
    echo "[validate_gnomad_v4_local] MISSING INDEX: ${file_name}.tbi"
    missing_indexes=$((missing_indexes + 1))
  fi
done

if [ "${missing_files}" -gt 0 ]; then
  echo "[validate_gnomad_v4_local] ERROR: Missing ${missing_files} VCF file(s)."
  exit 3
fi

if [ "${missing_indexes}" -gt 0 ]; then
  echo "[validate_gnomad_v4_local] ERROR: Missing ${missing_indexes} index file(s)."
  exit 4
fi

total_gb=$(awk -v b="${total_bytes}" 'BEGIN { printf "%.3f", b/1000000000 }')
total_gib=$(awk -v b="${total_bytes}" 'BEGIN { printf "%.3f", b/1024/1024/1024 }')
index_gb=$(awk -v b="${total_index_bytes}" 'BEGIN { printf "%.3f", b/1000000000 }')

echo "[validate_gnomad_v4_local] VCF total: ${total_gb} GB (${total_gib} GiB)"
echo "[validate_gnomad_v4_local] Index total: ${index_gb} GB"

lower_bound=$(awk -v e="${EXPECTED_TOTAL_GB}" -v t="${SIZE_TOLERANCE_PCT}" 'BEGIN { printf "%.3f", e * (100 - t) / 100 }')
upper_bound=$(awk -v e="${EXPECTED_TOTAL_GB}" -v t="${SIZE_TOLERANCE_PCT}" 'BEGIN { printf "%.3f", e * (100 + t) / 100 }')

if awk -v v="${total_gb}" -v lo="${lower_bound}" -v hi="${upper_bound}" 'BEGIN { exit !(v >= lo && v <= hi) }'; then
  echo "[validate_gnomad_v4_local] OK: total size within ${SIZE_TOLERANCE_PCT}% of ${EXPECTED_TOTAL_GB} GB."
  exit 0
fi

echo "[validate_gnomad_v4_local] WARNING: total size ${total_gb} GB is outside ${SIZE_TOLERANCE_PCT}% of ${EXPECTED_TOTAL_GB} GB."
exit 5
