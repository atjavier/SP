#!/usr/bin/env bash
set -euo pipefail

EVIDENCE_ROOT="${EVIDENCE_ROOT:-/opt/evidence}"
READY_MARKER="${EVIDENCE_ROOT}/.evidence-ready"

INSTALL_DBSNP="${INSTALL_DBSNP:-1}"
INSTALL_CLINVAR="${INSTALL_CLINVAR:-1}"
INSTALL_GNOMAD="${INSTALL_GNOMAD:-0}"

DBSNP_LOCAL_VCF_PATH="${DBSNP_LOCAL_VCF_PATH:-${EVIDENCE_ROOT}/dbsnp/dbsnp_all_grch38.vcf.gz}"
DBSNP_VCF_URL="${DBSNP_VCF_URL:-https://ftp.ncbi.nlm.nih.gov/snp/latest_release/VCF/GCF_000001405.40.gz}"
DBSNP_TBI_URL="${DBSNP_TBI_URL:-${DBSNP_VCF_URL}.tbi}"

CLINVAR_LOCAL_VCF_PATH="${CLINVAR_LOCAL_VCF_PATH:-${EVIDENCE_ROOT}/clinvar/clinvar_grch38.vcf.gz}"
CLINVAR_VCF_URL="${CLINVAR_VCF_URL:-https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz}"
CLINVAR_TBI_URL="${CLINVAR_TBI_URL:-${CLINVAR_VCF_URL}.tbi}"

GNOMAD_LOCAL_VCF_PATH="${GNOMAD_LOCAL_VCF_PATH:-${EVIDENCE_ROOT}/gnomad/v4.0/exomes}"
GNOMAD_VCF_BASE_URL="${GNOMAD_VCF_BASE_URL:-https://hgdownload.soe.ucsc.edu/gbdb/hg38/gnomAD/v4/exomes}"
GNOMAD_FILE_PREFIX="${GNOMAD_FILE_PREFIX:-gnomad.exomes.v4.0.sites.chr}"
GNOMAD_FILE_SUFFIX="${GNOMAD_FILE_SUFFIX:-.vcf.bgz}"
GNOMAD_CHROM_LIST="${GNOMAD_CHROM_LIST:-1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 X Y}"

echo "[setup_evidence_runtime] EVIDENCE_ROOT=${EVIDENCE_ROOT}"
echo "[setup_evidence_runtime] INSTALL_DBSNP=${INSTALL_DBSNP}"
echo "[setup_evidence_runtime] INSTALL_CLINVAR=${INSTALL_CLINVAR}"
echo "[setup_evidence_runtime] INSTALL_GNOMAD=${INSTALL_GNOMAD}"

mkdir -p "${EVIDENCE_ROOT}"

if ! command -v tabix >/dev/null 2>&1; then
  echo "[setup_evidence_runtime] ERROR: tabix is required but not found in PATH."
  exit 1
fi

download_file_if_missing() {
  local url="$1"
  local destination="$2"
  if [ -f "${destination}" ]; then
    echo "[setup_evidence_runtime] Reusing existing file ${destination}"
    return 0
  fi
  mkdir -p "$(dirname "${destination}")"
  echo "[setup_evidence_runtime] Downloading ${url}"
  curl -fL --retry 5 --retry-delay 2 --retry-all-errors "${url}" -o "${destination}.part"
  mv "${destination}.part" "${destination}"
}

ensure_tabix_index() {
  local vcf_path="$1"
  local tbi_url="$2"
  if [ -f "${vcf_path}.tbi" ] || [ -f "${vcf_path}.csi" ]; then
    echo "[setup_evidence_runtime] Reusing existing index for ${vcf_path}"
    return 0
  fi

  if [ -n "${tbi_url}" ]; then
    if curl -fL --retry 3 --retry-delay 2 "${tbi_url}" -o "${vcf_path}.tbi"; then
      echo "[setup_evidence_runtime] Downloaded tabix index from ${tbi_url}"
      return 0
    fi
    echo "[setup_evidence_runtime] Could not download ${tbi_url}; falling back to local indexing"
  fi

  echo "[setup_evidence_runtime] Building tabix index for ${vcf_path}"
  tabix -f -p vcf "${vcf_path}"
}

if [ "${INSTALL_DBSNP}" = "1" ]; then
  download_file_if_missing "${DBSNP_VCF_URL}" "${DBSNP_LOCAL_VCF_PATH}"
  ensure_tabix_index "${DBSNP_LOCAL_VCF_PATH}" "${DBSNP_TBI_URL}"
fi

if [ "${INSTALL_CLINVAR}" = "1" ]; then
  download_file_if_missing "${CLINVAR_VCF_URL}" "${CLINVAR_LOCAL_VCF_PATH}"
  ensure_tabix_index "${CLINVAR_LOCAL_VCF_PATH}" "${CLINVAR_TBI_URL}"
fi

if [ "${INSTALL_GNOMAD}" = "1" ]; then
  mkdir -p "${GNOMAD_LOCAL_VCF_PATH}"
  for chrom in ${GNOMAD_CHROM_LIST}; do
    file_name="${GNOMAD_FILE_PREFIX}${chrom}${GNOMAD_FILE_SUFFIX}"
    file_url="${GNOMAD_VCF_BASE_URL}/${file_name}"
    file_path="${GNOMAD_LOCAL_VCF_PATH}/${file_name}"
    download_file_if_missing "${file_url}" "${file_path}"
    ensure_tabix_index "${file_path}" "${file_url}.tbi"
  done
fi

{
  echo "SP_DBSNP_LOCAL_VCF_PATH=${DBSNP_LOCAL_VCF_PATH}"
  echo "SP_CLINVAR_LOCAL_VCF_PATH=${CLINVAR_LOCAL_VCF_PATH}"
  if [ "${INSTALL_GNOMAD}" = "1" ]; then
    echo "SP_GNOMAD_LOCAL_VCF_PATH=${GNOMAD_LOCAL_VCF_PATH}"
  else
    echo "SP_GNOMAD_LOCAL_VCF_PATH="
  fi
  echo "SP_EVIDENCE_MODE=${SP_EVIDENCE_MODE:-online}"
} > "${EVIDENCE_ROOT}/evidence-manifest.env"

touch "${READY_MARKER}"
echo "[setup_evidence_runtime] Setup complete."
