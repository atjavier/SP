#!/usr/bin/env bash
set -euo pipefail

EVIDENCE_ROOT="${EVIDENCE_ROOT:-/opt/evidence}"
READY_MARKER="${EVIDENCE_ROOT}/.evidence-ready"

INSTALL_DBSNP="${INSTALL_DBSNP:-1}"
INSTALL_CLINVAR="${INSTALL_CLINVAR:-1}"

DBSNP_LOCAL_VCF_PATH="${DBSNP_LOCAL_VCF_PATH:-${EVIDENCE_ROOT}/dbsnp/dbsnp_all_grch38.vcf.gz}"
DBSNP_VCF_URL="${DBSNP_VCF_URL:-https://ftp.ncbi.nlm.nih.gov/snp/latest_release/VCF/GCF_000001405.40.gz}"
DBSNP_TBI_URL="${DBSNP_TBI_URL:-${DBSNP_VCF_URL}.tbi}"

CLINVAR_LOCAL_VCF_PATH="${CLINVAR_LOCAL_VCF_PATH:-${EVIDENCE_ROOT}/clinvar/clinvar_grch38.vcf.gz}"
CLINVAR_VCF_URL="${CLINVAR_VCF_URL:-https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz}"
CLINVAR_TBI_URL="${CLINVAR_TBI_URL:-${CLINVAR_VCF_URL}.tbi}"

echo "[setup_evidence_runtime] EVIDENCE_ROOT=${EVIDENCE_ROOT}"
echo "[setup_evidence_runtime] INSTALL_DBSNP=${INSTALL_DBSNP}"
echo "[setup_evidence_runtime] INSTALL_CLINVAR=${INSTALL_CLINVAR}"
echo "[setup_evidence_runtime] INSTALL_GNOMAD=0 (disabled by project decision; gnomAD remains online)"

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

{
  echo "SP_DBSNP_LOCAL_VCF_PATH=${DBSNP_LOCAL_VCF_PATH}"
  echo "SP_CLINVAR_LOCAL_VCF_PATH=${CLINVAR_LOCAL_VCF_PATH}"
  echo "SP_GNOMAD_LOCAL_VCF_PATH="
  echo "SP_GNOMAD_MODE=online"
} > "${EVIDENCE_ROOT}/evidence-manifest.env"

touch "${READY_MARKER}"
echo "[setup_evidence_runtime] Setup complete."
