#!/usr/bin/env bash
set -euo pipefail

SNPEFF_ROOT="${SNPEFF_ROOT:-/opt/snpeff}"
SNPEFF_HOME="${SNPEFF_HOME:-${SNPEFF_ROOT}/snpEff}"
SNPEFF_JAR_PATH="${SNPEFF_JAR_PATH:-${SNPEFF_HOME}/snpEff.jar}"
SNPEFF_ZIP_URL="${SNPEFF_ZIP_URL:-https://snpeff.blob.core.windows.net/versions/snpEff_latest_core.zip}"
SNPEFF_ZIP_FALLBACK_URL="${SNPEFF_ZIP_FALLBACK_URL:-https://sourceforge.net/projects/snpeff/files/snpEff_latest_core.zip/download}"
SNPEFF_GENOME="${SNPEFF_GENOME:-GRCh38.86}"
SNPEFF_DATA_DIR="${SNPEFF_DATA_DIR:-./data}"
SP_JAVA_CMD="${SP_JAVA_CMD:-java}"
SP_SNPEFF_JAVA_XMX="${SP_SNPEFF_JAVA_XMX:-2g}"
READY_MARKER="${SNPEFF_ROOT}/.snpeff-ready"

echo "[setup_snpeff_runtime] SNPEFF_ROOT=${SNPEFF_ROOT}"
echo "[setup_snpeff_runtime] SNPEFF_HOME=${SNPEFF_HOME}"
echo "[setup_snpeff_runtime] SNPEFF_GENOME=${SNPEFF_GENOME}"
echo "[setup_snpeff_runtime] SNPEFF_DATA_DIR=${SNPEFF_DATA_DIR}"

mkdir -p "${SNPEFF_ROOT}"

if [ -f "${READY_MARKER}" ] && [ -f "${SNPEFF_JAR_PATH}" ]; then
  if [ -f "${SNPEFF_HOME}/data/${SNPEFF_GENOME}/snpEffectPredictor.bin" ]; then
    echo "[setup_snpeff_runtime] Existing SnpEff runtime detected. Skipping setup."
    exit 0
  fi
fi

if [ ! -f "${SNPEFF_JAR_PATH}" ]; then
  echo "[setup_snpeff_runtime] Installing snpEff runtime from ${SNPEFF_ZIP_URL}"
  tmp_zip="${SNPEFF_ROOT}/snpEff_latest_core.zip"
  if ! curl -fL "${SNPEFF_ZIP_URL}" -o "${tmp_zip}"; then
    echo "[setup_snpeff_runtime] Primary URL failed, trying fallback ${SNPEFF_ZIP_FALLBACK_URL}"
    curl -fL "${SNPEFF_ZIP_FALLBACK_URL}" -o "${tmp_zip}"
  fi
  unzip -o "${tmp_zip}" -d "${SNPEFF_ROOT}"
fi

if [ ! -f "${SNPEFF_JAR_PATH}" ]; then
  echo "[setup_snpeff_runtime] ERROR: snpEff.jar not found at ${SNPEFF_JAR_PATH}"
  exit 1
fi

echo "[setup_snpeff_runtime] Ensuring SnpEff genome database ${SNPEFF_GENOME}"
(
  cd "${SNPEFF_HOME}"
  "${SP_JAVA_CMD}" "-Xmx${SP_SNPEFF_JAVA_XMX}" -jar "${SNPEFF_JAR_PATH}" download -v -dataDir "${SNPEFF_DATA_DIR}" "${SNPEFF_GENOME}"
)

if [ ! -f "${SNPEFF_HOME}/data/${SNPEFF_GENOME}/snpEffectPredictor.bin" ]; then
  echo "[setup_snpeff_runtime] ERROR: expected genome DB missing at ${SNPEFF_HOME}/data/${SNPEFF_GENOME}/snpEffectPredictor.bin"
  exit 1
fi

touch "${READY_MARKER}"
echo "[setup_snpeff_runtime] Setup complete."
