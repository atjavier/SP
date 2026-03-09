#!/usr/bin/env bash
set -euo pipefail

VEP_ROOT="${VEP_ROOT:-/opt/vep}"
VEP_RELEASE="${VEP_RELEASE:-115}"
VEP_SPECIES="${VEP_SPECIES:-homo_sapiens}"
VEP_ASSEMBLY="${VEP_ASSEMBLY:-GRCh38}"
VEP_CACHE_VERSION="${VEP_RELEASE}_${VEP_ASSEMBLY}"
VEP_DIR="${VEP_ROOT}/ensembl-vep"
VEP_CACHE_DIR="${VEP_CACHE_DIR:-${VEP_ROOT}/.vep}"
VEP_CACHE_SPECIES_DIR="${VEP_CACHE_DIR}/${VEP_SPECIES}"
VEP_CACHE_VERSION_DIR="${VEP_CACHE_SPECIES_DIR}/${VEP_CACHE_VERSION}"
VEP_PLUGIN_DIR="${VEP_PLUGIN_DIR:-${VEP_CACHE_DIR}/Plugins}"
ALPHAMISSENSE_FILE_NAME="${ALPHAMISSENSE_FILE_NAME:-AlphaMissense_hg38.tsv.gz}"
ALPHAMISSENSE_URL="${ALPHAMISSENSE_URL:-https://storage.googleapis.com/dm_alphamissense/AlphaMissense_hg38.tsv.gz}"
ALPHAMISSENSE_FILE="${VEP_PLUGIN_DIR}/${ALPHAMISSENSE_FILE_NAME}"
READY_MARKER="${VEP_ROOT}/.vep-ready"
REQUIRED_API_FILE="${VEP_DIR}/Bio/EnsEMBL/Variation/DBSQL/VariationFeatureAdaptor.pm"

export HOME="${HOME:-/root}"
export PERL5LIB="${VEP_DIR}:${VEP_DIR}/modules:${PERL5LIB:-}"
export PATH="${VEP_DIR}/htslib:${PATH}"
export LD_LIBRARY_PATH="${VEP_DIR}/htslib:${LD_LIBRARY_PATH:-}"

echo "[setup_vep_runtime] VEP_ROOT=${VEP_ROOT}"
echo "[setup_vep_runtime] VEP_RELEASE=${VEP_RELEASE}"
echo "[setup_vep_runtime] VEP_CACHE_DIR=${VEP_CACHE_DIR}"
echo "[setup_vep_runtime] VEP_CACHE_VERSION_DIR=${VEP_CACHE_VERSION_DIR}"
echo "[setup_vep_runtime] VEP_PLUGIN_DIR=${VEP_PLUGIN_DIR}"

mkdir -p "${VEP_ROOT}" "${VEP_CACHE_DIR}" "${VEP_PLUGIN_DIR}"

check_vep_perl_runtime() {
  perl \
    -I"${VEP_DIR}" \
    -I"${VEP_DIR}/modules" \
    -MBio::DB::HTS::Tabix \
    -MBio::EnsEMBL::Variation::DBSQL::VariationFeatureAdaptor \
    -MList::MoreUtils \
    -MLWP::Simple \
    -e 1 >/tmp/sp_vep_perl_check.log 2>&1
}

if check_vep_perl_runtime; then
  HAS_VEP_PERL_RUNTIME=1
else
  HAS_VEP_PERL_RUNTIME=0
fi
echo "[setup_vep_runtime] Perl runtime check available=${HAS_VEP_PERL_RUNTIME}"

if [ -f "${READY_MARKER}" ] && [ -f "${VEP_DIR}/vep" ] && [ -f "${REQUIRED_API_FILE}" ] && [ -d "${VEP_CACHE_VERSION_DIR}" ] && [ -f "${ALPHAMISSENSE_FILE}" ] && [ -f "${ALPHAMISSENSE_FILE}.tbi" ] && [ "${HAS_VEP_PERL_RUNTIME}" -eq 1 ]; then
  echo "[setup_vep_runtime] Existing VEP runtime detected. Skipping setup."
  exit 0
fi

if [ ! -d "${VEP_DIR}/.git" ]; then
  echo "[setup_vep_runtime] Cloning ensembl-vep release/${VEP_RELEASE}"
  git clone --depth 1 --branch "release/${VEP_RELEASE}" https://github.com/Ensembl/ensembl-vep.git "${VEP_DIR}"
else
  echo "[setup_vep_runtime] Reusing existing ${VEP_DIR}"
fi

cd "${VEP_DIR}"

if [ ! -f "${REQUIRED_API_FILE}" ] || [ ! -f "${VEP_PLUGIN_DIR}/AlphaMissense.pm" ] || [ "${HAS_VEP_PERL_RUNTIME}" -ne 1 ]; then
  if [ "${HAS_VEP_PERL_RUNTIME}" -ne 1 ]; then
    echo "[setup_vep_runtime] Perl runtime dependencies missing; reinstalling VEP API/runtime support"
  fi
  if [ ! -f "${REQUIRED_API_FILE}" ]; then
    echo "[setup_vep_runtime] Ensembl Variation API missing; reinstalling VEP API runtime"
  fi
  echo "[setup_vep_runtime] Installing VEP API + plugin runtime"
  perl INSTALL.pl \
    --NO_UPDATE \
    --NO_TEST \
    --AUTO ap \
    --USE_HTTPS_PROTO \
    --SPECIES "${VEP_SPECIES}" \
    --ASSEMBLY "${VEP_ASSEMBLY}" \
    --PLUGINS AlphaMissense \
    --DESTDIR "${VEP_DIR}" \
    --CACHEDIR "${VEP_CACHE_DIR}" \
    --PLUGINSDIR "${VEP_PLUGIN_DIR}"
else
  echo "[setup_vep_runtime] VEP API/plugin runtime already present"
fi

if ! check_vep_perl_runtime; then
  echo "[setup_vep_runtime] ERROR: required Perl runtime modules are unavailable after install"
  cat /tmp/sp_vep_perl_check.log
  exit 1
fi

if [ ! -f "${REQUIRED_API_FILE}" ]; then
  echo "[setup_vep_runtime] ERROR: required Ensembl Variation API file missing: ${REQUIRED_API_FILE}"
  exit 1
fi

if [ ! -d "${VEP_CACHE_VERSION_DIR}" ]; then
  echo "[setup_vep_runtime] Installing VEP cache ${VEP_SPECIES}/${VEP_CACHE_VERSION}"
  perl INSTALL.pl \
    --NO_UPDATE \
    --NO_TEST \
    --NO_HTSLIB \
    --AUTO c \
    --USE_HTTPS_PROTO \
    --SPECIES "${VEP_SPECIES}" \
    --ASSEMBLY "${VEP_ASSEMBLY}" \
    --DESTDIR "${VEP_DIR}" \
    --CACHEDIR "${VEP_CACHE_DIR}" \
    --PLUGINSDIR "${VEP_PLUGIN_DIR}"
else
  echo "[setup_vep_runtime] Cache directory already present at ${VEP_CACHE_VERSION_DIR}"
fi

if [ ! -f "${ALPHAMISSENSE_FILE}" ]; then
  echo "[setup_vep_runtime] Downloading AlphaMissense data from ${ALPHAMISSENSE_URL}"
  curl -fL "${ALPHAMISSENSE_URL}" -o "${ALPHAMISSENSE_FILE}"
else
  echo "[setup_vep_runtime] AlphaMissense data already present at ${ALPHAMISSENSE_FILE}"
fi

if [ ! -f "${ALPHAMISSENSE_FILE}.tbi" ]; then
  echo "[setup_vep_runtime] Indexing AlphaMissense data with tabix"
  tabix -s 1 -b 2 -e 2 -S 1 -f "${ALPHAMISSENSE_FILE}"
else
  echo "[setup_vep_runtime] AlphaMissense tabix index already present"
fi

if [ ! -d "${VEP_CACHE_VERSION_DIR}" ]; then
  echo "[setup_vep_runtime] ERROR: expected cache directory missing: ${VEP_CACHE_VERSION_DIR}"
  exit 1
fi

touch "${READY_MARKER}"
echo "[setup_vep_runtime] Setup complete."
