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
VEP_CACHE_TAR_URL="${VEP_CACHE_TAR_URL:-https://ftp.ensembl.org/pub/release-${VEP_RELEASE}/variation/indexed_vep_cache/${VEP_SPECIES}_vep_${VEP_CACHE_VERSION}.tar.gz}"
VEP_CACHE_TAR_PATH="${VEP_CACHE_TAR_PATH:-${VEP_ROOT}/.cache/${VEP_SPECIES}_vep_${VEP_CACHE_VERSION}.tar.gz}"
VEP_CACHE_TAR_DIR="${VEP_CACHE_SPECIES_DIR}/${VEP_SPECIES}_vep_${VEP_CACHE_VERSION}"
ALPHAMISSENSE_FILE_NAME="${ALPHAMISSENSE_FILE_NAME:-AlphaMissense_hg38.tsv.gz}"
ALPHAMISSENSE_URL="${ALPHAMISSENSE_URL:-https://storage.googleapis.com/dm_alphamissense/AlphaMissense_hg38.tsv.gz}"
ALPHAMISSENSE_FILE="${VEP_PLUGIN_DIR}/${ALPHAMISSENSE_FILE_NAME}"
READY_MARKER="${VEP_ROOT}/.vep-ready"
REQUIRED_API_FILE="${VEP_DIR}/Bio/EnsEMBL/Variation/DBSQL/VariationFeatureAdaptor.pm"
ARIA2C_CONN="${ARIA2C_CONN:-}"
ARIA2C_MIN_CONN="${ARIA2C_MIN_CONN:-4}"
ARIA2C_MAX_CONN="${ARIA2C_MAX_CONN:-16}"
ARIA2C_SEGMENT_SIZE="${ARIA2C_SEGMENT_SIZE:-1M}"

export HOME="${HOME:-/root}"
export PERL5LIB="${VEP_DIR}:${VEP_DIR}/modules:${PERL5LIB:-}"
export PATH="${VEP_DIR}/htslib:${PATH}"
export LD_LIBRARY_PATH="${VEP_DIR}/htslib:${LD_LIBRARY_PATH:-}"

echo "[setup_vep_runtime] VEP_ROOT=${VEP_ROOT}"
echo "[setup_vep_runtime] VEP_RELEASE=${VEP_RELEASE}"
echo "[setup_vep_runtime] VEP_CACHE_DIR=${VEP_CACHE_DIR}"
echo "[setup_vep_runtime] VEP_CACHE_VERSION_DIR=${VEP_CACHE_VERSION_DIR}"
echo "[setup_vep_runtime] VEP_PLUGIN_DIR=${VEP_PLUGIN_DIR}"
echo "[setup_vep_runtime] VEP_CACHE_TAR_URL=${VEP_CACHE_TAR_URL}"
echo "[setup_vep_runtime] ARIA2C_CONN=${ARIA2C_CONN:-auto} (min=${ARIA2C_MIN_CONN}, max=${ARIA2C_MAX_CONN}, segment=${ARIA2C_SEGMENT_SIZE})"

mkdir -p "${VEP_ROOT}" "${VEP_CACHE_DIR}" "${VEP_PLUGIN_DIR}" "${VEP_CACHE_SPECIES_DIR}" "$(dirname "${VEP_CACHE_TAR_PATH}")"

download_file_if_missing() {
  local url="$1"
  local destination="$2"
  if [ -f "${destination}" ]; then
    echo "[setup_vep_runtime] Reusing existing file ${destination}"
    return 0
  fi
  mkdir -p "$(dirname "${destination}")"
  echo "[setup_vep_runtime] Downloading ${url}"
  if command -v aria2c >/dev/null 2>&1; then
    local conn
    conn="$(choose_aria2c_connections "${url}")"
    aria2c -x "${conn}" -s "${conn}" -k "${ARIA2C_SEGMENT_SIZE}" -c --file-allocation=none \
      -d "$(dirname "${destination}")" -o "$(basename "${destination}").part" "${url}"
  else
    curl -fL --retry 5 --retry-delay 2 --retry-all-errors "${url}" -o "${destination}.part"
  fi
  mv "${destination}.part" "${destination}"
}

get_content_length() {
  local url="$1"
  curl -sIL "${url}" | awk 'tolower($1)=="content-length:" {print $2}' | tail -n 1 | tr -d '\r'
}

choose_aria2c_connections() {
  local url="$1"
  if [ -n "${ARIA2C_CONN}" ]; then
    echo "${ARIA2C_CONN}"
    return 0
  fi

  local length conn
  length="$(get_content_length "${url}")"
  if [ -n "${length}" ] && [ "${length}" -gt 0 ] 2>/dev/null; then
    if [ "${length}" -lt 536870912 ]; then
      conn=4
    elif [ "${length}" -lt 2147483648 ]; then
      conn=8
    elif [ "${length}" -lt 8589934592 ]; then
      conn=16
    else
      conn=16
    fi
  else
    conn=8
  fi

  if [ "${conn}" -lt "${ARIA2C_MIN_CONN}" ]; then
    conn="${ARIA2C_MIN_CONN}"
  fi
  if [ "${conn}" -gt "${ARIA2C_MAX_CONN}" ]; then
    conn="${ARIA2C_MAX_CONN}"
  fi

  echo "${conn}"
}

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

check_alphamissense_runtime() {
  local smoke_dir="${VEP_ROOT}/.smoke"
  local input_vcf="${smoke_dir}/alphamissense-smoke.vcf"
  local output_json="${smoke_dir}/alphamissense-smoke.jsonl"
  mkdir -p "${smoke_dir}"

  cat >"${input_vcf}" <<'EOF'
##fileformat=VCFv4.2
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
7	140753336	.	A	T	.	.	.
EOF

  perl "${VEP_DIR}/vep" \
    --input_file "${input_vcf}" \
    --output_file "${output_json}" \
    --format vcf \
    --json \
    --force_overwrite \
    --offline \
    --cache \
    --dir_cache "${VEP_CACHE_DIR}" \
    --assembly "${VEP_ASSEMBLY}" \
    --sift b \
    --polyphen b \
    --plugin "AlphaMissense,file=${ALPHAMISSENSE_FILE}" \
    --dir_plugins "${VEP_PLUGIN_DIR}" \
    >/tmp/sp_vep_alpha_smoke.stdout 2>/tmp/sp_vep_alpha_smoke.stderr || return 1

  if grep -Eq '"am_pathogenicity"|"alphamissense_score"|"am_class"|"alphamissense_class"' "${output_json}"; then
    return 0
  fi
  return 1
}

if check_vep_perl_runtime; then
  HAS_VEP_PERL_RUNTIME=1
else
  HAS_VEP_PERL_RUNTIME=0
fi
echo "[setup_vep_runtime] Perl runtime check available=${HAS_VEP_PERL_RUNTIME}"

if check_alphamissense_runtime; then
  HAS_ALPHAMISSENSE_RUNTIME=1
else
  HAS_ALPHAMISSENSE_RUNTIME=0
fi
echo "[setup_vep_runtime] AlphaMissense runtime check available=${HAS_ALPHAMISSENSE_RUNTIME}"

if [ -f "${READY_MARKER}" ] && [ -f "${VEP_DIR}/vep" ] && [ -f "${REQUIRED_API_FILE}" ] && [ -d "${VEP_CACHE_VERSION_DIR}" ] && [ -f "${ALPHAMISSENSE_FILE}" ] && [ -f "${ALPHAMISSENSE_FILE}.tbi" ] && [ "${HAS_VEP_PERL_RUNTIME}" -eq 1 ] && [ "${HAS_ALPHAMISSENSE_RUNTIME}" -eq 1 ]; then
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
  if command -v aria2c >/dev/null 2>&1; then
    download_file_if_missing "${VEP_CACHE_TAR_URL}" "${VEP_CACHE_TAR_PATH}"
    echo "[setup_vep_runtime] Extracting VEP cache tarball"
    tar -xzf "${VEP_CACHE_TAR_PATH}" -C "${VEP_CACHE_SPECIES_DIR}"
    if [ ! -d "${VEP_CACHE_VERSION_DIR}" ]; then
      if [ -d "${VEP_CACHE_TAR_DIR}" ]; then
        mv "${VEP_CACHE_TAR_DIR}" "${VEP_CACHE_VERSION_DIR}"
      elif [ -d "${VEP_CACHE_SPECIES_DIR}/${VEP_SPECIES}/${VEP_CACHE_VERSION}" ]; then
        mv "${VEP_CACHE_SPECIES_DIR}/${VEP_SPECIES}/${VEP_CACHE_VERSION}" "${VEP_CACHE_VERSION_DIR}"
      fi
    fi
    if [ ! -d "${VEP_CACHE_VERSION_DIR}" ]; then
      echo "[setup_vep_runtime] Cache directory still missing after tar extract; falling back to INSTALL.pl"
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
    fi
  else
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
  fi
else
  echo "[setup_vep_runtime] Cache directory already present at ${VEP_CACHE_VERSION_DIR}"
fi

if [ ! -f "${ALPHAMISSENSE_FILE}" ]; then
  echo "[setup_vep_runtime] Downloading AlphaMissense data from ${ALPHAMISSENSE_URL}"
  download_file_if_missing "${ALPHAMISSENSE_URL}" "${ALPHAMISSENSE_FILE}"
else
  echo "[setup_vep_runtime] AlphaMissense data already present at ${ALPHAMISSENSE_FILE}"
fi

if [ ! -f "${ALPHAMISSENSE_FILE}.tbi" ]; then
  echo "[setup_vep_runtime] Indexing AlphaMissense data with tabix"
  tabix -s 1 -b 2 -e 2 -S 1 -f "${ALPHAMISSENSE_FILE}"
else
  echo "[setup_vep_runtime] AlphaMissense tabix index already present"
fi

if ! check_alphamissense_runtime; then
  echo "[setup_vep_runtime] AlphaMissense smoke test failed; reinstalling plugin module"
  perl INSTALL.pl \
    --NO_UPDATE \
    --NO_TEST \
    --NO_HTSLIB \
    --AUTO p \
    --PLUGINS AlphaMissense \
    --DESTDIR "${VEP_DIR}" \
    --CACHEDIR "${VEP_CACHE_DIR}" \
    --PLUGINSDIR "${VEP_PLUGIN_DIR}"
fi

if ! check_alphamissense_runtime; then
  echo "[setup_vep_runtime] ERROR: AlphaMissense plugin runtime check failed"
  if [ -f /tmp/sp_vep_alpha_smoke.stderr ]; then
    echo "[setup_vep_runtime] --- alpha smoke stderr tail ---"
    tail -n 80 /tmp/sp_vep_alpha_smoke.stderr || true
  fi
  exit 1
fi

if [ ! -d "${VEP_CACHE_VERSION_DIR}" ]; then
  echo "[setup_vep_runtime] ERROR: expected cache directory missing: ${VEP_CACHE_VERSION_DIR}"
  exit 1
fi

touch "${READY_MARKER}"
echo "[setup_vep_runtime] Setup complete."
