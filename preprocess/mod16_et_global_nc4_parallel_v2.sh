#!/bin/bash
# MOD16A2GF ET_500m: mosaic all tiles by AYYYYDDD -> global output
# Updated for new layout: rawdata/YYYY/DOY/*.hdf  (e.g., rawdata/2003/001/MOD16A2GF.A2003001....hdf)
#
# NOTE:
# 1) input list now writes RELATIVE paths (YYYY/DOY/filename.hdf), NOT absolute paths
# 2) gdalbuildvrt/gdalwarp are executed INSIDE RAW_DIR so relative paths resolve
# 3) Warp to GTiff then translate to NC4 (when netCDF driver is available)

set -euo pipefail

RAW_DIR="/share/home/dq076/bedrock/data/ET/MOD16A2GF_v6.1/rawdata"
WORK_ROOT="/tmp/${USER}/mod16_work"
OUT_DIR="/share/home/dq076/bedrock/data/ET/MOD16A2GF_v6.1/global_nc4"

PRODUCT="MOD16A2GF"
GRID_NAME="MOD_Grid_MOD16A2"
SDS_NAME="ET_500m"

RES_DEG="0.05"
RESAMPLE="near"
SRC_NODATA="32767"
DST_NODATA="-9999"
ZLEVEL="3"

JOBS="4"
NUM_THREADS="8"

# -----------------------
# Optional: generate DOY list by fixed range instead of scanning
# 0 = scan RAW_DIR to find AYYYYDDD keys (recommended)
# 1 = use fixed range START..END (2003/001 -> 2020/361 etc.)
# -----------------------
USE_FIXED_RANGE=0
START_YEAR=2003
START_DOY=1
END_YEAR=2020
END_DOY=361

# -----------------------
# STRONGLY RECOMMENDED ON HPC:
# ensure conda env + GDAL plugin/data paths are active inside batch jobs
# (comment out if you already source+activate outside)
# -----------------------
if [[ -f "/share/home/dq076/software/miniconda3/etc/profile.d/conda.sh" ]]; then
  # shellcheck disable=SC1091
  source "/share/home/dq076/software/miniconda3/etc/profile.d/conda.sh"
  conda activate gdal >/dev/null 2>&1 || true
  hash -r || true
fi

# Ensure GDAL can find plugins (libgdal-netcdf etc.) and data files in batch environment
if [[ -n "${CONDA_PREFIX:-}" ]]; then
  export GDAL_DRIVER_PATH="${CONDA_PREFIX}/lib/gdalplugins"
  export GDAL_DATA="${CONDA_PREFIX}/share/gdal"
  export PROJ_LIB="${CONDA_PREFIX}/share/proj"
fi

mkdir -p "$WORK_ROOT" "$OUT_DIR"

pad_doy() { printf "%03d" "$1"; }

# Compare (year,doy) <= (year2,doy2)
leq_yd() {
  local y1="$1" d1="$2" y2="$3" d2="$4"
  if (( y1 < y2 )); then return 0; fi
  if (( y1 > y2 )); then return 1; fi
  (( d1 <= d2 ))
}

echo "===> Parallel: JOBS=${JOBS}, gdalwarp NUM_THREADS=${NUM_THREADS}"

# STRONGLY RECOMMENDED: print actual GDAL binaries used (HPC PATH may differ in batch jobs)
echo "===> gdalinfo:      $(command -v gdalinfo)"
echo "===> gdal_translate: $(command -v gdal_translate)"
echo "===> gdalwarp:      $(command -v gdalwarp)"
echo "===> GDAL version:  $(gdalinfo --version 2>/dev/null || true)"

# check whether GDAL has netCDF driver (this is what matters for gdal_translate -of netCDF)
HAS_NC=$(gdalinfo --format netCDF >/dev/null 2>&1 && echo "YES" || echo "NO")
echo "===> GDAL netCDF driver available: ${HAS_NC}"

# ------------------------------------------------------------
# Build DOY_LIST (values like 2003001, 2003009 ... without leading 'A')
# ------------------------------------------------------------
if [[ "$USE_FIXED_RANGE" -eq 1 ]]; then
  echo "===> Using fixed range: ${START_YEAR}/$(pad_doy "${START_DOY}") -> ${END_YEAR}/$(pad_doy "${END_DOY}")"
  DOY_LIST=()
  for ((y=START_YEAR; y<=END_YEAR; y++)); do
    d_from=1; d_to=366
    (( y == START_YEAR )) && d_from="${START_DOY}"
    (( y == END_YEAR   )) && d_to="${END_DOY}"
    for ((d=d_from; d<=d_to; d++)); do
      if ! leq_yd "${y}" "${d}" "${END_YEAR}" "${END_DOY}"; then
        break
      fi
      DOY_LIST+=( "$(printf "%04d%03d" "$y" "$d")" )
    done
  done
else
  echo "===> Collecting AYYYYDDD keys from filenames under: $RAW_DIR/YYYY/DOY/"
  # Scan all HDF files and extract AYYYYDDD
  mapfile -t DOY_LIST < <(
    find "$RAW_DIR" -type f -name "${PRODUCT}.A*.hdf" -printf "%f\n" \
    | sed -nE 's/^'"${PRODUCT}"'\.A([0-9]{7})\..*\.hdf$/\1/p' \
    | sort -u
  )
fi

echo "===> Found ${#DOY_LIST[@]} time slices"

process_one_doy() {
  local DOY="$1"            # e.g. 2003001
  local KEY="A${DOY}"       # e.g. A2003001
  local YEAR="${DOY:0:4}"   # 2003
  local DOY3="${DOY:4:3}"   # 001
  local SUBDIR="${YEAR}/${DOY3}"

  local DOY_WORK="${WORK_ROOT}/${KEY}"
  mkdir -p "$DOY_WORK"

  local OUT_NC="${OUT_DIR}/global_${PRODUCT}_ET_${KEY}_${RES_DEG}deg.nc4"
  local OUT_TIF="${OUT_DIR}/global_${PRODUCT}_ET_${KEY}_${RES_DEG}deg.tif"

  if [[ -f "$OUT_NC" || -f "$OUT_TIF" ]]; then
    echo "[SKIP] ${KEY} exists"
    rm -rf "$DOY_WORK"
    return 0
  fi

  # Collect HDF tiles in rawdata/YYYY/DOY/
  shopt -s nullglob
  local files=( "${RAW_DIR}/${SUBDIR}/${PRODUCT}.${KEY}"*.hdf )
  shopt -u nullglob

  if [[ ${#files[@]} -eq 0 ]]; then
    echo "[WARN] ${KEY}: no tiles in ${SUBDIR}"
    rm -rf "$DOY_WORK"
    return 0
  fi

  echo "[RUN ] ${KEY}: dir=${SUBDIR} tiles=${#files[@]}"

  local LIST_TXT="${DOY_WORK}/inputs_et.txt"
  : > "$LIST_TXT"

  # IMPORTANT: write RELATIVE paths from RAW_DIR (so cd RAW_DIR can resolve them)
  for HDF in "${files[@]}"; do
    local base rel
    base=$(basename "$HDF")
    rel="${SUBDIR}/${base}"
    echo "HDF4_EOS:EOS_GRID:\"${rel}\":${GRID_NAME}:${SDS_NAME}" >> "$LIST_TXT"
  done

  local VRT="${DOY_WORK}/global_ET_${KEY}_sinu.vrt"

  (
    cd "$RAW_DIR"

    gdalbuildvrt -overwrite -input_file_list "$LIST_TXT" "$VRT"

    echo "[WARP] ${KEY}: writing $(basename "$OUT_TIF")"
    gdalwarp \
      -overwrite \
      -t_srs EPSG:4326 \
      -te -180 -90 180 90 \
      -tr "$RES_DEG" "$RES_DEG" \
      -tap \
      -r "$RESAMPLE" \
      -srcnodata "$SRC_NODATA" \
      -dstnodata "$DST_NODATA" \
      -multi -wo "NUM_THREADS=${NUM_THREADS}" \
      -ot Float32 \
      -of GTiff \
      -co "TILED=YES" \
      -co "COMPRESS=DEFLATE" \
      -co "ZLEVEL=${ZLEVEL}" \
      "$VRT" \
      "$OUT_TIF"
  )

  if [[ "$HAS_NC" == "YES" ]]; then
    echo "[NC  ] ${KEY}: translate to $(basename "$OUT_NC")"
    gdal_translate \
      -of netCDF \
      -co "FORMAT=NC4" \
      -co "COMPRESS=DEFLATE" \
      -co "ZLEVEL=${ZLEVEL}" \
      "$OUT_TIF" \
      "$OUT_NC" || {
        echo "[WARN] ${KEY}: gdal_translate to netCDF failed; keep GeoTIFF: $OUT_TIF"
        rm -rf "$DOY_WORK"
        return 0
      }
    rm -f "$OUT_TIF"
    echo "[OK ] ${KEY} -> $(basename "$OUT_NC")"
  else
    echo "[OK ] ${KEY} -> $(basename "$OUT_TIF") (no netCDF driver in GDAL)"
  fi

  rm -rf "$DOY_WORK"
}

# -----------------------
# STRONGLY RECOMMENDED:
# capture background job failures reliably (set -e doesn't cover '&' jobs)
# -----------------------
pids=()
for DOY in "${DOY_LIST[@]}"; do
  while [[ $(jobs -rp | wc -l) -ge "$JOBS" ]]; do
    sleep 1
  done
  process_one_doy "$DOY" &
  pids+=("$!")
done

fail=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    fail=1
  fi
done

(( fail == 0 )) || { echo "===> Some jobs failed."; exit 1; }

echo "===> ALL DONE"
echo "Output dir: $OUT_DIR"
