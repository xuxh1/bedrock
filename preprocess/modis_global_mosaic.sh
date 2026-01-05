#!/bin/bash
# MOD16A2GF ET_500m: mosaic all tiles by AYYYYDDD -> global NC4 (DEFLATE ZLEVEL=3)
# Parallel by DOY (background job queue) + internal gdalwarp threading

set -euo pipefail

########################################
# USER CONFIG
########################################
RAW_DIR="/share/home/dq076/bedrock/data/ET/MOD16A2GF_v6.1/rawdata"

# 强烈建议：中间文件走本地盘（通常 /tmp 在本地 / 上）
WORK_ROOT="/share/home/dq076/bedrock/data/ET/MOD16A2GF_v6.1/mod16_work"
# 如果你确认 /tmp 不是本地，或空间不够，可改回 /share 的某个目录：
# WORK_ROOT="/share/home/dq076/bedrock/data/ET/MOD16A2GF_v6.1/_work"

OUT_DIR="/share/home/dq076/bedrock/data/ET/MOD16A2GF_v6.1/global_nc4"

PRODUCT="MOD16A2GF"
GRID_NAME="MOD_Grid_MOD16A2"
SDS_NAME="ET_500m"

RES_DEG="0.05"
RESAMPLE="near"           # ET 建议 near；如你明确要平均可改 average
SRC_NODATA="32767"        # 来自 HDF 的 _FillValue
DST_NODATA="-9999"        # 输出 nc4 的 NoData
ZLEVEL="3"                # zip_3
NUM_THREADS="6"           # 每个 gdalwarp 线程数（建议 4~6）
JOBS="6"                  # 同时跑多少个 DOY（建议 6~8）
########################################

mkdir -p "$WORK_ROOT" "$OUT_DIR"

# --- collect unique DOY keys (YYYYDDD) ---
echo "===> Collecting AYYYYDDD keys from filenames in: $RAW_DIR"

mapfile -t DOY_LIST < <(
  find "$RAW_DIR" -maxdepth 1 -type f -name "${PRODUCT}.A*.hdf" -printf "%f\n" \
  | sed -nE 's/^'"${PRODUCT}"'\.A([0-9]{7})\..*\.hdf$/\1/p' \
  | sort -u
)

if [[ ${#DOY_LIST[@]} -eq 0 ]]; then
  echo "[ERROR] No files matched: ${PRODUCT}.A*.hdf in $RAW_DIR" >&2
  exit 1
fi

echo "===> Found ${#DOY_LIST[@]} time slices"
echo "===> Parallel: JOBS=${JOBS}, gdalwarp NUM_THREADS=${NUM_THREADS}"

process_one_doy() {
  local DOY="$1"
  local KEY="A${DOY}"

  local DOY_WORK="${WORK_ROOT}/${KEY}"
  mkdir -p "$DOY_WORK"

  local OUT_NC="${OUT_DIR}/global_${PRODUCT}_ET_${KEY}_${RES_DEG}deg.nc4"
  if [[ -f "$OUT_NC" ]]; then
    echo "[SKIP] ${KEY} exists"
    rm -rf "$DOY_WORK"
    return 0
  fi

  # Build SDS list
  local LIST_TXT="${DOY_WORK}/inputs_et.txt"
  : > "$LIST_TXT"

  shopt -s nullglob
  local files=( "${RAW_DIR}/${PRODUCT}.${KEY}"*.hdf )
  shopt -u nullglob

  if [[ ${#files[@]} -eq 0 ]]; then
    echo "[WARN] ${KEY}: no tiles"
    rm -rf "$DOY_WORK"
    return 0
  fi

  for HDF in "${files[@]}"; do
    echo "HDF4_EOS:EOS_GRID:\"${HDF}\":${GRID_NAME}:${SDS_NAME}" >> "$LIST_TXT"
  done

  # Mosaic (Sinusoidal)
  local VRT="${DOY_WORK}/global_ET_${KEY}_sinu.vrt"
  gdalbuildvrt -overwrite -input_file_list "$LIST_TXT" "$VRT" >/dev/null

  # Warp to WGS84 and write NC4 w/ compression
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
    -of netCDF \
    -co "FORMAT=NC4" \
    -co "COMPRESS=DEFLATE" \
    -co "ZLEVEL=${ZLEVEL}" \
    "$VRT" \
    "$OUT_NC" >/dev/null

  echo "[OK] ${KEY} -> $(basename "$OUT_NC")"
  rm -rf "$DOY_WORK"
}

# --- simple job queue (max JOBS concurrent) ---
running=0
for DOY in "${DOY_LIST[@]}"; do
  process_one_doy "$DOY" &
  ((running+=1))
  if [[ "$running" -ge "$JOBS" ]]; then
    wait -n
    ((running-=1))
  fi
done
wait

echo "===> ALL DONE"
echo "Output dir: $OUT_DIR"
