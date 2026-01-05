import os
import re
import shlex
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import pandas as pd
import xarray as xr
import numpy as np


# ----------------------------
# CONFIG
# ----------------------------
PAT = re.compile(
    r"MOD16A2GF\.A(?P<year>\d{4})(?P<doy>\d{3})\.(?P<tile>h\d{2}v\d{2})\.(?P<col>\d{3})\.(?P<prod>\d+)\.hdf$"
)

GRID_NAME = "MOD_Grid_MOD16A2"   # confirmed by your gdalinfo SDS path
SDS_NAME = "ET_500m"

# EPSG:4326 target resolution in degrees (approx ~500m at equator)
TR_DEG = 0.005

# fallback constants (you verified by gdalinfo on the SDS)
FALLBACK_SCALE = 0.1
FALLBACK_FILL = 32767

# Compression settings
NC_COMPRESS = {"FORMAT": "NC4", "COMPRESS": "DEFLATE", "ZLEVEL": "1"}


def run(cmd):
    print("[CMD]", " ".join(shlex.quote(c) for c in cmd), flush=True)
    subprocess.run(cmd, check=True)


def parse_mod16_name(p: Path):
    m = PAT.match(p.name)
    if not m:
        return None
    year = int(m.group("year"))
    doy = int(m.group("doy"))
    date = datetime(year, 1, 1) + timedelta(days=doy - 1)
    return {
        "path": str(p),
        "year": year,
        "doy": doy,
        "date": date.strftime("%Y-%m-%d"),
        "datestr": f"A{year}{doy:03d}",  # A2020361
        "tile": m.group("tile"),
        "collection": m.group("col"),
        "production": m.group("prod"),
    }


def build_index(root_dir: str, out_csv: str):
    root = Path(root_dir)
    files = list(root.rglob("MOD16A2GF.A*.hdf"))

    rows = []
    for f in files:
        info = parse_mod16_name(f)
        if info:
            rows.append(info)

    df = pd.DataFrame(rows).sort_values(["datestr", "tile"])
    df.to_csv(out_csv, index=False)
    print(f"[INDEX] Saved {len(df)} rows to {out_csv}", flush=True)
    return df


def sds_path(hdf_path: str) -> str:
    return f'HDF4_EOS:EOS_GRID:"{hdf_path}":{GRID_NAME}:{SDS_NAME}'


def apply_fill_and_scale(in_nc: Path, out_nc: Path):
    """
    Read GDAL-produced nc, mask FillValue, apply scale_factor, write analysis-ready nc.
    """
    ds = xr.open_dataset(in_nc)

    if SDS_NAME not in ds:
        # Some GDAL versions may name variable differently; try the first data var
        data_vars = list(ds.data_vars)
        raise KeyError(f"Cannot find variable {SDS_NAME}. Available: {data_vars}")

    da = ds[SDS_NAME]

    scale = da.attrs.get("scale_factor", FALLBACK_SCALE)
    fill = da.attrs.get("_FillValue", None)

    # GDAL sometimes stores nodata as missing_value, or only via encoding
    if fill is None:
        fill = da.attrs.get("missing_value", None)
    if fill is None:
        fill = da.encoding.get("_FillValue", None)
    if fill is None:
        fill = FALLBACK_FILL

    da = da.where(da != fill)
    da = da.astype("float32") * float(scale)

    # Update attrs: remove packed encoding keys
    da.attrs.update({
        "units": "mm/8day",  # kg/m^2 == mm water
        "long_name": "Evapotranspiration (MOD16A2GF v6.1), scaled and masked",
        "source_product": "MOD16A2GF v6.1",
        "scale_applied": float(scale),
        "fillvalue_masked": int(fill),
    })
    for k in ["scale_factor", "add_offset", "_FillValue", "missing_value", "valid_range"]:
        da.attrs.pop(k, None)

    ds[SDS_NAME] = da

    # Write compact nc4
    encoding = {
        SDS_NAME: {
            "dtype": "float32",
            "zlib": True,
            "complevel": 1,
            "_FillValue": np.nan,
        }
    }
    out_nc.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(out_nc, encoding=encoding)
    ds.close()


def process_one_datestr(datestr: str, g: pd.DataFrame, out_dir: str, tmp_root: str):
    """
    For one 8-day timestamp:
    1) mosaic tiles to VRT (Sinusoidal)
    2) translate to temporary Sinusoidal nc4 (Float32, zlevel=1)
    3) warp to EPSG:4326 nc4 (Float32, zlevel=1)
    4) mask FillValue + apply scale_factor -> final *_phys.nc4
    """
    out_dir = Path(out_dir)
    tmp_root = Path(tmp_root)

    out_dir.mkdir(parents=True, exist_ok=True)
    tmp_root.mkdir(parents=True, exist_ok=True)

    # Final product (analysis-ready)
    out_phys = out_dir / f"MOD16A2GF_{datestr}_{SDS_NAME}_global_epsg4326_phys.nc"
    if out_phys.exists():
        return f"[SKIP] {datestr} phys exists"

    # Intermediate (GDAL warped but not scaled/masked)
    out_warp = out_dir / f"MOD16A2GF_{datestr}_{SDS_NAME}_global_epsg4326_raw.nc"
    # Keep raw optionally; if you don't want it, write to tmp and delete later.

    tmp_dir = tmp_root / datestr
    tmp_dir.mkdir(parents=True, exist_ok=True)

    vrt_sinu = tmp_dir / f"{datestr}_{SDS_NAME}_sinu.vrt"
    nc_sinu = tmp_dir / f"{datestr}_{SDS_NAME}_sinu.nc"

    # 1) Mosaic tiles -> VRT
    inputs = [sds_path(p) for p in g["path"].tolist()]
    cmd_vrt = ["gdalbuildvrt", "-overwrite", "-resolution", "highest", str(vrt_sinu)] + inputs
    run(cmd_vrt)

    # 2) VRT -> Sinusoidal NetCDF (Float32 + NC4 + ZLEVEL=1)
    cmd_nc = [
        "gdal_translate",
        "-of", "netCDF",
        "-ot", "Float32",
        str(vrt_sinu),
        str(nc_sinu),
        "-co", f"FORMAT={NC_COMPRESS['FORMAT']}",
        "-co", f"COMPRESS={NC_COMPRESS['COMPRESS']}",
        "-co", f"ZLEVEL={NC_COMPRESS['ZLEVEL']}",
    ]
    run(cmd_nc)

    # 3) Reproject -> EPSG:4326 NetCDF (Float32 + NC4 + ZLEVEL=1)
    cmd_warp = [
        "gdalwarp",
        "-overwrite",
        "-t_srs", "EPSG:4326",
        "-te", "-180", "-90", "180", "90",
        "-tr", str(TR_DEG), str(TR_DEG),
        "-tap",
        "-r", "bilinear",
        "-multi",
        "-wo", "NUM_THREADS=4",
        "-ot", "Float32",
        "-of", "netCDF",
        str(nc_sinu),
        str(out_warp),
        "-co", f"FORMAT={NC_COMPRESS['FORMAT']}",
        "-co", f"COMPRESS={NC_COMPRESS['COMPRESS']}",
        "-co", f"ZLEVEL={NC_COMPRESS['ZLEVEL']}",
    ]
    run(cmd_warp)

    # 4) Mask FillValue + apply scale_factor -> final phys nc
    apply_fill_and_scale(out_warp, out_phys)

    # Optional cleanup: keep warped raw or delete to save space
    # If you don't need raw, uncomment:
    # try: out_warp.unlink(missing_ok=True)
    # except Exception: pass

    # Cleanup temp
    try:
        vrt_sinu.unlink(missing_ok=True)
        nc_sinu.unlink(missing_ok=True)
        tmp_dir.rmdir()
    except Exception:
        pass

    return f"[OK] {datestr} -> {out_phys.name}"


def get_available_cpus():
    # Better than os.cpu_count() in SLURM
    for var in ("SLURM_CPUS_PER_TASK", "SLURM_CPUS_ON_NODE", "PBS_NP"):
        v = os.environ.get(var)
        if v:
            return int(str(v).split("(")[0])
    return os.cpu_count() or 1


def main(root_dir: str, out_dir: str, tmp_root: str, index_csv: str, max_workers: int = 4, limit_dates=None):
    index_csv = str(Path(index_csv).resolve())

    # Build CSV if missing
    if not Path(index_csv).exists():
        build_index(root_dir, index_csv)

    df = pd.read_csv(index_csv)
    groups = list(df.groupby("datestr"))
    groups.sort(key=lambda x: x[0])

    if limit_dates is not None:
        groups = groups[:limit_dates]

    print(f"[RUN] timestamps={len(groups)} | max_workers={max_workers}", flush=True)

    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futs = [
            ex.submit(process_one_datestr, datestr, g, out_dir, tmp_root)
            for datestr, g in groups
        ]
        for f in as_completed(futs):
            print(f.result(), flush=True)


if __name__ == "__main__":
    root_dir = "/share/home/dq076/bedrock/data/ET/MOD16A2GF_v6.1/rawdata"
    out_dir = "/share/home/dq076/bedrock/data/ET/MOD16A2GF_v6.1/global_epsg4326"
    tmp_root = "/share/home/dq076/bedrock/data/ET/MOD16A2GF_v6.1/_tmp"
    index_csv = "/share/home/dq076/bedrock/data/ET/MOD16A2GF_v6.1/rawdata/mod16a2gf_index.csv"

    # I/O-heavy: start small
    max_workers = min(4, get_available_cpus())

    # For a quick test, set limit_dates=2
    main(root_dir, out_dir, tmp_root, index_csv, max_workers=max_workers, limit_dates=None)
