import subprocess
from pathlib import Path

ROOT_DIR = Path("/tera04/zhwei/xionghui/bedrock/data").resolve()
# DATA_DIR = ROOT_DIR / "P/MSWEP_V280"
# cmd = ["cdo", "-O", "-L", "-b", "F32", 
#        "-f", "nc4", "-z", "zip_3", 
#        "-setattribute,precipitation@units='mm 8d-1'", 
#        DATA_DIR/"P_MSWEP_2003-2020_8D_p05_mm8d-1.nc", 
#        DATA_DIR/"P_MSWEP_2003-2020_8D_p05_mm8d-1_.nc",]
DATA_DIR = ROOT_DIR / "P/MSWEP_V280"
cmd = ["cdo", "-O", "-L", "-b", "F32", 
       "-f", "nc4", "-z", "zip_3", 
       "-setattribute,precipitation@units='mm 8d-1'", 
       DATA_DIR/"P_MSWEP_2003-2020_8D_p05_mm8d-1.nc", 
       DATA_DIR/"P_MSWEP_2003-2020_8D_p05_mm8d-1_.nc",]
p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)