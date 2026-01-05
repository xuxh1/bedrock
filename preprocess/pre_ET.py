"""
MODIS蒸散发数据处理脚本（简化版）
将HDF格式数据转换为0.05度分辨率的NetCDF文件
"""

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


def run_shell_command(command):
    """运行shell命令，如果失败则打印错误信息"""
    # 打印执行的命令
    print(f"[执行] {' '.join(shlex.quote(str(arg)) for arg in command)}")
    
    # 运行命令
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # 检查是否成功
    if result.returncode != 0:
        print(f"[错误] 命令执行失败:\n{result.stderr[:2000]}")
        raise subprocess.CalledProcessError(result.returncode, command)
    
    return result.stdout


def get_hdf_info(file_path):
    """从HDF文件名中提取信息"""
    # 文件名格式示例：MOD16A2GF.A2021001.h10v05.061.2022293211312.hdf
    pattern = r"MOD16A2GF\.A(\d{4})(\d{3})\.(h\d{2}v\d{2})\.(\d{3})\.(\d+)\.hdf$"
    
    match = re.match(pattern, file_path.name)
    if not match:
        return None
    
    year = int(match.group(1))
    day_of_year = int(match.group(2))
    tile_id = match.group(3)
    
    # 计算具体日期
    start_date = datetime(year, 1, 1)
    file_date = start_date + timedelta(days=day_of_year - 1)
    
    return {
        "path": str(file_path),
        "year": year,
        "doy": day_of_year,
        "date": file_date.strftime("%Y-%m-%d"),
        "datestr": f"A{year}{day_of_year:03d}",
        "tile": tile_id,
        "collection": match.group(4),
        "production": match.group(5),
    }


def get_gdal_hdf_path(hdf_file_path):
    """构建GDAL能够识别的HDF文件路径格式"""
    # 从完整路径中提取文件名
    hdf_filename = Path(hdf_file_path).name
    # 正确的GDAL HDF格式：EOS_GRID:"文件名":MOD_Grid_MOD16A2:ET_500m
    return f'HDF4_EOS:EOS_GRID:"{hdf_filename}":MOD_Grid_MOD16A2:ET_500m'


def process_one_date(datestr, date_files, output_dir, temp_dir):
    """处理单个日期的所有文件"""
    
    # 准备输出文件路径
    output_dir = Path(output_dir)
    temp_dir = Path(temp_dir) / datestr
    
    # 创建目录
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    # 定义输出文件名
    raw_output = output_dir / f"MOD16A2GF_{datestr}_ET_500m_0p05deg_raw.nc"
    final_output = output_dir / f"MOD16A2GF_{datestr}_ET_500m_0p05deg_phys.nc"
    
    # 如果最终文件已存在，跳过处理
    if final_output.exists():
        return f"[跳过] {datestr}"
    
    # 获取原始数据目录
    raw_data_dir = Path(date_files["path"].iloc[0]).parent
    print(f"处理目录: {raw_data_dir}")
    
    # 步骤1: 创建输入文件列表
    input_list = temp_dir / "file_list.txt"
    with open(input_list, "w") as f:
        for hdf_file in date_files["path"].tolist():
            # 构建GDAL能够识别的路径
            gdal_path = get_gdal_hdf_path(hdf_file)
            f.write(gdal_path + "\n")
    
    # 步骤2: 创建虚拟镶嵌文件
    vrt_file = temp_dir / f"{datestr}_ET_500m.vrt"
    
    # 在执行gdalbuildvrt之前，切换到HDF文件所在的目录
    # 这样GDAL才能正确找到文件
    original_dir = os.getcwd()  # 保存当前工作目录
    try:
        os.chdir(raw_data_dir)  # 切换到HDF文件目录
        print(f"切换到目录: {raw_data_dir}")
        
        # 构建相对路径到临时文件列表
        relative_input_list = Path(temp_dir).relative_to(raw_data_dir) / "file_list.txt"
        relative_vrt_file = Path(temp_dir).relative_to(raw_data_dir) / f"{datestr}_ET_500m.vrt"
        
        run_shell_command([
            "gdalbuildvrt",
            "-overwrite",
            "-input_file_list", str(relative_input_list),
            str(relative_vrt_file)
        ])
    finally:
        os.chdir(original_dir)  # 切换回原始目录
    
    # 步骤3: 重投影和重采样到0.05度分辨率
    # 注意：这里使用临时目录中的VRT文件的绝对路径
    run_shell_command([
        "gdalwarp",
        "-overwrite",
        "-t_srs", "EPSG:4326",           # 转换为经纬度坐标
        "-te", "-180", "-90", "180", "90",  # 全球范围
        "-tr", "0.05", "0.05",          # 0.05度分辨率
        "-tap",                         # 对齐到网格
        "-r", "average",                # 使用平均法重采样
        "-multi",                       # 使用多线程
        "-wo", "NUM_THREADS=4",         # 4个工作线程
        "-ot", "Float32",               # 输出32位浮点数
        "-of", "netCDF",                # 输出NetCDF格式
        str(vrt_file),                  # 输入VRT文件（使用绝对路径）
        str(raw_output),                # 输出文件
        
        # 压缩设置
        "-co", "FORMAT=NC4",
        "-co", "COMPRESS=DEFLATE",
        "-co", "ZLEVEL=3",
    ])
    
    # 步骤4: 处理数据值（应用填充值和缩放因子）
    # 打开NetCDF文件
    dataset = xr.open_dataset(raw_output)
    
    # 获取数据
    data = dataset["ET_500m"]
    
    # 获取缩放因子，如果没有则使用0.1
    scale = data.attrs.get("scale_factor", 0.1)
    
    # 获取填充值
    fill_value = (
        data.attrs.get("_FillValue") or
        data.attrs.get("missing_value") or
        data.encoding.get("_FillValue") or
        32767  # 默认值
    )
    
    # 处理数据
    # 1. 将填充值替换为NaN
    # 2. 应用缩放因子
    data = data.where(data != fill_value)
    data = data.astype("float32") * float(scale)
    
    # 更新属性
    data.attrs.update({
        "units": "mm/8day",
        "long_name": "Evapotranspiration (MOD16A2GF), upscaled to 0.05deg",
        "scale_applied": float(scale),
        "fillvalue_masked": int(fill_value),
    })
    
    # 移除不需要的属性
    for attr in ["scale_factor", "add_offset", "_FillValue", "missing_value", "valid_range"]:
        data.attrs.pop(attr, None)
    
    # 保存回数据集
    dataset["ET_500m"] = data
    
    # 配置输出
    encoding = {
        "ET_500m": {
            "dtype": "float32",
            "zlib": True,
            "complevel": 1,
            "_FillValue": np.nan,
        }
    }
    
    # 保存处理后的文件
    dataset.to_netcdf(final_output, encoding=encoding)
    dataset.close()
    
    # 清理临时文件
    try:
        input_list.unlink(missing_ok=True)
        vrt_file.unlink(missing_ok=True)
        temp_dir.rmdir()
    except:
        pass
    
    return f"[完成] {datestr}"


def scan_hdf_files(data_dir, index_file):
    """扫描目录中的HDF文件，创建索引"""
    
    print(f"正在扫描目录: {data_dir}")
    
    # 查找所有HDF文件
    hdf_files = []
    for file_path in Path(data_dir).rglob("MOD16A2GF.A*.hdf"):
        hdf_files.append(file_path)
    
    print(f"找到 {len(hdf_files)} 个HDF文件")
    
    # 提取文件信息
    file_info_list = []
    for file_path in hdf_files:
        info = get_hdf_info(file_path)
        if info:
            file_info_list.append(info)
    
    # 创建数据框并排序
    file_df = pd.DataFrame(file_info_list)
    file_df = file_df.sort_values(["datestr", "tile"])
    
    # 保存到CSV
    file_df.to_csv(index_file, index=False)
    print(f"索引已保存到: {index_file}")
    
    return file_df


def main():
    """主函数"""
    
    # 配置路径
    raw_data_dir = "/share/home/dq076/bedrock/data/ET/MOD16A2GF_v6.1/rawdata"
    output_dir = "/share/home/dq076/bedrock/data/ET/MOD16A2GF_v6.1/p05"
    temp_dir = "/share/home/dq076/bedrock/data/ET/MOD16A2GF_v6.1/_tmp"
    index_file = "/share/home/dq076/bedrock/data/ET/MOD16A2GF_v6.1/mod16a2gf_index.csv"
    
    # 创建必要目录
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    Path(temp_dir).mkdir(parents=True, exist_ok=True)
    
    # 步骤1: 扫描文件并创建索引
    if not Path(index_file).exists():
        print("索引文件不存在，正在创建...")
        file_df = scan_hdf_files(raw_data_dir, index_file)
    else:
        print("读取已有索引文件...")
        file_df = pd.read_csv(index_file)
    
    print(f"总共找到 {len(file_df)} 条文件记录")
    
    # 步骤2: 按日期分组
    date_groups = []
    for datestr, group in file_df.groupby("datestr"):
        date_groups.append((datestr, group))
    
    # 按日期排序
    date_groups.sort(key=lambda x: x[0])
    
    # 测试模式：只处理前2个日期
    test_mode = True
    if test_mode:
        date_groups = date_groups[:2]
        print(f"测试模式：只处理前 {len(date_groups)} 个日期")
    
    print(f"准备处理 {len(date_groups)} 个日期")
    
    # 步骤3: 并行处理
    # 确定使用的进程数（不超过4个）
    max_workers = 4
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        tasks = []
        for datestr, date_files in date_groups:
            task = executor.submit(
                process_one_date,
                datestr,
                date_files,
                output_dir,
                temp_dir
            )
            tasks.append(task)
        
        # 收集结果
        for task in as_completed(tasks):
            try:
                result = task.result()
                print(result)
            except Exception as e:
                print(f"[错误] 处理失败: {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("MODIS数据处理器 - 简化版")
    print("=" * 60)
    
    main()
    
    print("=" * 60)
    print("处理完成!")
    print("=" * 60)