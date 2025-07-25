# mosaic_overlap.py
import os
from typing import List, Tuple
import numpy as np
import rasterio
from rasterio.windows import Window
from rasterio.transform import from_bounds
from rasterio.enums import Resampling
from concurrent.futures import ThreadPoolExecutor, as_completed
from rtree import index
from osgeo import gdal
import gc
gdal.SetCacheMax(100 * 1024 * 1024)  # 100MB

# 类型映射
dtype_map = {
    "Byte": 'uint8', "Int16": 'int16', "UInt16": 'uint16',
    "Int32": 'int32', "UInt32": 'uint32',
    "Float32": 'float32', "Float64": 'float64'
}
resample_map = {
    "nearest": Resampling.nearest,
    "bilinear": Resampling.bilinear,
    "cubic": Resampling.cubic,
    "average": Resampling.average,
    "max": Resampling.max,
    "min": Resampling.min,
    "mode": Resampling.mode,
    "med": Resampling.med,
    "q1": Resampling.q1,
    "q3": Resampling.q3,
    "sum": Resampling.sum
}

def build_rtree_index(files: List[str]) -> Tuple[index.Index, List[str]]:
    rtree_idx = index.Index()
    paths = []
    for fid, f in enumerate(files):
        with rasterio.open(f) as src:
            bounds = src.bounds
            rtree_idx.insert(fid, (bounds.left, bounds.bottom, bounds.right, bounds.top))
            paths.append(f)
    return rtree_idx, paths

def process_window_rtree(rtree_idx, paths, out_win, out_transform, method, dst_nodata,dtype,resample):
    win_bounds = rasterio.windows.bounds(out_win, out_transform)
    h, w = out_win.height, out_win.width
    candidate_ids = list(rtree_idx.intersection(win_bounds))
    arrays = []

    if not candidate_ids:
        return np.full((1, h, w), dst_nodata, dtype=dtype)

    for fid in candidate_ids:
        with rasterio.open(paths[fid]) as src:
            try:
                src_window = src.window(*win_bounds).round_offsets().round_lengths()
                arr = src.read(window=src_window,
                               boundless=True,
                               fill_value=dst_nodata,
                               resampling=resample)
                arr = arr[:, :h, :w]
                arrays.append(arr)
            except Exception:
                continue

    if not arrays:
        return np.full((1, h, w), dst_nodata, dtype=dtype)

    stacked = np.stack(arrays, axis=0)  # shape: (n_files, bands, h, w)
    bands = stacked.shape[1]
    output = np.empty((bands, h, w), dtype=dtype)

    for b in range(bands):
        band_data = np.ma.masked_equal(stacked[:, b, :, :], dst_nodata)
        if method == 'mean':
            output[b] = np.ma.mean(band_data, axis=0).filled(dst_nodata)
        elif method == 'max':
            output[b] = np.ma.max(band_data, axis=0).filled(dst_nodata)
        elif method == 'min':
            output[b] = np.ma.min(band_data, axis=0).filled(dst_nodata)
        elif method == 'sum':
            output[b] = np.ma.sum(band_data, axis=0).filled(dst_nodata)
        else:
            raise ValueError(f"Unsupported method: {method}")

    del arrays
    del band_data
    del stacked
    gc.collect()
    return output

# 主函数
def mosaic_overlap(files: List[str],
                   out_path: str,
                   method: str = 'mean',
                   block_size: int = 512,
                   n_workers: int = 4,
                   dst_dtype: str = 'float32',
                   dst_nodata = None, # 输出 nodata 值
                   dst_crs = None,
                   driver: str = 'GTiff',
                   creation_options: List[str] = None,
                   resample: str = 'nearest',
                   flush_interval = 100,
                   log = None,
                   error = None,
                   thread_obj=None,
                   progress_cb=None):

    if creation_options is None:
        creation_options = ['COMPRESS=LZW', 'TILED=YES', 'BIGTIFF=YES']

    # 读取所有 bounds，获取输出范围和分辨率
    bounds_list = []
    resolutions = []
    for f in files:
        with rasterio.open(f) as src:
            bounds_list.append(src.bounds)
            resolutions.append(src.res)

    # 获取图幅边界
    left = min(b.left for b in bounds_list)
    bottom = min(b.bottom for b in bounds_list)
    right = max(b.right for b in bounds_list)
    top = max(b.top for b in bounds_list)

    # 使用最小分辨率（避免错位）
    res_x = min(r[0] for r in resolutions)
    res_y = min(r[1] for r in resolutions)
    width = int(round((right - left) / res_x))
    height = int(round((top - bottom) / res_y))
    print(left, bottom, right, top, res_x, res_y)
    print(width, height)
    transform = from_bounds(left, bottom, right, top, width, height)

    # 读取第一个文件，获取 bands 和 dtype、nodata 值、CRS
    with rasterio.open(files[0]) as ref:
        src_bands = ref.count
        src_dtype = ref.dtypes[0] # 第一个文件的数据类型
        src_nodata = ref.nodata  # 第一个文件的 nodata 值
        src_crs = ref.crs # 第一个文件的 CRS

    # 建立 R-tree 索引
    rtree_idx, paths = build_rtree_index(files)
    if log:
            log(f"索引建立完成")
    # 构造所有写入窗口
    windows = []
    for row in range(0, height, block_size):
        for col in range(0, width, block_size):
            win_w = min(block_size, width - col)
            win_h = min(block_size, height - row)
            windows.append(Window(col, row, win_w, win_h))

    total = len(windows)
    done = 0

    np_dtype = np.dtype(dtype_map.get(dst_dtype, src_dtype)) # 输出的值类型

    # 设定NODATA值
    dst_nodata = dst_nodata if dst_nodata is not None else src_nodata
    # 设定输出的 CRS
    dst_crs = dst_crs if dst_crs is not None else src_crs

    # 写入文件
    with rasterio.open(out_path, 'w',
                       driver=driver,
                       dtype=np_dtype.name, # 输出的数据类型
                       height=height,
                       width=width,
                       crs=dst_crs, # 输出的 CRS
                       transform=transform,
                       nodata=dst_nodata,
                       tiled=True,
                       blockxsize=block_size,
                       blockysize=block_size,
                       compress='lzw',
                       count=src_bands, # 输出的波段数
                       **{k.split('=')[0]: k.split('=')[1] for k in creation_options if '=' in k}) as dst:

        try:
            with ThreadPoolExecutor(max_workers=n_workers) as pool:
                future_map = {
                    pool.submit(process_window_rtree,
                                rtree_idx,
                                paths,
                                win,
                                transform,
                                method,
                                dst_nodata,
                                dtype_map.get(dst_dtype, src_dtype),
                                resample_map.get(resample)): win
                    for win in windows
                }

                write_count = 0
                for f in as_completed(future_map):
                    if thread_obj and thread_obj.isInterruptionRequested():
                        os._exit(1)

                    arr = f.result()
                    win = future_map.pop(f)
                    dst.write(arr, window=win)
                    del arr
                    del f
                    del win
                    gc.collect()
                    # 定期刷新缓存
                    write_count += 1
                    done += 1
                    if write_count % flush_interval == 0:
                        if log:
                            log(f"[flush] 已写入 {write_count} 块，刷新到磁盘")
                        gdal_ds = gdal.Open(out_path, gdal.GA_Update)
                        gdal_ds.FlushCache()
                        gdal_ds = None  # 强制关闭释放内存

                    if progress_cb:
                        progress_cb(int(done * 100 / total))

        except KeyboardInterrupt:
            if error:
                error("用户中断操作")
            raise
        except Exception as e:
            if error:
                error(f"处理过程中出现错误：{e}")
            raise