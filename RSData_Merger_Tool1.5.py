# MergerUI.py
import os, sys, traceback, glob, shutil
from PyQt5.QtCore import QThread, pyqtSignal
from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout, QHBoxLayout,
                             QPushButton, QLineEdit, QTextEdit, QFileDialog,
                             QLabel, QProgressBar, QCheckBox, QComboBox)
from PyQt5.QtGui import QIcon
from osgeo import gdal
from mosaic_overlap import mosaic_overlap
import os
import signal

def signal_handler(signum, frame):
    print(f"收到信号 {signum}，强制退出进程")
    os._exit(1)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ---------- MergeThread ----------
class MergeThread(QThread):
    log = pyqtSignal(str)
    error = pyqtSignal(str)
    progress = pyqtSignal(int)

    def __init__(self, files, out_path, opts, merge_method):
        super().__init__()
        self.files = files
        self.out_path = out_path
        self.method = merge_method
        self.opts = opts

    def run(self):
        try:
            self.log.emit(f"找到 {len(self.files)} 个文件")
            mosaic_overlap(
                files=self.files,
                out_path=self.out_path,
                method=self.method,
                block_size=self.opts.get('block_size'), # 分块大小
                n_workers=self.opts.get('n_workers', 2), # 线程数
                dst_dtype=self.opts.get('dst_dtype'),
                dst_nodata=self.opts.get('dstNodata'),
                dst_crs=self.opts.get('dstSRS'),
                creation_options=self.opts.get('creationOptions', ['COMPRESS=LZW', 'TILED=YES']),
                # resample=self.opts.get('resample', 'nearest'),
                flush_interval = self.opts.get('flush_interval', 100),
                log = self.log.emit,  # 日志回调
                error = self.error.emit,  # 错误回调
                thread_obj=self,  # 把线程自身传进去
                progress_cb=lambda pct: self.progress.emit(pct)
            )
            self.log.emit(f"✅ 合并完成：{self.out_path}")
        except InterruptedError as e:
            self.log.emit(str(e))
        except Exception as e:
            self.error.emit(f"合并失败: {str(e)}")
            self.error.emit(traceback.format_exc())

# ---------- HDFMergeThread ----------
class HDFMergeThread(QThread):
    log = pyqtSignal(str)
    error = pyqtSignal(str)
    progress = pyqtSignal(int)

    def __init__(self, hdf_files, out_path, opts, temp_dir, subdataset_index, merge_method):
        super().__init__()
        self.hdf_files = hdf_files
        self.out_path = out_path
        self.temp_dir = temp_dir
        self.subdataset_index = subdataset_index
        self.method = merge_method
        self.opts = opts

    def run(self):
        try:
            if not self.hdf_files:
                raise RuntimeError("未找到任何 .hdf/nc 文件")
            os.makedirs(self.temp_dir, exist_ok=True)
            # 1. 提取子集到临时 TIF
            self.log.emit(f"提取子集 {self.subdataset_index} 到临时目录 {self.temp_dir}")
            for hdf in self.hdf_files:
                ds = gdal.Open(hdf)
                sd = ds.GetSubDatasets()[self.subdataset_index]
                print(sd)
                out_tif = os.path.join(self.temp_dir, f"{os.path.basename(hdf)}_{sd[1]}.tif")
                gdal.Warp(out_tif, sd[0], format='GTiff')
                ds = None
            # 2. 合并
            tifs = glob.glob(os.path.join(self.temp_dir, '*.tif'))
            mosaic_overlap(
                files=tifs,
                out_path=self.out_path,
                method=self.method,
                block_size=self.opts.get('block_size'), # 分块大小
                n_workers=self.opts.get('n_workers', 2), # 线程数
                dst_dtype=self.opts.get('dst_dtype'),
                dst_nodata=self.opts.get('dstNodata'),
                dst_crs=self.opts.get('dstSRS'),
                creation_options=self.opts.get('creationOptions', ['COMPRESS=LZW', 'TILED=YES']),
                # resample=self.opts.get('resample', 'nearest'),
                flush_interval = self.opts.get('flush_interval', 100),
                log = self.log.emit,  # 日志回调
                error = self.error.emit,  # 错误回调
                thread_obj=self,  # 把线程自身传进去
                progress_cb=lambda pct: self.progress.emit(pct)
            )
            self.log.emit("✅ HDF 子数据集合并完成")
        except Exception as e:
            self.error.emit(f"HDF 合并失败: {str(e)}")
            self.error.emit(traceback.format_exc())
        # finally:
            # if os.path.exists(self.temp_dir):
            #     shutil.rmtree(self.temp_dir)

# ---------- 主界面 ----------
class MergerUI(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("遥感影像拼接工具 v1.5 By LMQ")
        self.resize(600, 500)
        self.setWindowIcon(QIcon(self._get_resource_path('app_icon.ico')))
        # ---------- 控件 ----------
        v = QVBoxLayout(self)

        # 输入目录
        h1 = QHBoxLayout()
        h1.addWidget(QLabel('<font color="red">*</font>输入影像目录:'))
        self.le_in_dir = QLineEdit()
        self.le_in_dir.setToolTip(
            "默认检索以下类型的文件\n"
            "'*.tif', '*.tiff', '*.asc',\n"
            "'*.img', '*.nc', '*.hdf'\n"
            "gdal支持多种类型的影像格式\n"
            "可以自定义输入扩展名\n"
            "更多格式暂未测试\n"
            "asc tif hdf 已测试"
        )
        self.le_in_dir.setMouseTracking(True)   # 关键
        h1.addWidget(self.le_in_dir)
        btn_in = QPushButton("浏览")
        btn_in.clicked.connect(lambda: self.browse_dir(self.le_in_dir))
        h1.addWidget(btn_in)
        v.addLayout(h1)

        # 1) 文件类型下拉框
        h_type = QHBoxLayout()
        h_type.addWidget(QLabel("文件类型:"))
        self.cb_file_type = QComboBox()
        self.file_types = ['tif', 'tiff', 'asc',  'hdf', 'img', 'nc','png','自定义']
        self.cb_file_type.addItems(self.file_types)
        self.cb_file_type.currentTextChanged.connect(self.toggle_ext_edit)
        h_type.addWidget(self.cb_file_type)
        v.addLayout(h_type)

        # 2) 自定义扩展名输入框（初始禁用）
        h_ext = QHBoxLayout()
        h_ext.addWidget(QLabel("扩展名(逗号分隔):"))
        self.le_ext = QLineEdit("tif,tiff,asc")
        self.le_ext.setEnabled(False)   # 默认禁用
        h_ext.addWidget(self.le_ext)
        v.addLayout(h_ext)

        # 输出路径
        h2 = QHBoxLayout()
        h2.addWidget(QLabel('<font color="red">*</font>输出文件 如 XX.tif:'))
        self.le_out = QLineEdit()
        h2.addWidget(self.le_out)
        btn_out = QPushButton("浏览")
        btn_out.clicked.connect(self.browse_out)
        h2.addWidget(btn_out)
        v.addLayout(h2)

        # 子数据集选择下拉框
        self.cb_subdataset = QComboBox()
        v.addWidget(QLabel("选择子数据集:"))
        v.addWidget(self.cb_subdataset)

        # 1) 重叠区域融合方法
        h_method = QHBoxLayout()
        h_method.addWidget(QLabel('<font color="red">*</font>融合方法:'))
        self.cb_method = QComboBox()
        self.cb_method.addItems(["mean", "max", "min",  "sum", "first", "last"])  
        h_method.addWidget(self.cb_method)  
        v.addLayout(h_method)

        # warp 选项
        # h_warp = QHBoxLayout()
        # h_warp.addWidget(QLabel('重采样算法:'))
        # self.cb_alg = QComboBox()
        # self.cb_alg.addItems(["nearest", "bilinear", "cubic", "average", "max", "min", "mode", "med","q1","q3","sum"])
        # h_warp.addWidget(self.cb_alg)
        # v.addLayout(h_warp)

        # 输出像素类型
        h_type = QHBoxLayout()
        h_type.addWidget(QLabel('<font color="red">*</font>输出像素类型:'))
        self.cb_type = QComboBox()
        self.cb_type.addItems([
            "Float32",     # 32 位浮点
            "Byte",        # 8 位无符号整型
            "Int16",       # 16 位有符号整型
            "UInt16",      # 16 位无符号整型
            "Int32",       # 32 位有符号整型
            "UInt32",      # 32 位无符号整型
            "Float64",     # 64 位浮点
            "CInt16",      # 16 位复数整型
            "CInt32",      # 32 位复数整型
            "CFloat32",    # 32 位复数浮点
            "CFloat64"     # 64 位复数浮点
        ])
        h_type.addWidget(self.cb_type)
        v.addLayout(h_type)

        # 输出坐标系
        h_srs = QHBoxLayout()
        label_srs = QLabel("目标坐标系 (EPSG/Proj4/WKT)")
        h_srs.addWidget(label_srs)
        self.le_srs = QLineEdit()
        self.le_srs.setToolTip(
            "留空：沿用源坐标系\n"
            "EPSG:4326 → WGS84 经纬度\n"
            "EPSG:32650 → UTM 50N\n"
            "也可直接写 Proj4 字符串\n"
            "示例：+proj=utm +zone=50 +datum=WGS84 +units=m"
        )
        self.le_srs.setMouseTracking(True)   # 关键
        h_srs.addWidget(self.le_srs)
        v.addLayout(h_srs)

        # dstNodata
        h_nodata = QHBoxLayout()
        lable_nodata = QLabel("Nodata 值:")
        h_nodata.addWidget(lable_nodata)
        self.le_nodata = QLineEdit()
        self.le_nodata.setText("")   # 默认为空
        self.le_nodata.setToolTip("不设置则沿用源的nodata")
        self.le_nodata.setMouseTracking(True)   # 关键
        h_nodata.addWidget(self.le_nodata)
        v.addLayout(h_nodata)

        # warpMemoryLimit
        # 内存限制自定义
        h_mem = QHBoxLayout()
        h_mem.addWidget(QLabel('<font color="red">*</font>分块窗口大小:'))
        self.le_mem = QLineEdit("1024")
        self.le_mem.setToolTip(
            "每次处理多大的像素块，单位为像素\n" \
            "默认 256 块，依据内存空间设定合理值\n"
        )
        self.le_mem.setMouseTracking(True)   # 关键
        h_mem.addWidget(self.le_mem)

        h_mem.addWidget(QLabel('<font color="red">*</font>线程数:'))
        self.le_work = QLineEdit("10")
        self.le_work.setToolTip(
            "设置多线程处理时的线程数\n" \
            "同时计算多少个分块\n"
            "默认 8 线程，可以适当增加\n"
        )
        self.le_work.setMouseTracking(True)   # 关键
        h_mem.addWidget(self.le_work)

        h_mem.addWidget(QLabel('<font color="red">*</font>刷盘块数:'))
        self.le_flush = QLineEdit("100")
        self.le_flush.setToolTip(
            "设置每隔每次写入多少块数据到磁盘\n" \
            "默认 100 块，依据内存空间设定合理值\n"
        )
        self.le_flush.setMouseTracking(True)   # 关键
        h_mem.addWidget(self.le_flush)

        v.addLayout(h_mem)

        # creationOptions 自定义输入框
        h_opt = QHBoxLayout()
        h_opt.addWidget(QLabel("Creation Options:"))
        self.le_opts = QLineEdit()
        self.le_opts.setText("COMPRESS=LZW,TILED=YES")   # 默认
        h_opt.addWidget(self.le_opts)
        v.addLayout(h_opt)

        # BIGTIFF
        self.chk_big = QCheckBox("启用 BIGTIFF")
        self.chk_big.setChecked(True)          # 默认打开

        h_check = QHBoxLayout()
        h_check.addWidget(self.chk_big)
        h_check.addStretch()  # 让两个复选框靠左
        v.addLayout(h_check)

        # 日志区
        self.log_edit = QTextEdit()
        self.log_edit.setReadOnly(True)
        v.addWidget(self.log_edit)

        # 进度条
        self.progress_bar = QProgressBar()  
        v.addWidget(self.progress_bar)

        # 合并按钮
        self.btn_merge = QPushButton("开始合并")
        self.btn_merge.clicked.connect(self.start_merge)
        v.addWidget(self.btn_merge)

    def update_subdataset_list(self, directory):
        self.cb_subdataset.clear()
        # 扫描 HDF 和 NetCDF 文件
        for ext in ('*.hdf', '*.nc'):
            for filepath in glob.glob(os.path.join(directory, ext)):
                ds = gdal.Open(filepath, gdal.GA_ReadOnly)
                if ds is None:
                    continue
                subdatasets = ds.GetSubDatasets()
                for name, desc in subdatasets:
                    self.cb_subdataset.addItem(desc)  # desc 更人类可读
                ds = None
                break  # 只显示一个文件的子数据集，你可以选择是否去掉 break

    def _get_resource_path(self, relative_path):
        """获取资源的绝对路径（适配打包和开发环境）"""
        try:
            # 打包后的临时文件夹路径
            base_path = sys._MEIPASS
        except AttributeError:
            # 开发环境的当前目录
            base_path = os.path.abspath(".")
        
        return os.path.join(base_path, relative_path)

    def toggle_ext_edit(self, text):
        self.le_ext.setEnabled(text == "自定义")

    # ---------- 槽函数 ----------
    def browse_dir(self, line_edit):
        d = QFileDialog.getExistingDirectory(self, "选择目录")
        if d:
            line_edit.setText(d)
            print(line_edit.text())
            self.update_subdataset_list(d)  # 更新子数据集列表

    def browse_out(self):
        # 设置文件过滤器，支持多种栅格数据格式
        exts = ['*.tif', '*.tiff']
        file_filter = "All Supported Formats ({});;{}".format(" ".join(exts), ";;".join(exts))
        
        # 打开文件保存对话框
        f, _ = QFileDialog.getSaveFileName(self, "保存为...", "", file_filter)
        if f:
            self.le_out.setText(f)

    def log(self, txt):
        self.log_edit.append(f'<font color="black">{txt}</font>')
        self.log_edit.ensureCursorVisible()

    def error(self, txt):
        self.log_edit.append(f'<font color="red"><b>{txt}</b></font>')
        self.log_edit.ensureCursorVisible()

    def get_files(self, folder):
        selected = self.cb_file_type.currentText()

        if selected == "自定义":
            raw = self.le_ext.text().strip()
            if not raw:
                return []
            exts = [f"*.{e.strip().lstrip('*.')}" for e in raw.split(',') if e.strip()]
        else:
            exts = [f"*.{selected}"]

        files = []
        for pattern in exts:
            files.extend(glob.glob(os.path.join(folder, pattern)))
        return sorted(files)

    def start_merge(self):
        in_dir = self.le_in_dir.text()
        out_file = self.le_out.text()
        if not in_dir or not out_file:
            self.error("请先选择目录和输出文件")
            return

        files = self.get_files(in_dir)
        if not files:
            self.error("目录下无可用的文件")
            return

        # 解析 creationOptions（用英文逗号分隔键值对）
        raw_opts = self.le_opts.text().strip()
        creation_opts = [s.strip() for s in raw_opts.split(',') if s.strip()]

        # 如果勾选了 BIGTIFF，确保加进去
        if self.chk_big.isChecked() and 'BIGTIFF=YES' not in creation_opts:
            creation_opts.append('BIGTIFF=YES')

        opts = {
            # 'resampleAlg': self.cb_alg.currentText(), # 重采样算法
            'block_size': int(self.le_mem.text()), # 分块窗口大小
            'n_workers': int(self.le_work.text()), # 线程数
            'creationOptions': creation_opts, # GDAL 写入选项
            'dst_dtype': self.cb_type.currentText(),  # 输出像素类型
            'flush_interval': int(self.le_flush.text()),  # 输出像素类型
        }
        # 输出坐标系设置
        srs_text = self.le_srs.text().strip()
        if srs_text and srs_text.lower() != 'none':
            opts['dstSRS'] = srs_text
        # nodata 设置
        nodata_text = self.le_nodata.text().strip()
        if nodata_text:                      # 非空才设置
            opts['dstNodata'] = int(nodata_text)
        self.log(f"选项：{opts}")
        # print(opts)
        self.progress_bar.setValue(0)
        self.btn_merge.setEnabled(False)
        self.log("开始处理……")
        # HDF 子数据集合并
        # print(files)
        if self.cb_subdataset.count() > 0:
            self.log("多数据集子集拼接")
            subdataset_index = self.cb_subdataset.currentIndex()
            temp_dir = os.path.join(os.path.dirname(in_dir), 'temp')
            os.makedirs(temp_dir, exist_ok=True)  # 如果目录已存在，不会抛出错误
            
            self.worker = HDFMergeThread(files, out_file, opts, temp_dir, subdataset_index,self.cb_method.currentText())
            self.worker.log.connect(self.log)
            self.worker.error.connect(self.error)
            self.worker.progress.connect(self.progress_bar.setValue)
            self.worker.finished.connect(lambda: self.btn_merge.setEnabled(True))
            self.worker.start()
            return
        
        self.worker = MergeThread(files, out_file, opts, self.cb_method.currentText())
        self.worker.log.connect(self.log)
        self.worker.error.connect(self.error)
        self.worker.progress.connect(self.progress_bar.setValue)
        self.worker.finished.connect(lambda: self.btn_merge.setEnabled(True))
        self.worker.start()

    def closeEvent(self, event):
        if hasattr(self, 'worker') and self.worker.isRunning():
            self.worker.requestInterruption()  # 请求线程中断
            self.worker.quit()  # 退出事件

# ---------- 入口 ----------
if __name__ == '__main__':
    app = QApplication(sys.argv)
    win = MergerUI()
    win.show()
    sys.exit(app.exec_())