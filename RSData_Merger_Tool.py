import os, sys, traceback, glob
from osgeo import gdal
from PyQt5.QtCore import QThread, pyqtSignal
from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout, QHBoxLayout,
                             QPushButton, QLineEdit, QTextEdit, QFileDialog,
                             QLabel, QProgressBar, QCheckBox, QComboBox)
from PyQt5.QtGui import QIcon
import shutil
# ---------- 后台合并线程 ----------
class MergeThread(QThread):
    log      = pyqtSignal(str)       # 普通日志
    error    = pyqtSignal(str)       # 错误日志
    progress = pyqtSignal(int)       # 0-100

    def __init__(self, asc_files, out_path, opts):
        super().__init__()
        self.asc_files = asc_files
        self.out_path  = out_path
        self.opts      = opts        # dict 参数
        
    def run(self):
        try:
            if not self.asc_files:
                raise RuntimeError("未找到任何 .asc 文件，请检查输入目录")
                
            self.log.emit(f"找到 {len(self.asc_files)} 个文件")

            def _progress(pct, msg, data):
                self.progress.emit(int(pct * 100))
                return 1

            gdal.UseExceptions()
            self.log.emit("开始合并...")
            
            # 添加更多调试信息
            self.log.emit(f"输出路径: {self.out_path}")
            self.log.emit(f"选项: {self.opts}")
            
            gdal.Warp(self.out_path, self.asc_files, **self.opts,
                    callback=_progress)
            # ✅ 关键：刷新缓存并关闭
            out_ds = gdal.Open(self.out_path, gdal.GA_Update)
            if out_ds:
                out_ds.FlushCache()  # 强制写盘
                out_ds = None 
            self.progress.emit(100)       # 关闭数据集 
            self.log.emit(f"✅ 合并完成：{self.out_path}")
        except Exception as e:
            self.error.emit(f"合并失败: {str(e)}")
            self.error.emit(traceback.format_exc())

# ---- HDF 子数据集合并线程 ----
class HDFMergeThread(QThread):
    log = pyqtSignal(str)
    error = pyqtSignal(str)
    progress = pyqtSignal(int)

    def __init__(self, hdf_files, out_path, opts, temp_dir, subdataset_index):
        super().__init__()
        self.hdf_files = hdf_files
        self.out_path = out_path
        self.opts = opts
        self.temp_dir = temp_dir
        self.subdataset_index = subdataset_index

    def run(self):
        try:
            if not self.hdf_files:
                raise RuntimeError("未找到任何 .hdf 文件，请检查输入目录")

            self.log.emit(f"找到 {len(self.hdf_files)} 个文件")
            self.log.emit(f"正在提取子集到临时文件夹")
            gdal.UseExceptions()
            for hdf_file in self.hdf_files:
                basename = os.path.basename(hdf_file)
                ds = gdal.Open(hdf_file, gdal.GA_ReadOnly)
                subdatasets = ds.GetSubDatasets()
                sd = subdatasets[self.subdataset_index]
                temp_file_path = os.path.join(self.temp_dir, f'{basename}_{sd[1]}.tif')
                gdal.Warp(temp_file_path, [sd[0]], format='GTiff')
                ds = None
            self.log.emit(f"成功提取子集到临时文件夹{self.temp_dir}")
            # 合并提取的栅格文件
            asc_files = glob.glob(os.path.join(self.temp_dir, '*.tif'))
            # 添加更多调试信息
            self.log.emit("开始合并...")
            self.log.emit(f"找到 {len(asc_files)} 个文件")
            self.log.emit(f"输出路径: {self.out_path}")
            self.log.emit(f"选项: {self.opts}")
            if asc_files:

                def _progress(pct, msg, data):
                    self.progress.emit(int(pct * 100))
                    return 1
                
                gdal.Warp(self.out_path, asc_files, **self.opts,
                        callback=_progress)
                # ✅ 关键：刷新缓存并关闭
                out_ds = gdal.Open(self.out_path, gdal.GA_Update)
                if out_ds:
                    out_ds.FlushCache()  # 强制写盘
                    out_ds = None 
                self.progress.emit(100)       # 关闭数据集 
                self.log.emit(f"✅ 合并完成：{self.out_path}")
            else:
                self.error.emit("未找到任何栅格文件进行合并")

        except Exception as e:
            self.error.emit(f"合并失败: {str(e)}")
            self.error.emit(traceback.format_exc())

        finally:
            # 删除临时文件夹
            self.log.emit(f"删除临时文件夹{self.temp_dir}")
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)

# ---------- 主界面 ----------
class MergerUI(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("遥感影像拼接工具 v1.0 By LMQ")
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
            "'*.img', '*.jp2', '*.png', '*.nc', '*.hdf'\n"
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
        self.cb_file_type.addItems(["全部", "自定义"])
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

        # warp 选项
        v.addWidget(QLabel('<font color="red">*</font>重采样算法:'))
        self.cb_alg = QComboBox()
        self.cb_alg.addItems(["Average", "NearestNeighbour", "Bilinear", "Cubic", "CubicSpline", "Lanczos", "max", "min","Mode","Med","Q1","Q3","Sum"])
        v.addWidget(self.cb_alg)

        # 输出像素类型
        v.addWidget(QLabel('<font color="red">*</font>输出像素类型:'))
        self.cb_type = QComboBox()
        self.cb_type.addItems([
            "Byte",        # 8 位无符号整型
            "Int16",       # 16 位有符号整型
            "UInt16",      # 16 位无符号整型
            "Int32",       # 32 位有符号整型
            "UInt32",      # 32 位无符号整型
            "Float32",     # 32 位浮点
            "Float64",     # 64 位浮点
            "CInt16",      # 16 位复数整型
            "CInt32",      # 32 位复数整型
            "CFloat32",    # 32 位复数浮点
            "CFloat64"     # 64 位复数浮点
        ])
        v.addWidget(self.cb_type)

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

        # warpMemoryLimit
        # 内存限制自定义
        h_mem = QHBoxLayout()
        h_mem.addWidget(QLabel("工作缓冲区的大小 (MB):"))
        self.le_mem = QLineEdit("256")
        h_mem.addWidget(self.le_mem)
        v.addLayout(h_mem)

        # dstNodata 自定义输入框
        h_nodata = QHBoxLayout()
        lable_nodata = QLabel("Nodata 值:")
        h_nodata.addWidget(lable_nodata)
        self.le_nodata = QLineEdit()
        self.le_nodata.setText("")   # 默认为空
        self.le_nodata.setToolTip("不设置则沿用源的nodata")
        self.le_nodata.setMouseTracking(True)   # 关键
        h_nodata.addWidget(self.le_nodata)
        v.addLayout(h_nodata)

        # creationOptions 自定义输入框
        h_opt = QHBoxLayout()
        h_opt.addWidget(QLabel("Creation Options:"))
        self.le_opts = QLineEdit()
        self.le_opts.setText("COMPRESS=LZW,TILED=YES")   # 默认
        h_opt.addWidget(self.le_opts)
        v.addLayout(h_opt)

        # 多线程 IO
        self.chk_multi = QCheckBox("启用多线程 IO")
        self.chk_multi.setChecked(True)

        # BIGTIFF
        self.chk_big = QCheckBox("启用 BIGTIFF")
        self.chk_big.setChecked(True)          # 默认打开

        h_check = QHBoxLayout()
        h_check.addWidget(self.chk_big)
        h_check.addWidget(self.chk_multi)
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
        # 清空子数据集下拉列表
        self.cb_subdataset.clear()
        # 遍历目录中的所有文件
        for hdf_file in glob.glob(os.path.join(directory, '*.hdf')):
            ds = gdal.Open(hdf_file, gdal.GA_ReadOnly)
            subdatasets = ds.GetSubDatasets()
            for i in range(len(subdatasets)):
                self.cb_subdataset.addItem(f"{subdatasets[i][1]}")
            ds = None
            break

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
        exts = ['*.tif', '*.tiff', '*.asc', '*.img', '*.jp2', '*.png', '*.nc', '*.hdf']
        file_filter = "All Supported Formats ({});;{}".format(" ".join(exts), ";;".join(exts))
        
        # 打开文件保存对话框
        f, _ = QFileDialog.getSaveFileName(self, "保存为...", "", file_filter)
        if f:
            self.le_out.setText(f)

    def log(self, txt):
        self.log_edit.append(txt)
        self.log_edit.ensureCursorVisible()

    def error(self, txt):
        self.log('<font color="red"><b>' + txt.replace('\n', '<br>') + '</b></font>')

    def get_files(self, folder):
        if self.cb_file_type.currentText() == "全部":
            # GDAL 能识别的所有栅格
            exts = ['*.tif', '*.tiff', '*.asc', '*.img', '*.jp2', '*.png', '*.nc', '*.hdf']
        else:
            # 用户自定义
            raw = self.le_ext.text().strip()
            if not raw:
                return []
            exts = [f"*.{e.strip()}" for e in raw.split(',')]

        files = []
        for e in exts:
            files.extend(glob.glob(os.path.join(folder, e)))
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

        dtype_map = {
            "Byte": gdal.GDT_Byte,
            "Int16": gdal.GDT_Int16,
            "UInt16": gdal.GDT_UInt16,
            "Int32": gdal.GDT_Int32,
            "UInt32": gdal.GDT_UInt32,
            "Float32": gdal.GDT_Float32,
            "Float64": gdal.GDT_Float64,
            "CInt16": gdal.GDT_CInt16,
            "CInt32": gdal.GDT_CInt32,
            "CFloat32": gdal.GDT_CFloat32,
            "CFloat64": gdal.GDT_CFloat64
        }

        opts = {
            'resampleAlg': getattr(gdal, f'GRA_{self.cb_alg.currentText().capitalize()}'),
            'warpMemoryLimit': int(self.le_mem.text()),
            'multithread': self.chk_multi.isChecked(),
            'creationOptions': creation_opts,
            'outputType': dtype_map[self.cb_type.currentText()]
        }
        # 输出坐标系设置
        srs_text = self.le_srs.text().strip()
        if srs_text and srs_text.lower() != 'none':
            opts['dstSRS'] = srs_text
        # nodata 设置
        nodata_text = self.le_nodata.text().strip()
        if nodata_text:                      # 非空才设置
            opts['dstNodata'] = int(nodata_text)
        print(opts)
        self.progress_bar.setValue(0)
        self.btn_merge.setEnabled(False)
        self.log("开始处理……")
        # HDF 子数据集合并
        # print(files)
        if self.cb_subdataset.count() > 0:
            print("HDF 子数据集并")
            subdataset_index = self.cb_subdataset.currentIndex()
            temp_dir = os.path.join(os.path.dirname(in_dir), 'temp')
            os.makedirs(temp_dir, exist_ok=True)  # 如果目录已存在，不会抛出错误
            
            self.worker = HDFMergeThread(files, out_file, opts, temp_dir, subdataset_index)
            self.worker.log.connect(self.log)
            self.worker.error.connect(self.error)
            self.worker.progress.connect(self.progress_bar.setValue)
            self.worker.finished.connect(lambda: self.btn_merge.setEnabled(True))
            self.worker.start()
            return
        
        self.worker = MergeThread(files, out_file, opts)
        self.worker.log.connect(self.log)
        self.worker.error.connect(self.error)
        self.worker.progress.connect(self.progress_bar.setValue)
        self.worker.finished.connect(lambda: self.btn_merge.setEnabled(True))
        self.worker.start()

    def closeEvent(self, event):
        if hasattr(self, 'worker') and self.worker.isRunning():
            self.worker.terminate()  # 请求线程终止
            self.worker.wait()  # 等待线程实际结束
        event.accept()

# ---------- 入口 ----------
if __name__ == '__main__':
    app = QApplication(sys.argv)
    win = MergerUI()
    win.show()
    sys.exit(app.exec_())