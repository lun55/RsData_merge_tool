# 基于GDAL的遥感影像拼接工具 v1.0

一个基于 [GDAL](https://gdal.org/) 和 [PyQt5](https://www.riverbankcomputing.com/software/pyqt/intro) 的桌面工具，支持将多个遥感影像文件进行自动拼接处理  

---
![工具界面](https://github.com/user-attachments/assets/8972130b-1845-40d0-8c47-a41008a1672d)

---

## ✨ 功能特性

- 🔗 利用 GDAL 的 `Warp` 实现多影像拼接
- 📊 进度条实时反馈处理进度
- 💬 日志窗口输出处理信息和错误提示
- 🖥️ 支持 GUI 图形界面操作

---

## 📦 安装依赖

确保使用 Python 3.11 环境，并在虚拟环境中运行以下命令安装依赖项：

```bash
altgraph==0.17.4
GDAL==3.6.2
packaging==25.0
pefile==2023.2.7
pip==25.1.1
pyinstaller==6.14.2
pyinstaller-hooks-contrib==2025.7
PyQt5==5.15.11
PyQt5-Qt5==5.15.2
PyQt5_sip==12.17.0
pywin32-ctypes==0.2.3
setuptools==65.5.0
```
--- 

## ⚙️ 打包
使用 PyInstaller 打包成可执行文件：

```bash
pyinstaller RSData_Merger_Tool.spec
```

