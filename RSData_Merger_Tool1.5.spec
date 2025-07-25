# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['RSData_Merger_Tool1.5.py','mosaic_overlap.py'],
    pathex=[],
    binaries=[],
    datas=[('app_icon.ico', '.')],
    hiddenimports=[
        'rasterio.vrt', 
        'rasterio._base',
        'rasterio._io',
        'rasterio._warp',
        'rasterio._features',
        'rasterio._err',
        'rasterio.sample',
        'rasterio.enums',
        'rasterio.transform',
        'rasterio.windows',
        'rtree',
        'osgeo.gdal',
        'osgeo.osr',
        'osgeo.ogr',
        'PyQt5.QtCore',
        'PyQt5.QtWidgets',
        'PyQt5.QtGui',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='遥感影像拼接工具V1.5',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='app_icon.ico'
)
