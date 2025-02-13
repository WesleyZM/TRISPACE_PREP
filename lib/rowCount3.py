from osgeo import gdal
import sys

dataset = gdal.Open(sys.argv[1], gdal.GA_ReadOnly)
num_rows = dataset.RasterYSize
print(num_rows)