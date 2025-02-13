from osgeo import gdal
import sys
# this allows GDAL to throw Python Exceptions
gdal.UseExceptions()

argv = gdal.GeneralCmdLineProcessor( sys.argv )

if argv is None:
    sys.exit( 0 )

try:
    print(argv[1])
    src_ds = gdal.Open(argv[1])
except RuntimeError:
    print ('Unable to open INPUT.tif')
    sys.exit(1)

try:
    srcband = src_ds.RasterCount
    print(srcband)
except RuntimeError:
    # for example, try GetRasterBand(10)
    print ('Band ( %i ) not found' % band_num)
    sys.exit(1)