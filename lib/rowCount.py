from osgeo import gdal
import sys
# this allows GDAL to throw Python Exceptions
gdal.UseExceptions()

argv = gdal.GeneralCmdLineProcessor( sys.argv )

if argv is None:
    sys.exit( 0 )

try:
    #print(argv[1])
    src_ds = gdal.Open(argv[1])
except RuntimeError, e:
    print 'Unable to open INPUT.tif'
    print e
    sys.exit(1)

try:
    srcband = src_ds.RasterYSize
    print(srcband)
except RuntimeError, e:
    # for example, try GetRasterBand(10)
    print 'Band ( %i ) not found' % band_num
    print e
    sys.exit(1)