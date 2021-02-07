"""
Unittests for utility functions

"""
from numpy import pi

from pyproj import CRS, Transformer
from shapely.ops import transform
from shapely.geometry.base import BaseGeometry
from shapely.geometry import Point

from metacatalog.util.location import get_search_shape


def _do_back_transform(geom: BaseGeometry) -> BaseGeometry:
    src = CRS('EPSG:4326')
    dst = CRS('EPSG:3857')
    t = Transformer.from_crs(src, dst)

    return transform(t.transform, geom)


def check_search_area_from_point():
    p = (1.0, 1.0)
    buffer = 60

    # these are the circumference and area expected
    res_area = pi * buffer**2
    res_circ = pi * buffer * 2

    shape = _do_back_transform(get_search_shape(p, buffer=buffer))

    assert abs(shape.area - res_area) < 100
    assert abs(shape.length - res_circ) < 1
    
    return True

def check_import_options():
    wkt = 'Point (14 15)'
    buffer = 100000  #100km

    p1 = get_search_shape(Point((14, 15)), buffer=buffer)
    p2 = get_search_shape(wkt, buffer=buffer)

    p1 = _do_back_transform(p1)
    p2 = _do_back_transform(p2)

    assert abs(p1.area - p2.area) < 100   #100m
    assert abs(p1.length - p2.length) < 10 # 10m

    return True


def test_spatial_filter():
    assert check_search_area_from_point()
    assert check_import_options()