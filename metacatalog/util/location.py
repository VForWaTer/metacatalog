"""
Location utility functions can convert the various possible inputs 
into a shapely.Polygon that can be used as a search area.

"""
from shapely.geometry import Point, Polygon, MultiPoint
from shapely.geometry.base import BaseGeometry
from shapely import wkt, wkb
from shapely.ops import transform

from pyproj import Transformer, Proj, CRS

from geoalchemy2.functions import ST_Within, ST_GeomFromText, ST_SetSRID
from sqlalchemy.orm import Query

from metacatalog import models

def get_search_shape(arg, buffer=None, buffer_use_epsg=3857) -> BaseGeometry:
    """
    Calculate a search shape from a whole bunch of arguments.
    The search shape can optionally be buffered. 
    .. note::
        Some arguments might resolve to a Point geometry. In these cases
        make sure to **use a buffer value**, at least by a small value.
        Otherwise the query will end up to search for **exact** matches, 
        which will always turn ``False``, even for the Point itself.
    
    Parameters
    ----------
    buffer : int, float
        Optional. If given, the search area resolved from the argument
        will be buffered by the given value. Remind that buffer has to transform
        the data to apply Euklidean buffers.
    buffer_use_epsg : int
        The EPSG number of a projected CRS that shall be used during buffering.
        Mind the Note below.
    arg : list, str, byte, shapely.BaseGeometry
        A search area can be generate from a rich list of input arguments. 
        Depending on the data type and shape of :attr:`arg`, different search areas
        are resolved.

        - **str** has to be a WKT of type Point or Polygon
        - **byte** has to be a WKB of type Point or Polygon
        - **BaseGeometry** any valid shapely Geometry, that implements the buffer function
        - **list** has to be a list of int or float values. Resolving is based on shape:
            - ``len(arg)==2`` is used as ``Point(arg)``. Remind to buffer the Point
            - ``len(arg)==4`` is a BoundingBox defined as ``[left, bottom, right, up]``
            - 2 dimensional lists are converted to Polygons

    Note
    ----
    metacatalog uses unprojected WGS84 coordinates for the contained coordinates.
    For applying a buffer, the coordinates of the argument need to be transformed 
    into a projected coordinate reference system using meter as a unit. 
    After buffering, the search area is back-transformed to WGS84. This can have 
    accuracy implications. Nevertheless, this step is necessary, as the database 
    can then filter without reprojections of possibly large amounts of data.
    By default the argument is transformed to Transversal Mercartor Projection, as 
    this has global coverage. Unfortunately, the transformation error can get 
    massive in some, especially small, study areas. Whenever possible overwrite the 
    CRS to be used for buffering by a local reference system like UTM.

    Returns
    -------
    area : shapely.geometry.Polygon
        The resolved Polygon, which can be used to create a spatial filter to 
        find :class:`Entries <metacatalog.models.Entry>` within the area.

    See Also
    --------
    build_query
    around

    """
    # coordinates given
    if isinstance(arg, (list, tuple)):
        # point, bbox or polygon
        if len(arg) == 2:
            # point
            geom = Point(arg)
        elif len(arg) == 4:
            # bbox [min x, min y, max x, max y]
            l, b, r, u = arg
            geom = Polygon([(l, b), (l, u), (r, u), (r, b)])
        elif all([len(_) == 2 for _ in arg]):
            # polygon
            geom = Polygon(args)
        else:
            raise AttributeError("Can't convert to Point or Polygon.")
    
    # wkt
    elif isinstance(arg, str):
        geom = wkt.loads(arg)
    
    # wkb
    elif isinstance(arg, bytes):
        geom = wkb.loads(arg)

    # already 
    elif isinstance(arg, BaseGeometry):
        geom = arg
    
    # not supported
    else:
        raise TypeError("The given arg object can't be converted to a Geometry")
        
    # check 
    if not geom.is_valid:
        raise AttributeError("Could not resolve to valid Geometry:\n{poly}".format(poly=geom.to_wkt()))

    if buffer is None:
        return area
    
    if not isinstance(buffer, (int, float)):
        raise AttributeError("radius has to be an integer or float buffer value")
    
    # buffer at least with 0 distance to tidy polygons
    # create transformers
#    src = Proj(init="epsg:4326")
#    dest = Proj(init="epsg:%d" % buffer_use_epsg)
    src = CRS('EPSG:4326')
    dst = CRS('EPSG:%d' % buffer_use_epsg)
#    from_dest = Transformer.from_proj(src, dest)
#    to_dest = Transformer.from_proj(dest, src)
    from_dest = Transformer.from_crs(src, dst)
    to_dest = Transformer.from_crs(dst, src)

    _g = transform(from_dest.transform, geom)
    area = _g.buffer(buffer)

    return transform(to_dest.transform, area)


def build_query(query: Query, area: Polygon) -> Query:
    """
    Small helper function to create the spatial filter.

    Parameters
    ----------
    query : Query
        sqlalchemy orm query object. The filter will be appended
        (in an AND manner) to the exisiting query.
    area : Polygon
        shapely.geometry.Polygon of the search area. It will be 
        resolved by a Within spatial filter.
    
    Returns
    -------
    query : Query
        The filtered (unexecuted) sqlalchemy ORM query object.

    """
    # turn the area to an SQL clause
    search = ST_SetSRID(ST_GeomFromText(area.to_wkt()), 4326)

    # create the filter
    return query.filter(ST_Within(models.Entry.location, search))


def around(entry, distance, unit='km', query=None, buffer_use_epsg=3857):
    """
    Find other :class:`Entries <metacatalog.models.Entry>` around a given instance
    by specifying a distance around the entry. If an sqlalchemy query is supplied, 
    the resulting query is returned, otherwise the filter area itself is returned.

    Parameters
    ----------
    entry : Entry
        The Entry instance that is used to find neighbors. A list of Entry instances 
        is also accepted, as well as an :class:`EntryGroup <metacatalog.models.EntryGroup>`,
        which will resolve to a list of entries. A list will be converted to the Convex 
        Hull around all instances.
    distance : int, float
        The search distance around the entry instance(s). The distance should be given 
        in meter, if km or miles is used, specify the unit accordingly. Also mind the 
        Note below.
    unit : str
        Can be one of ``['meter', 'km', 'mile', 'nautic']``. The unit will be used to 
        convert the given distance to meter.
    buffer_use_epsg : int
        The EPSG number of a projected CRS that shall be used during buffering.
        Mind the Note below.

    Note
    ----
    metacatalog uses unprojected WGS84 coordinates for the contained coordinates.
    For applying a buffer, the coordinates of the argument need to be transformed 
    into a projected coordinate reference system using meter as a unit. 
    After buffering, the search area is back-transformed to WGS84. This can have 
    accuracy implications. Nevertheless, this step is necessary, as the database 
    can then filter without reprojections of possibly large amounts of data.
    By default the argument is transformed to Transversal Mercartor Projection, as 
    this has global coverage. Unfortunately, the transformation error can get 
    massive in some, especially small, study areas. Whenever possible overwrite the 
    CRS to be used for buffering by a local reference system like UTM.

    See Also
    --------
    build_query
    get_search_shape


    """
    if unit.lower() == 'km':
        unit = 1000.
    elif unit.lower() == 'm' or unit.lower() == 'meter':
        unit = 1.
    elif unit.lower() == 'mile':
        unit = 1609.34
    elif unit.lower() == 'nautic':
        unit = 1852.
    else:
        raise AttributeError("unit has to be in ['meter', 'km', 'mile', 'nautic']")

    # calculate buffer distance
    buffer = distance * unit

    # get the Entry location
    if isinstance(entry, models.Entry):
        entries = [entry]
    elif isinstance(entry, models.EntryGroup):
        entries = entry.entries
    elif isinstance(entry, (list, tuple)) and all([isinstance(e, models.Entry) for e in entry]):
        entries = entry
    
    # make a multipoint and use the convex hull 
    # -> works because a MultiPoint's convex hull of only one Point collapeses to a the Point itself
    mp = MultiPoint(points=[e.location_shape for e in entries])
    arg = mp.convex_hull

    # calculat the search area
    area = get_search_shape(arg, buffer=buffer, buffer_use_epsg=buffer_use_epsg)

    # if a query is given -> return filter else return the search area
    if query is not None:
        # get the ids which where questions
        ids = [e.id for e in entries]
        query = build_query(query, area)
        query = query.filter(~models.Entry.id.in_(ids))
        return query
    else:
        return area

