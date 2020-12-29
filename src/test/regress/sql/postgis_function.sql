--验证postgis的analyze信息能否使用成功
set current_schema=postgis;
SELECT count(1) from geom_polygon as p1 join geom_line as p2 ON ST_INTERSECTS(p1.the_geom, p2.the_geom) where p1.id<1000;

/*
###############################################################################
# TESTCASE NAME : postgis_accessors.sql
# COMPONENT(S)  : test function accessors
# PREREQUISITE  : 
# PLATFORM      : SUSE11.3
# DESCRIPTION   : 验证postgis几何对象存取函数
# TAG           : ST_CeometryType,ST_Boundary,ST_CoordDim,ST_Dimension
# TC LEVEL      : Level 1
################################################################################
*/

--S1.验证函数ST_CeometryType返回几何图形的类型
SELECT GeometryType(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'));
--S2.验证函数ST_Boundary返回几何的边界
SELECT ST_AsText(ST_Boundary(ST_GeomFromText('LINESTRING(1 1,0 0, -1 1)')));
--S3.验证函数ST_CoordDim返回几何对象所处的坐标维度
SELECT ST_CoordDim('CIRCULARSTRING(1 2 3, 1 3 4, 5 6 7, 8 9 10, 11 12 13)');
--S4.验证函数ST_Dimension返回几何对象的固有维度
SELECT ST_Dimension('GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))');
--S5.验证函数ST_EndPoint返回几何的最后一个点
SELECT ST_AsText(ST_EndPoint('LINESTRING(1 1, 2 2, 3 3)'::geometry));
--S6.验证函数ST_Envelope返回几何图形的边界框
SELECT ST_AsText(ST_Envelope('POINT(1 3)'::geometry));
--S7.验证函数ST_ExteriorRing返回表示POLYGON几何体外环的线串
SELECT ST_AsEWKT( ST_ExteriorRing(ST_GeomFromEWKT('POLYGON((0 0 1, 1 1 1, 1 2 1, 1 3 1, 0 0 1))')));
--S8.验证函数ST_GeometryN返回几何体第N个几何
SELECT n, ST_AsEWKT(ST_GeometryN(the_geom, n)) As geomewkt
FROM (
VALUES (ST_GeomFromEWKT('MULTIPOINT(1 2 7, 3 4 7, 5 6 7, 8 9 10)') ),
( ST_GeomFromEWKT('MULTICURVE(CIRCULARSTRING(2.5 2.5,4.5 2.5, 3.5 3.5), (10 11, 12 11))') )
)As foo(the_geom)
CROSS JOIN generate_series(1,100) n
WHERE n <= ST_NumGeometries(the_geom);
--S9.验证函数ST_CeometryType返回几何图形的类型
SELECT ST_GeometryType(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'));
--S10.验证函数ST_InteriorRingN返回几何的第N个内部线串环
SELECT ST_AsText(ST_InteriorRingN(the_geom, 1)) As the_geom
FROM (SELECT ST_BuildArea(
ST_Collect(ST_Buffer(ST_Point(1,2), 20,3),
ST_Buffer(ST_Point(1, 2), 10,3))) As the_geom
) as foo;
--S11.验证函数ST_IsClosed几何封闭
SELECT ST_IsClosed('LINESTRING(0 0, 1 1, 0 0)'::geometry);
--S12.验证函数ST_IsCollection几何是集合
SELECT ST_IsCollection('LINESTRING(0 0, 1 1)'::geometry);
SELECT ST_IsCollection('MULTIPOINT EMPTY'::geometry);
--S13.验证函数ST_IsEmpty几何为空
SELECT ST_IsEmpty(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'));
--S14.验证函数ST_IsRing几何简单且封闭
SELECT ST_IsRing(the_geom), ST_IsClosed(the_geom), ST_IsSimple(the_geom)
FROM (SELECT 'LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)'::geometry AS the_geom) AS foo;
--S15.验证函数ST_IsSimple几何无相交的点
SELECT ST_IsSimple(ST_GeomFromText('POLYGON((1 2, 3 4, 5 6, 1 2))'));
--S16.验证函数ST_IsValid几何格式正确
SELECT ST_IsValid(ST_GeomFromText('LINESTRING(0 0, 1 1)')) As good_line,
ST_IsValid(ST_GeomFromText('POLYGON((0 0, 1 1, 1 2, 1 1, 0 0))')) As bad_poly;
--S17.验证函数ST_IsValidReason返回几何无效原因
SELECT ST_IsValidReason('LINESTRING(220227 150406,2220227 150407,222020 150410)');
--S18.验证函数ST_IsValidDetail返回几何无效详细信息
SELECT * FROM ST_IsValidDetail('LINESTRING(220227 150406,2220227 150407,222020 150410)');
--S19.验证函数ST_M返回该点M坐标
SELECT ST_M(ST_GeomFromEWKT('POINT(1 2 3 4)'));
--S20.验证函数ST_NDims返回几何坐标尺寸
SELECT ST_NDims(ST_GeomFromText('POINT(1 1)')) As d2point,
ST_NDims(ST_GeomFromEWKT('POINT(1 1 2)')) As d3point,
ST_NDims(ST_GeomFromEWKT('POINTM(1 1 0.5)')) As d2pointm;
--S21.验证函数ST_NPoints返回几何顶点
SELECT ST_NPoints(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'));
--S22.验证函数ST_NRings返回几何环的数量
SELECT ST_NRings(the_geom) As Nrings, ST_NumInteriorRings(the_geom) As ninterrings
FROM (SELECT ST_GeomFromText('POLYGON((1 2, 3 4, 5 6, 1 2))') As the_geom) As foo;
--S23.验证函数ST_NumGeometries返回几何体的面数
SELECT ST_NumGeometries(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'));
--S24.验证函数ST_NumInteriorRings返回几何内环数
--SELECT  ST_NumInteriorRings(the_geom) AS numholes FROM GEOM_POLYGON;
--S25.验证函数ST_NumPatches返回几何的面数
SELECT ST_NumPatches(ST_GeomFromEWKT('POLYHEDRALSURFACE( ((0 0 0, 0 0 1, 0 1 1, 0 1 0, 0 0 0)),
((0 0 0, 0 1 0, 1 1 0, 1 0 0, 0 0 0)), 
((0 0 0, 1 0 0, 1 0 1, 0 0 1, 0 0 0)),
((1 1 0, 1 1 1, 1 0 1, 1 0 0, 1 1 0)),
((0 1 0, 0 1 1, 1 1 1, 1 1 0, 0 1 0)),
 ((0 0 1, 1 0 1, 1 1 1, 0 1 1, 0 0 1)) )'));
--S26.验证函数ST_NumPoints返回几何点的个数
SELECT ST_NumPoints(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'));
--S27.验证函数ST_PatchN返回几何的第N个面
SELECT ST_AsEWKT(ST_PatchN(geom, 2)) As geomewkt
FROM (
VALUES (ST_GeomFromEWKT('POLYHEDRALSURFACE( ((0 0 0, 0 0 1, 0 1 1, 0 1 0, 0 0 0)),
((0 0 0, 0 1 0, 1 1 0, 1 0 0, 0 0 0)), ((0 0 0, 1 0 0, 1 0 1, 0 0 1, 0 0 0)),
((1 1 0, 1 1 1, 1 0 1, 1 0 0, 1 1 0)),
((0 1 0, 0 1 1, 1 1 1, 1 1 0, 0 1 0)), ((0 0 1, 1 0 1, 1 1 1, 0 1 1, 0 0 1)) )')) ) As foo(geom);
--S28.验证函数ST_PointN返回几何的第N个点
SELECT ST_AsText(
ST_PointN(
column1,
generate_series(1, ST_NPoints(column1))
))
FROM ( VALUES ('LINESTRING(0 0, 1 1, 2 2)'::geometry) ) AS foo;
--S29.验证函数ST_SRID返回几何的空间参考值
SELECT ST_SRID(ST_GeomFromText('POINT(-71.1043 42.315)',4326));
--S30.验证函数ST_StartPoint返回几何的起始点
SELECT ST_AsText(ST_StartPoint('LINESTRING(0 1, 0 2)'::geometry));
--S31.验证函数ST_Summary返回几何的文本摘要
SELECT ST_Summary(ST_GeomFromText('LINESTRING(0 0, 1 1)')) as geom,
ST_Summary(ST_GeogFromText('POLYGON((0 0, 1 1, 1 2, 1 1, 0 0))')) geog;
--S32.验证函数ST_X返回该点的X坐标
SELECT ST_X(ST_GeomFromEWKT('POINT(1 2 3 4)'));
--S33.验证函数ST_XMax返回几何的X坐标最大值
SELECT ST_XMax('BOX3D(1 2 3, 4 5 6)');
--S34.验证函数ST_XMin返回几何的X坐标最小值
SELECT ST_XMin('BOX3D(1 2 3, 4 5 6)');
--S35.验证函数ST_Y返回该点的Y坐标
SELECT ST_Y(ST_GeomFromEWKT('POINT(1 2 3 4)'));
--S36.验证函数ST_YMax返回几何的Y坐标最大值
SELECT ST_YMax('BOX3D(1 2 3, 4 5 6)');
--S37.验证函数ST_YMin返回几何的Y坐标最小值
SELECT ST_YMin('BOX3D(1 2 3, 4 5 6)');
--S38.验证函数ST_Z返回该点的Z坐标
SELECT ST_Z(ST_GeomFromEWKT('POINT(1 2 3 4)'));
--S39.验证函数ST_ZMax返回几何的Z坐标最大值
SELECT ST_ZMax('BOX3D(1 2 3, 4 5 6)');
--S40.验证函数ST_Zmflag返回几何的ZM标志
SELECT ST_Zmflag(ST_GeomFromEWKT('LINESTRING(1 2, 3 4)'));
--S41.验证函数ST_ZMin返回几何的Z坐标最小值
SELECT ST_ZMin('BOX3D(1 2 3, 4 5 6)');

--I2.验证复杂函数join功能

/*
###############################################################################
# TESTCASE NAME : postgis_constructors.sql
# COMPONENT(S)  : test function constructors
# PREREQUISITE  : 
# PLATFORM      : SUSE11.3
# DESCRIPTION   : 验证几何对象创建功能
# TAG           : 
# TC LEVEL      : Level 1
################################################################################
*/

--ST_BdPolyFromText 给定任意集合的封闭线串作为MultiLineStringWell-已知文本形式构造一个多边形

--ST_BdMPolyFromText 给定一个MultiLolyString文本表示的闭合线串的任意集合构造一个MultiPolygon众所周知的文本表示

--S1.验证函数ST_Box2dFromGeoHash：从GeoHash字符串中返回一个BOX2D值，第二个参数值越大，精度越高
SELECT ST_Box2dFromGeoHash(ST_GeoHash(the_geom), 5) from geom_point where id=10;
--S2.验证函数ST_GeogFromText：给定WKT返回指定的地理值。
SELECT ST_GeogFromText(ST_AsText(the_geom)) from geom_line where id=15;
--S3.验证函数ST_GeographyFromText：给定WKT返回指定的地理值
SELECT ST_GeographyFromText(ST_AsText(the_geom)) from geom_polygon where id<5 order by id;
--S4.验证函数ST_GeogFromWKB：给定WKB或EWKB返回地理值
SELECT ST_GeogFromWKB(ST_AsBinary(the_geom)) from geom_point where id=11;
--S5.验证函数ST_GeomCollFromText：给定集合WKT创建一个集合几何体。如果没有给出SRID，则默认为0
SELECT ST_GeomCollFromText(ST_AsText(the_geom)) from geom_collection where id=10;
--S6.验证函数ST_GeomFromEWKB：给定EWKB返回指定的ST_Geometry值
SELECT ST_GeomFromEWKB(St_AsEWKB(the_geom)) from geom_point where id < 20 order by id;
--S7.验证函数ST_GeomFromEWKT：给定EWKT返回指定的ST_Geometry值
SELECT ST_GeomFromEWKT(ST_AsEWKT(the_geom)) from geom_point where id=1;
SELECT ST_GeomFromEWKT(St_AsEWKT(the_geom)) from geom_multipolygon where id<5 order by id;
--S8.验证函数ST_GeometryFromText：给定几何的Text表示格式输出几何
SELECT ST_GeometryFromText(ST_AsTExt(the_geom)) from geom_polygon where name='ZZZZ' order by id;
--S9.验证函数ST_GeomFromGeoHash：给定GeoHash字符串返回几何
SELECT ST_AsText(ST_GeomFromGeoHash(ST_GeoHash(the_geom))) from geom_point order by id limit 10;
--S10.验证函数ST_GeomFromGML：给定几何体的GML表示形式输出PostGIS几何体对象
SELECT ST_AsText(ST_GeomFromGML(ST_AsGML(the_geom))) from geom_multilinestring order by id limit 10;
--s11.验证函数ST_GeomFromGeoJSON：给定几何JSON表示形式输出几何体对象
SELECT 'ST_AsGeoJson',ST_GeomFromGeoJSON(ST_AsGeoJson(the_geom)) from geom_line where citiId=100 ;
SELECT ST_AsText(ST_GeomFromGeoJSON('{"type":"LineString","coordinates ":[[1,2,3],[4,5,6],[7,8,9]]}')) As wkt;
--s12.验证函数ST_GeomFromKML：给定几何的KML表示形式输出几何对象
SELECT ST_AsText(ST_GeomFromKML(ST_AsKML(the_geom)))  from geom_multilinestring order by id limit 10;;
--S13.验证函数ST_GeomFromText：给定几何Text表示形式输出几何对象
SELECT ST_GeomFromText(ST_AsText(the_geom)) from geom_point where name='UUUU';
--S14.验证函数ST_GeomFromWKB：给定几何WKB表示形式格式输出几何对象
SELECT ST_GeomFromWKB(ST_AsEWKB(the_geom)) from geom_multipolygon  where id<10 order by id;
--S15.验证函数ST_LineFromMultiPoint：给定多个点返回一条线
SELECT ST_LineFromMultiPoint(the_geom) from geom_multipoint where id=18;
--S16.验证函数ST_LineFromText：给定线条的WKB表示形式，返回一条线
SELECT ST_LineFromText(ST_Astext(the_geom)) from geom_Line AS null_return where name='AAAA' ;
--S17.验证函数ST_LineStringFromWKB：给定线条WKB格式返回一条线
SELECT
ST_LineStringFromWKB(
ST_AsBinary(the_geom)) from geom_point AS geom_return where id=22 ;
--S18.验证函数ST_MakeBox2D：给定几何点创建 box2d
select ST_AsText(ST_SetSRID(ST_MakeBox2D(ST_Point(-989502.1875, 528439.5625), ST_Point(-987121.375 ,529933.1875)),2163)); 
--S19.验证函数ST_3DMakeBox：给定几何点创建box3d
SELECT ST_3DMakeBox(ST_MakePoint(-989502.1875, 528439.5625, 10),
ST_MakePoint(-987121.375 ,529933.1875, 10)) As abb3d;
--S20.验证函数ST_MakeLine：给定点创建线
SELECT ST_AsText(ST_MakeLine(ST_MakePoint(1,2), ST_MakePoint(3,4)));
--S21.验证函数ST_MakeEnvelope：给定x坐标最小值最大值，y坐标最小值最大值创建矩形
SELECT ST_AsText(ST_MakeEnvelope(10, 10, 11, 11,4326));                                                                                                                                  --S22.验证函数ST_MakePolygon：给定线条创建多边形
SELECT ST_MakePolygon(ST_GeomFromText(ST_AsText(the_geom))) from geom_line where id=9;
--S23.验证函数ST_MakePoint：给定x，y，z，m坐标创建点
SELECT ST_AsText(ST_MakePoint(1, 2,1.5,3));
--S24.验证函数ST_MakePointM：给定x，y，m坐标创建点
SELECT ST_AsEWKT(ST_MakePointM(-71.1043443253471, 42.3150676015829, 10));
--S25.验证函数ST_MLineFromText：给定WKT返回指定的multilinestring
SELECT ST_AsText(ST_MLineFromText(ST_AsText(the_geom))) from geom_multilinestring where id=25;
--S26.验证函数ST_MPointFromText：给定WKT返回指定的multipoint
SELECT ST_AsText(ST_MPointFromText(ST_AsText(the_geom))) from geom_multipoint where id=19;
--S27.验证函数ST_MPolyFromText：给定WKT返回指定的multipolygon
SELECT ST_AsText(ST_MPolyFromText(ST_AsText(the_geom))) from geom_multipolygon where id<5 order by id;

/*
###############################################################################
# TESTCASE NAME : postgis_geometry_editors.sql
# COMPONENT(S)  : test function geometry_editors
# PREREQUISITE  : 
# PLATFORM      : SUSE11.3
# DESCRIPTION   : 验证postgis函数编辑功能
# TAG           : ST_AddPoint,ST_Affine,ST_Force2D,ST_LineMerge,ST_Rotate
# TC LEVEL      : Level 1
################################################################################
*/

--S1.验证函数ST_AddPoint添加一个点到linestring
SELECT ST_AsEWKT(ST_AddPoint(ST_GeomFromEWKT(ST_AsText(the_geom)), ST_MakePoint (1, 2, 3), 1)) from geom_line where geom_line.id=1;
SELECT ST_AsEWKT(ST_AddPoint(ST_GeomFromEWKT(ST_AsText(the_geom)),table_1.point_1, 1))  from geom_line,(select ST_AsText(the_geom) as point_1 from geom_point where id=1) table_1 where geom_line.id=1;
SELECT ST_AsEWKT(ST_AddPoint(ST_GeomFromEWKT('LINESTRING(0 0 1, 1 1 1)'), ST_MakePoint (1, 2, 3)));
--S2.验证函数ST_Affine几何三维仿射变换
SELECT ST_AsEWKT(ST_Affine(the_geom, cos(pi()), -sin(pi()), 0, sin(pi()), cos(pi()), 0, 0, 0, 1, 0, 0, 0)) As using_affine,
	   ST_AsEWKT(ST_Rotate(the_geom, pi())) As using_rotate
    FROM (SELECT ST_GeomFromEWKT('LINESTRING(1 2 3, 1 4 3)') As the_geom) As foo;
SELECT ST_AsEWKT(ST_Affine(the_geom, cos(pi()), -sin(pi()), 0, sin(pi()), cos(pi()), -sin(pi()), 0, sin(pi()), cos(pi()), 0, 0, 0))
	FROM (SELECT ST_GeomFromEWKT('LINESTRING(1 2 3, 1 4 3)') As the_geom) As foo;
--S3.验证函数ST_Force2D几何强制转换为2D表示
SELECT ST_AsEWKT(ST_Force2D(ST_GeomFromEWKT('CIRCULARSTRING(1 1 2, 2 3 2, 4 5 2, 6 7 2, 5 6 2)')));
SELECT ST_AsEWKT(ST_Force2D(the_geom)) from geom_polygon where id<5 order by citiID;
SELECT ST_AsEWKT(ST_Force2D(the_geom)) from geom_line where citiID < 5 order by id;
--S4.验证函数ST_Force3D几何强制转换为3D表示
SELECT ST_AsEWKT(ST_Force3D(ST_GeomFromEWKT('CIRCULARSTRING(1 1 2, 2 3 2, 4 5 2, 6 7 2, 5 6 2)')));
SELECT ST_AsEWKT(ST_Force3D(the_geom)) from geom_polygon where name='CCCC' order by id;
--S5.验证函数ST_Force3DZ几何强制转换为3DZ表示
SELECT ST_AsEWKT(ST_Force3DZ(ST_GeomFromEWKT('CIRCULARSTRING(1 1 2, 2 3 2, 4 5 2, 6 7 2, 5 6 2)')));
SELECT ST_AsEWKT(ST_Force3DZ(the_geom)) from geom_polygon where name='BBBB' order by id;
--S6.验证函数ST_Force3DM几何强制转换3DM表示
SELECT ST_AsEWKT(ST_Force3DM(ST_GeomFromEWKT('CIRCULARSTRING(1 1 2, 2 3 2, 4 5 2, 6 7 2, 5 6 2)')));
SELECT ST_AsEWKT(ST_Force3DM(the_geom)) from geom_multipolygon order by id limit 10;
--S7.验证函数ST_Force4D几何强制转换4D表示
SELECT ST_AsEWKT(ST_Force4D(ST_GeomFromEWKT('CIRCULARSTRING(1 1 2, 2 3 2, 4 5 2, 6 7 2, 5 6 2)')));
SELECT ST_AsEWKT(ST_Force4D(the_geom)) from geom_line order by id limit 10;
--S8.验证函数ST_ForceCollection几何强制转换集合表示
SELECT ST_AsEWKT(ST_ForceCollection('POLYGON((0 0 1,0 5 1,5 0 1,0 0 1),(1 1 1,3 1 1,1 3 1,1 1 1))'));
SELECT ST_AsText(ST_ForceCollection('CIRCULARSTRING(220227 150406,2220227 150407,220227 150406)'));
SELECT ST_AsEWKT(ST_ForceCollection('POLYHEDRALSURFACE(((0 0 0,0 0 1,0 1 1,0 1 0,0 0 0)),
((0 0 0,0 1 0,1 1 0,1 0 0,0 0 0)),
((0 0 0,1 0 0,1 0 1,0 0 1,0 0 0)),
((1 1 0,1 1 1,1 0 1,1 0 0,1 1 0)),
((0 1 0,0 1 1,1 1 1,1 1 0,0 1 0)),
((0 0 1,1 0 1,1 1 1,0 1 1,0 0 1)))'));
--S9.ST_ForceSFS
--S10.验证函数ST_ForceRHR几何强制装换RHR表示
SELECT ST_AsEWKT(ST_ForceRHR(the_geom)) from geom_polygon order by id limit 7;
--S11.验证函数ST_LineMerge合并多条线
SELECT ST_AsText(ST_LineMerge(ST_GeomFromText(ST_AsText(the_geom)))) from geom_multilinestring where id=3;
--S12.验证函数ST_CollectionExtract截取指定类型元素的几何
SELECT ST_AsText(ST_CollectionExtract(ST_GeomFromText('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(0 0)))'),1));
SELECT ST_AsText(ST_CollectionExtract(ST_GeomFromText('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1)),LINESTRING(2 2, 3 3))'),2));

--S13.验证函数ST_CollectionHomogenize返回几何最简单表示
SELECT ST_AsText(ST_CollectionHomogenize('GEOMETRYCOLLECTION(POINT(0 0))'));
SELECT ST_AsText(ST_CollectionHomogenize('GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0, 1 1))'));
--S14.验证函数ST_Multi返回几何multi表示
SELECT ST_AsText(ST_Multi(ST_GeomFromText(ST_Astext(the_geom)))) from geom_point where id<5 order by id;
SELECT ST_AsText(ST_Multi(ST_GeomFromText(ST_Astext(the_geom)))) from geom_line where id<5 order by id;
SELECT ST_AsText(ST_Multi(ST_GeomFromText(ST_Astext(the_geom)))) from geom_polygon where id<5 order by id;
SELECT ST_AsText(ST_Multi(ST_GeomFromText(ST_Astext(the_geom)))) from geom_multipoint where id<5 order by id;
--S15.验证函数ST_RemovePoint从线上移除一个点
SELECT ST_RemovePoint(ST_GeomFromEWKT('LINESTRING(1 2 3, 1 4 3, 1 5 5)'), 1);
SELECT ST_RemovePoint(ST_GeomFromEWKT(the_geom), 1) from geom_line where id=7;
SELECT ST_RemovePoint(ST_GeomFromEWKT('point(1 2 3)'), 1);   
--S16.验证函数ST_Reverse翻转几何图形
SELECT ST_AsText(the_geom) as line, ST_AsText(ST_Reverse(the_geom)) As reverseline FROM (SELECT ST_MakeLine(ST_MakePoint(1,2), ST_MakePoint(1,10)) As the_geom) as foo;
--S17.验证函数ST_Rotate围绕原点逆时针旋转的几何
SELECT ST_AsEWKT(ST_Rotate(the_geom, pi())) from geom_point where id=6;
SELECT ST_AsEWKT(ST_Rotate(the_geom, -pi()/3)) from geom_line where id=6;
SELECT ST_AsEWKT(ST_Rotate(the_geom, pi()/2)) from geom_polygon where id=6;
SELECT ST_AsEWKT(ST_Rotate(the_geom, -pi())) from geom_multilinestring where id=6;

--S18.验证函数ST_RotateX围绕X轴旋转的几何
SELECT ST_AsEWKT(ST_RotateX(the_geom, pi()/2)) from geom_point order by id limit 10;
SELECT ST_AsEWKT(ST_RotateX(the_geom, -pi()/3)) from geom_line order by id limit 5;
SELECT ST_AsEWKT(ST_RotateX(the_geom, pi()/2)) from geom_polygon where id=9;
--S19.验证函数ST_RotateY围绕Y轴旋转的几何
SELECT ST_AsEWKT(ST_RotateY(the_geom, pi()/2)) from geom_multipoint where name='VVVV' order by id;
SELECT ST_AsEWKT(ST_RotateY(the_geom, pi())) from geom_multilinestring where name='CCCC' order by id;
--S20.验证函数ST_RotateZ围绕Z轴旋转的几何
SELECT ST_AsEWKT(ST_RotateZ(the_geom, pi()/2)) from geom_multipoint where name='VVVV' order by id;
SELECT ST_AsEWKT(ST_RotateZ(the_geom, pi()/2)) from geom_multipolygon where name='UUUU' order by citiID;
--S21.验证函数ST_Scale放大或缩小几何
SELECT ST_AsEWKT(ST_Scale(ST_GeomFromEWKT(ST_AsText(the_geom)), 0.5, 0.75, 0.8)) from geom_point where id=13;
SELECT ST_AsEWKT(ST_Scale(ST_GeomFromEWKT(ST_AsText(the_geom)), 0.5, 0.5, 0.1)) from geom_line where id=25;
SELECT ST_AsEWKT(ST_Scale(ST_GeomFromEWKT(ST_AsText(the_geom)), 0.3, 0.9, 0.4)) from geom_polygon where id=37;
--S22.验证函数ST_Segmentize
SELECT ST_AsText(ST_Segmentize(ST_GeomFromText(the_geom),5)) from geom_multilinestring order by id limit 10;
SELECT ST_AsText(ST_Segmentize(ST_GeomFromText(ST_AsText(the_geom)),10)) from geom_polygon where id<10 order by id;
--S23.验证函数ST_SetPoint替换线中的点
SELECT ST_AsText(ST_SetPoint(the_geom, 0, 'POINT(-1 1)')) from geom_line where id=5;
SELECT ST_AsEWKT(ST_SetPoint(foo.the_geom, ST_NumPoints(foo.the_geom) - 1, ST_GeomFromEWKT ('POINT(-1 1 3)')))
FROM (SELECT ST_GeomFromEWKT('LINESTRING(-1 2 3,-1 3 4, 5 6 7)') As the_geom) As foo;
--S24.验证函数ST_SetSRID设置指定SRID
SELECT ST_SetSRID(ST_Point(-123.365556, 48.428611),4326) As wgs84long_lat;
SELECT ST_Transform(ST_SetSRID(ST_Point(-123.365556, 48.428611),4326),3785) As spere_merc;
--S25.验证函数ST_SnapToGrid
SELECT ST_AsText(ST_SnapToGrid(
ST_GeomFromText('LINESTRING(1.1115678 2.123, 4.111111 3.2374897, 4.11112 3.23748667) 
'),
0.001)
);
SELECT ST_AsEWKT(ST_SnapToGrid(
ST_GeomFromEWKT('LINESTRING(-1.1115678 2.123 2.3456 1.11111,
4.111111 3.2374897 3.1234 1.1111, -1.11111112 2.123 2.3456 1.1111112)'),
ST_GeomFromEWKT('POINT(1.12 2.22 3.2 4.4444)'),
0.1, 0.1, 0.1, 0.01) );
SELECT ST_AsEWKT(ST_SnapToGrid(ST_GeomFromEWKT('LINESTRING(-1.1115678 2.123 3 2.3456,
4.111111 3.2374897 3.1234 1.1111)'),
0.01) );
--S26.验证函数ST_Snap
SELECT ST_AsText(ST_Snap(poly,line, ST_Distance(poly,line)*1.01)) AS polysnapped
FROM (SELECT
ST_GeomFromText('MULTIPOLYGON(
((26 125, 26 200, 126 200, 126 125, 26 125 ),
( 51 150, 101 150, 76 175, 51 150 ) ),
(( 151 100, 151 200, 176 175, 151 100 )))') As poly,
ST_GeomFromText('LINESTRING (5 107, 54 84, 101 100)') As line
) As foo;
SELECT ST_AsText(
ST_Snap(poly,line, ST_Distance(poly, line)*1.25)
) AS polysnapped
FROM (SELECT
ST_GeomFromText('MULTIPOLYGON(
(( 26 125, 26 200, 126 200, 126 125,26 125 ),
( 51 150, 101 150, 76 175, 51 150 ) ),
(( 151 100, 151 200, 176 175, 151 100 )))') As poly,
ST_GeomFromText('LINESTRING (5 107, 54 84, 101 100)') As line
) As foo;
SELECT ST_AsText(
ST_Snap(line, poly, ST_Distance(poly, line)*1.01)
) AS linesnapped
FROM (SELECT
ST_GeomFromText('MULTIPOLYGON(
((26 125, 26 200, 126 200, 126 125, 26 125),
(51 150, 101 150, 76 175, 51 150 )) ,
((151 100, 151 200, 176 175, 151 100)))') As poly,
ST_GeomFromText('LINESTRING (5 107, 54 84, 101 100)') As line
) As foo;
SELECT ST_AsText(
ST_Snap(line, poly, ST_Distance(poly, line)*1.25)
) AS linesnapped
FROM (SELECT
ST_GeomFromText('MULTIPOLYGON(
(( 26 125, 26 200, 126 200, 126 125, 26 125 ),
(51 150, 101 150, 76 175, 51 150 )),
((151 100, 151 200, 176 175, 151 100 )))') As poly,
ST_GeomFromText('LINESTRING (5 107, 54 84, 101 100)') As line
) As foo;
--S27.验证函数ST_Transform不同空间参考下的几何
SELECT ST_AsText(ST_Transform(ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,
743265 2967450,743265.625 2967416,743238 2967416))',2249),4326)) As wgs_geom;
SELECT ST_AsEWKT(ST_Transform(ST_GeomFromEWKT('SRID=2249;CIRCULARSTRING(743238 2967416 1,743238 2967450 2,743265 2967450 3,743265.625 2967416 3,743238 2967416 4)'),4326));
--S28.验证函数ST_Translate偏移几何
SELECT ST_AsText(ST_Translate(ST_GeomFromText('POINT(-71.01 42.37)',4326),1,0)) As wgs_transgeomtxt;
SELECT ST_AsText(ST_Translate(ST_GeomFromText('LINESTRING(-71.01 42.37,-71.11 42.38)',4326),1,0.5)) As wgs_transgeomtxt;
SELECT ST_AsEWKT(ST_Translate(CAST('POINT(0 0 0)' As geometry), 5, 12,3));
SELECT ST_AsText(ST_Translate(ST_Collect('CURVEPOLYGON(CIRCULARSTRING(4 3,3.12 0.878,1 0,-1.121 5.1213,6 7, 8 9,4 3))','POINT(1 3)'),1,2));
--S29.验证函数ST_TransScale放大或缩小几何
SELECT ST_AsEWKT(ST_TransScale(ST_GeomFromEWKT(ST_AsText(the_geom)), 0.5, 1, 1, 2)) from geom_line order by id limit 10;

/*
###############################################################################
# TESTCASE NAME : postgis_managementsql
# COMPONENT(S)  : test management function
# PREREQUISITE  : 无
# PLATFORM      : all
# DESCRIPTION   : 列存表添加几何列，删除几何列，查看各种库的版本信息
# TAG           : AddGeometryColumn，DropGeometryColumn，DropGeometryTable，version
# TC LEVEL      : Level 1
################################################################################
*/
--I1验证行存表添加和删除几何列的功能
--S1清理以前创建的表
DROP TABLE IF EXISTS my_spatial_table;
--S2创建行存表表字段为id，cities
CREATE TABLE IF NOT EXISTS my_spatial_table (id serial, cities varchar(20));
--S3使用postgis函数为创建的表添加几何列，参数依次为schema，table，几何列名，SRID，几何类型，坐标维度
SELECT AddGeometryColumn ('my_spatial_table','geom',4326,'POINT',2);
--S4插入几何数据
INSERT INTO my_spatial_table(cities, geom) VALUES('xian', ST_GeomFromText('POINT(1 2)',4326) );
--S5查询插入的数据
SELECT ST_AsEWKT(ST_SetSRID(geom, 3857)) from my_spatial_table where id=1;
--S6Populate_Geometry_Columns 确保几何列使用类型修饰符定义或具有适当的空间约束
SELECT Populate_Geometry_Columns('my_spatial_table'::regclass);
--S7UpdateGeometrySRID 更新几何列中所有要素的SRID，geometry_columns元数据和srid
SELECT UpdateGeometrySRID('my_spatial_table','geom',3857);
--S8使用postgis函数从表中删除几何列
SELECT DropGeometryColumn ('my_spatial_table','geom');
--S9查询删除的几何列不存在
SELECT geom from my_spatial_table;
--S10DropGeometryTable 在geometry_columns中删除表及其所有引用
SELECT DropGeometryTable ('my_spatial_table');
--S11.查询删除的表不存在
SELECT geom from my_spatial_table;

--I2验证查看版本信息函数功能
--S1验证函数PostGIS_Full_Version返回postgis版本和构建的配置信息
SELECT PostGIS_Full_Version();
--S2验证函数PostGIS_GEOS_Version返回GEOS库的版本号
SELECT PostGIS_GEOS_Version();
--S3验证函数PostGIS_LibXML_Version返回libxml2库的版本号
SELECT PostGIS_LibXML_Version();
--S5验证函数PostGIS_Lib_Version返回PostGis库的版本号
SELECT PostGIS_Lib_Version();
--S6验证函数PostGIS_PROJ_Version返回PROJ4库的版本号
SELECT PostGIS_PROJ_Version();
--S7验证函数PostGIS_Scripts_Build_Date返回PostGis scripts的创建时间
SELECT PostGIS_Scripts_Build_Date();
--S8验证函数PostGIS_Scripts_Installed返回此数据库中安装的postgis脚本的版本
SELECT PostGIS_Scripts_Installed();
--S9验证函数PostGIS_Scripts_Released返回随安装的postgis库发布的postgissql脚本的版本号
SELECT PostGIS_Scripts_Released();
--S10验证函数PostGIS_Version返回postgis版本号和编译时选项
SELECT PostGIS_Version();

SELECT (g.gdump).path, ST_AsEWKT((g.gdump).geom) as wkt
FROM
(SELECT
ST_DumpPoints(ST_GeomFromEWKT('POLYHEDRALSURFACE( ((0 0 0, 0 0 1, 0 1 1, 0 1 0, 0 0 
0)),
((0 0 0, 0 1 0, 1 1 0, 1 0 0, 0 0 0)), ((0 0 0, 1 0 0, 1 0 1, 0 0 1, 0 0 0)),
((1 1 0, 1 1 1, 1 0 1, 1 0 0, 1 1 0)),
((0 1 0, 0 1 1, 1 1 1, 1 1 0, 0 1 0)), ((0 0 1, 1 0 1, 1 1 1, 0 1 1, 0 0 1)) )') ) AS gdump
) AS g;
SELECT Box2D(ST_GeomFromText('LINESTRING(1 2, 3 4, 5 6)'));
SELECT Box2D(ST_GeomFromText('CIRCULARSTRING(220268 150415,220227 150505,220227 150406)') 
);
select '114', ST_perimeter2d('MULTIPOLYGON( ((0 0, 10 0, 10 10, 0 10, 0 0)),( (0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7 , 5 7, 5 5) ) ,( (0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 7 5, 7 7, 5 7, 5 5),(1 1,2 1, 2 2, 1 2, 1 1) ) )'::GEOMETRY) as value;
select  ST_GeogFromText('POINT(-69.83262 43.43636)') ;
select  ST_GeogFromText('SRID=4326;POINT(-11.1111111 40)');
SELECT ST_AsText(ST_SnapToGrid(
ST_GeomFromText('LINESTRING(1.1115678 2.123, 4.111111 3.2374897, 4.11112 3.23748667) 
'),
0.001)
);
SELECT ST_AsEWKT(ST_SnapToGrid(
ST_GeomFromEWKT('LINESTRING(-1.1115678 2.123 2.3456 1.11111,
4.111111 3.2374897 3.1234 1.1111, -1.11111112 2.123 2.3456 1.1111112)'),
ST_GeomFromEWKT('POINT(1.12 2.22 3.2 4.4444)'),
0.1, 0.1, 0.1, 0.01) );
SELECT ST_AsEWKT(ST_SnapToGrid(ST_GeomFromEWKT('LINESTRING(-1.1115678 2.123 3 2.3456,
4.111111 3.2374897 3.1234 1.1111)'),
0.01) );

SELECT ST_Length_Spheroid( the_geom, sph_m ) As tot_len,
ST_Length_Spheroid(ST_GeometryN(the_geom,1), sph_m) As len_line1,
ST_Length_Spheroid(ST_GeometryN(the_geom,2), sph_m) As len_line2
FROM (SELECT ST_GeomFromText('MULTILINESTRING((-118.584 38.374,-118.583 38.5),
(-71.05957 42.3589 , -71.061 43))') As the_geom,
CAST('SPHEROID["GRS_1980",6378137,298.257222101]' As spheroid) As sph_m) as foo;

SELECT ST_Length_Spheroid( the_geom, sph_m ) As tot_len,
ST_Length_Spheroid(ST_GeometryN(the_geom,1), sph_m) As len_line1,
ST_Length_Spheroid(ST_GeometryN(the_geom,2), sph_m) As len_line2
FROM (SELECT ST_GeomFromEWKT('MULTILINESTRING((-118.584 38.374 20,-118.583 38.5 30) 
,
(-71.05957 42.3589 75, -71.061 43 90))') As the_geom,
CAST('SPHEROID["GRS_1980",6378137,298.257222101]' As spheroid) As sph_m) as foo;
SELECT ST_AsText(ST_ClosestPoint(pt,line) 
) AS cp_pt_line,
ST_AsText(ST_ClosestPoint(line,pt 
)) As cp_line_pt
FROM (SELECT 'POINT(100 100)'::geometry 
As pt,
'LINESTRING (20 80, 98 
190, 110 180, 50 75 )'::geometry As line
) As foo;

SELECT ST_DFullyWithin(geom_a, geom_b, 10) as DFullyWithin10, ST_DWithin(geom_a, 
geom_b, 10) as DWithin10, ST_DFullyWithin(geom_a, geom_b, 20) as DFullyWithin20 from
(select ST_GeomFromText('POINT(1 1)') as geom_a,ST_GeomFromText('LINESTRING(1 5, 2 7, 1 
9, 14 12)') as geom_b) t1;
SELECT '#146', ST_Distance(g1,g2), ST_Dwithin(g1,g2,0.01), ST_AsEWKT(g2) FROM (SELECT ST_geomFromEWKT('LINESTRING(1 2, 2 4)') As g1, ST_Collect(ST_GeomFromEWKT('LINESTRING(0 0, -1 -1)'), ST_GeomFromEWKT('MULTIPOINT(1 2,2 3)')) As g2) As foo;
 
SELECT '#657.3',ST_DWithin(ST_Project('POINT(10 10)'::geography, 2000, pi()/2), 'POINT(10 10)'::geography, 2000);
SELECT '#1264', ST_DWithin('POLYGON((-10 -10, -10 10, 10 10, 10 -10, -10 -10))'::geography, 'POINT(0 0)'::geography, 0);
SELECT ST_Intersects('POINT(0 0)'::geometry, 'LINESTRING ( 2 0, 0 2 )'::geometry);

SELECT ST_Intersects('POINT(0 0)'::geometry, 'LINESTRING ( 0 0, 0 2 )'::geometry);

SELECT ST_Intersects(
ST_GeographyFromText('SRID=4326;LINESTRING(-43.23456 72.4567,-43.23456 72.4568)'),
ST_GeographyFromText('SRID=4326;POINT(-43.23456 72.4567772)')
);

SELECT ST_AsEWKT(ST_Affine(the_geom, cos(pi()), -sin(pi()), 0, sin(pi()), cos(pi()), 0, 
0, 0, 1, 0, 0, 0)) As using_affine,
ST_AsEWKT(ST_Rotate(the_geom, pi())) As using_rotate
FROM (SELECT ST_GeomFromEWKT('LINESTRING(1 2 3, 1 4 3)') As the_geom) As foo;
select '#1023.b', postgis_addbbox('POINT(10 4)'::geometry) = 'POINT(10 4)'::geometry;
SELECT 'T1B', ST_Summary(postgis_addbbox('POINT(0 0)'::geometry));
SELECT 'T1ZMB', ST_Summary(postgis_addbbox('POINT(0 0 0 0)'::geometry));
SELECT ST_Length2D(ST_GeomFromText('LINESTRING(743238 2967416,743238 2967450,743265 2967450,
743265.625 2967416,743238 2967416)',2249));
SELECT ST_AsText(ST_Centroid('MULTIPOINT ( -1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 
0, 6 0, 7 8, 9 8, 10 6 )'));

SELECT ST_PolygonFromText('POLYGON((-71.1776585052917 42.3902909739571,-71.1776820268866 
42.3903701743239,
-71.1776063012595 42.3903825660754,-71.1775826583081 42.3903033653531,-71.1776585052917 
42.3902909739571))');

SELECT ST_PolygonFromText('POINT(1 2)') IS NULL as point_is_notpoly;
SELECT ST_AsEWKT(ST_Force3D(ST_GeomFromEWKT('CIRCULARSTRING(1 1 2, 2 3 2, 4 5 2, 6 7 2, 
5 6 2)')));
SELECT ST_AsEWKT(ST_Force3D('POLYGON((0 0,0 5,5 0,0 0),(1 1,3 1,1 3,1 1))'));
SELECT ST_Npoints(the_geom) As np_before, ST_NPoints(ST_Simplify(the_geom,0.1)) As 
np01_notbadcircle, ST_NPoints(ST_Simplify(the_geom,0.5)) As np05_notquitecircle,
ST_NPoints(ST_Simplify(the_geom,1)) As np1_octagon, ST_NPoints(ST_Simplify(the_geom,10)) As 
np10_triangle,
(ST_Simplify(the_geom,100) is null) As np100_geometrygoesaway
FROM (SELECT ST_Buffer('POINT(1 3)', 10,12) As the_geom) As foo;

