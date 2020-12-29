/*
###############################################################################
# TESTCASE NAME : postgis_outputs.sql
# COMPONENT(S)  : test function outputs
# PREREQUISITE  : 
# PLATFORM      : SUSE11.3
# DESCRIPTION   : 验证postgis函数中几何的不同格式输出
# TAG           : ST_AsBinary,ST_AsEWKB,ST_AsEWKT,ST_AsGeoJSON,ST_AsTexti,ST_GeoHash
# TC LEVEL      : 
################################################################################
*/

set current_schema=postgis;
--S1.验证函数ST_AsBinary输出几何的二进制表示格式
SELECT '579',ST_AsBinary(the_geom) from geom_polygon where name='AAAA';
--S2.验证函数ST_AsEWKB输出几何扩展二进制表示格式
SELECT '581',ST_AsEWKB(the_geom) as st_asewkb from geom_line order by name, st_asewkb limit 10;
--S3.验证函数ST_AsEWKT输出几何的扩展文本表示格式
SELECT '583',ST_AsEWKT(the_geom) from geom_point order by citiID limit 10;
--S4.验证函数ST_AsGeoJSON输出几何的json表示格式
SELECT ST_AsGeoJSON(the_geom) from geom_multipoint order by id limit 1;
--S5.验证函数ST_AsGML输出几何的GML表示格式
SELECT ST_AsGML(ST_GeomFromText(ST_AsText(the_geom))) from geom_multipolygon where citiID<10 order by id;
--S6.验证函数ST_AsHEXEWKB输出几何HEXEWKB表示格式
SELECT ST_AsHEXEWKB(ST_GeomFromText(ST_AsText(the_geom))) from geom_multilinestring where id=13;
--S7.验证函数ST_AsKML输出几何KML表示格式
SELECT ST_AsKML(ST_GeomFromText(ST_AsText(the_geom))) from geom_multipolygon where id=5;
--S8.验证函数ST_AsLatLonText输出几何度分秒表示
SELECT (ST_AsLatLonText(ST_AsText(the_geom), 'D\textdegree{}M''S.SSS"C')) from geom_point where id<10 order by id; 
--S9.验证函数ST_AsSVG输出几何SVG表示格式
SELECT ST_AsSVG(ST_GeomFromText(ST_AsText(the_geom),4326)) from geom_point order by id limit 10;
--S10.验证函数ST_AsText输出几何文本表示格式
SELECT ST_AsText(the_geom) from geom_multipolygon order by id limit 10;
--S11.验证函数ST_AsX3D输出几何3D表示格式
SELECT '<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE X3D PUBLIC "ISO//Web3D//DTD X3D 3.0//EN" "http://www.web3d.org/specifications/x3d-3.0.dtd">
<X3D>
<Scene>
<Transform>
<Shape>
<Appearance>
<Material emissiveColor=''0 0 1''/>
</Appearance> ' ||
ST_AsX3D( ST_GeomFromEWKT('POLYHEDRALSURFACE( ((0 0 0, 0 0 1, 0 1 1, 0 1 0, 0 0 0)),
((0 0 0, 0 1 0, 1 1 0, 1 0 0, 0 0 0)), ((0 0 0, 1 0 0, 1 0 1, 0 0 1, 0 0 0)),
((1 1 0, 1 1 1, 1 0 1, 1 0 0, 1 1 0)),
((0 1 0, 0 1 1, 1 1 1, 1 1 0, 0 1 0)), ((0 0 1, 1 0 1, 1 1 1, 0 1 1, 0 0 1)) )')) ||
'</Shape>
</Transform>
</Scene>
</X3D>' As x3ddoc;
--S12.验证函数ST_GeoHash输出几何Hash表示格式
SELECT ST_GeoHash(the_geom) from geom_multipoint where name='UUUU';


/*
###############################################################################
# TESTCASE NAME : postgis_processing.sql
# COMPONENT(S)  : test function processing
# PREREQUISITE  : 
# PLATFORM      : SUSE11.3
# DESCRIPTION   : 验证postgis几何处理函数
# TAG           : ST_Buffer,ST_BuildArea,ST_Collect
################################################################################
*/
--I1.processing简单语句测试
--S1.验证函数ST_Buffer返回几何体给定距离内所有点的集合
SELECT ST_Buffer(ST_GeomFromText(the_geom),50, 'quad_segs=8') from geom_point where id=5;
SELECT ST_Buffer(ST_GeomFromText('LINESTRING(50 50,150 150,150 50)'), 10, 'endcap=round join=round');
SELECT ST_Buffer(ST_GeomFromText(ST_AsText(the_geom)),50, 'quad_segs=8') from geom_polygon where id<10 order by id;
--S2.验证函数ST_BuildArea创建给定几何体的组成线条形成的面状几何
SELECT ST_BuildArea(ST_Collect(smallc,bigc))FROM (SELECT ST_Buffer(ST_GeomFromText(tb1.geom), 25) As smallc,ST_Buffer(ST_GeomFromText(tb2.geom), 50) as bigc from (select ST_Astext(the_geom) as geom from geom_line where id=6) tb1, (select ST_Astext(the_geom) as geom from geom_line where id=15) tb2);
--S3.验证函数ST_Collect从其他几何体的集合中返回指定的geometry值
SELECT ST_AsText(ST_Collect(ST_GeomFromText(tb1.geom),ST_GeomFromText(tb2.geom))) from (select ST_AsText(the_geom) as geom from geom_point where id=7) tb1,(select ST_AsText(the_geom) as geom from geom_line where id=9) tb2;
--S4.验证函数ST_ConcaveHull几何的凹形表示可能包含组内所有几何的凹形几何
SELECT d.name,ST_ConcaveHull(ST_Collect(d.the_geom), 0.99) As geom FROM geom_polygon As d GROUP BY d.id,d.name order by d.name limit 10;
--S6.验证函数ST_CurveToLine将一个循环/曲线转换成线/多边形
SELECT ST_AsText(ST_CurveToLine(ST_GeomFromText('CIRCULARSTRING(220268 150415,220227 150505,220227 150406)')));
--S7.验证函数ST_DelaunayTriangles返回给定输入点周围的三角部分
SELECT ST_DelaunayTriangles(ST_Union(ST_GeomFromText('POLYGON((175 150, 20 40,50 60, 125 100, 175 150))'),ST_Buffer(ST_GeomFromText('POINT(110 170)'), 20)))As dtriag;
--S8.验证函数ST_Difference返回表示与几何B不相交的几何A的部分几何
SELECT ST_AsText(ST_Difference(ST_GeomFromText('LINESTRING(50 100, 50 200)'),ST_GeomFromText('LINESTRING(50 50, 50 150)')));
--S9.验证函数ST_Dump返回组成几何的一组geometry_dump行


SELECT ST_AsEWKT(ST_Difference(ST_GeomFromEWKT('MULTIPOINT(-118.58 38.38 5,-118.60 38.329 
6,-118.614 38.281 7)'), ST_GeomFromEWKT('POINT(-118.614 38.281 5)')));
SELECT ST_AsText(ST_EndPoint('LINESTRING(1 1, 2 2, 3 3)'::geometry));
SELECT ST_AsEWKT(ST_EndPoint('LINESTRING(1 1 2, 1 2 3, 0 0 5)'));
T ST_3DDistance(
ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 4)'),2163),
ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 
20)'),2163)
) As dist_3d,
ST_Distance(
ST_Transform(ST_GeomFromText('POINT(-72.1235 42.3521)',4326),2163),
ST_Transform(ST_GeomFromText('LINESTRING(-72.1260 42.45, -72.123 42.1546)', 4326) 
,2163)
) As dist_2d;

SELECT ST_3DDistance(poly, mline) As dist3d,
ST_Distance(poly, mline) As dist2d
FROM (SELECT ST_GeomFromEWKT('POLYGON((175 150 5, 20 40 5, 35 45 5, 50 60 5, 100 
100 5, 175 150 5))') As poly,
ST_GeomFromEWKT('MULTILINESTRING((175 155 2, 20 40 20, 50 60 -2, 125 100 1, 
175 155 1),
(1 10 2, 5 20 1))') As mline ) As foo;
SELECT ST_GeomCollFromText('GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(1 2, 3 4))');
SELECT ST_GeomCollFromText('GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(1 2, 3 4))',4326);
SELECT ST_AsText(ST_MakeLine(ST_MakePoint(1,2), ST_MakePoint(3,4)));
SELECT ST_AsEWKT(ST_MakeLine(ST_MakePoint(1,2,3), ST_MakePoint(3,4,5)));
SELECT ST_AsText(ST_Envelope('POINT(1 3)'::geometry));
SELECT ST_AsText(ST_Envelope('LINESTRING(0 0, 1 3)'::geometry));
SELECT Box3D(geom), Box2D(geom), ST_AsText(ST_Envelope(geom)) As envelopewkt
FROM (SELECT 'POLYGON((0 0, 0 1000012333334.34545678, 1.0000001 1, 1.0000001 0, 0 0))':: 
geometry As geom) As foo;
SELECT ST_Mem_Size(ST_GeomFromText('CIRCULARSTRING(220268 150415,220227 150505,220227 150406)'));
SELECT ST_MPointFromText('MULTIPOINT(1 2, 3 4)');
SELECT ST_MPointFromText('MULTIPOINT(-70.9590 42.1180, -70.9611 42.1223)', 4326);
SELECT tbl1.column1, tbl2.column1, tbl1.column2 |&> tbl2.column2 AS overabove
FROM
( VALUES 
(1, 'LINESTRING(6 0, 6 4)'::geometry)) AS tbl1,
( VALUES
(2, 'LINESTRING(0 0, 3 3)'::geometry),
(3, 'LINESTRING(0 1, 0 5)'::geometry),
(4, 'LINESTRING(1 2, 4 6)'::geometry)) AS tbl2;

SELECT ST_AsText(ST_Transform(ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,
743265 2967450,743265.625 2967416,743238 2967416))',2249),4326)) As wgs_geom;
SELECT ST_AsEWKT(ST_Transform(ST_GeomFromEWKT('SRID=2249;CIRCULARSTRING(743238 2967416 
1,743238 2967450 2,743265 2967450 3,743265.625 2967416 3,743238 2967416 4)'),4326));

select 'LINESTRING(0 0, 1 1)'::geometry ~= 'LINESTRING(0 1, 1 0)'::geometry as equality;
SELECT '<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE X3D PUBLIC "ISO//Web3D//DTD X3D 3.0//EN" "http://www.web3d.org/specifications/x3d-3.0.dtd">
<X3D>
<Scene>
<Transform>
<Shape>
<Appearance>
<Material emissiveColor=''0 0 1''/>
</Appearance> ' ||
ST_AsX3D( ST_GeomFromEWKT('POLYHEDRALSURFACE( ((0 0 0, 0 0 1, 0 1 1, 0 1 0, 0 0 0)),
((0 0 0, 0 1 0, 1 1 0, 1 0 0, 0 0 0)), ((0 0 0, 1 0 0, 1 0 1, 0 0 1, 0 0 0)),
((1 1 0, 1 1 1, 1 0 1, 1 0 0, 1 1 0)),
((0 1 0, 0 1 1, 1 1 1, 1 1 0, 0 1 0)), ((0 0 1, 1 0 1, 1 1 1, 0 1 1, 0 0 1)) )')) ||
'</Shape>
</Transform>
</Scene>
</X3D>' As x3ddoc;


SELECT ST_AsX3D(
ST_Translate(
ST_Force_3d(
ST_Buffer(ST_Point(10,10),5, 'quad_segs=2')), 0,0,
3)
,6) As x3dfrag;

SELECT ST_AsX3D(ST_GeomFromEWKT('TIN (((
0 0 0,
0 0 1,
0 1 0,
0 0 0
)), ((
0 0 0,
0 1 0,
1 1 0,
0 0 0
))
)')) As x3dfrag;

SELECT ST_AsEWKT(ST_MemUnion(the_geom))
FROM
(SELECT ST_GeomFromEWKT('POLYGON((-7 4.2 2,-7.1 4.2 3,-7.1 4.3 2,
-7 4.2 2))') as the_geom
UNION ALL
SELECT ST_GeomFromEWKT('POINT(5 5 5)') as the_geom
UNION ALL
SELECT ST_GeomFromEWKT('POINT(-2 3 1)') as the_geom
UNION ALL
SELECT ST_GeomFromEWKT('LINESTRING(5 5 5, 10 10 10)') as the_geom ) as foo;


SELECT ST_AsEWKT(ST_MemUnion(the_geom))
FROM
(SELECT ST_GeomFromEWKT('POLYGON((-7 4.2,-7.1 4.2,-7.1 4.3,
-7 4.2))') as the_geom
UNION ALL
SELECT ST_GeomFromEWKT('POINT(5 5 5)') as the_geom
UNION ALL
SELECT ST_GeomFromEWKT('POINT(-2 3 1)') as the_geom
UNION ALL
SELECT ST_GeomFromEWKT('LINESTRING(5 5 5, 10 10 10)') as the_geom ) as foo;
SELECT ST_XMax(ST_GeomFromText('LINESTRING(1 3 4, 5 6 7)'));
SELECT ST_XMax(CAST('BOX(-3 2, 3 4)' As box2d));
SELECT ST_AsText(ST_Line_SubString(ST_GeomFromText('LINESTRING(25 50, 100 125, 150 190)'), 
0.333, 0.666));

SELECT ST_AsText(ST_Boundary(ST_GeomFromText('LINESTRING(1 1,0 0, -1 1)')));
SELECT ST_AsEWKT(ST_Boundary(ST_GeomFromEWKT('POLYGON((1 1 1,0 0 1, -1 1 1, 1 1 1))')));

SELECT ST_AsEWKT(ST_Boundary(ST_GeomFromEWKT('MULTILINESTRING((1 1 1,0 0 0.5, -1 1 1),(1 1 
0.5,0 0 0.5, -1 1 0.5, 1 1 0.5) )')));
SELECT 'polygonize_garray', ST_astext(ST_polygonize('{0102000000020000000000000000000000000000000000000000000000000024400000000000000000:0102000000020000000000000000002440000000000000000000000000000000000000000000000000:0102000000020000000000000000002440000000000000244000000000000000000000000000000000:0102000000020000000000000000002440000000000000244000000000000024400000000000000000:0102000000020000000000000000002440000000000000244000000000000000000000000000002440:0102000000020000000000000000000000000000000000244000000000000000000000000000002440:0102000000020000000000000000000000000000000000244000000000000024400000000000002440:0102000000020000000000000000000000000000000000244000000000000000000000000000000000:0102000000020000000000000000000000000000000000244000000000000024400000000000000000}'::geometry[]));

SELECT 'polygonize_garray', ST_astext(ST_geometryn(ST_polygonize('{LINESTRING(0 0, 10 0):LINESTRING(10 0, 10 10):LINESTRING(10 10, 0 10):LINESTRING(0 10, 0 0)}'::geometry[]), 1));

SELECT '#3470b', ST_Area(ST_Polygonize(ARRAY[NULL, 'LINESTRING (0 0, 10 0, 10 10)', NULL, 'LINESTRING (0 0, 10 10)', NULL]::geometry[]));
SELECT ST_AsText(ST_AddMeasure(
ST_GeomFromEWKT('LINESTRING(1 0, 2 0, 4 0)'),1,4)) As ewelev;
SELECT ST_AsText(ST_AddMeasure(
ST_GeomFromEWKT('LINESTRING(1 0 4, 2 0 4, 4 0 4)'),10,40)) As ewelev;
SELECT ST_AsText(ST_AddMeasure(
ST_GeomFromEWKT('MULTILINESTRINGM((1 0 4, 2 0 4, 4 0 4),(1 0 4, 2 0 4, 4 0 4))'),10,70)) As 
ewelev;
SELECT ST_YMin('BOX3D(1 2 3, 4 5 6)');
SELECT ST_YMin(ST_GeomFromText('LINESTRING(1 3 4, 5 6 7)'));
SELECT ST_AsText(ST_Project('POINT(0 0)'::geography, 100000, radians(45.0)));
SELECT ST_AsText(ST_Project('POINT(0 0)'::geography, 100000, pi()/4));
SELECT GeometryType(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 
29.07)'));
SELECT ST_AsEWKT(ST_Rotate('LINESTRING (50 160, 50 50, 100 50)', pi()));
SELECT ST_AsEWKT(ST_Rotate('LINESTRING (50 160, 50 50, 100 50)', pi()/6, 50, 160));
SELECT ST_AsEWKT(ST_Rotate(geom, -pi()/3, ST_Centroid(geom)))
FROM (SELECT 'LINESTRING (50 160, 50 50, 100 50)'::geometry AS geom) AS foo;
SELECT ST_AsEWKT(ST_MakePointM(-71.1043443253471, 42.3150676015829, 10));
SELECT ST_3DPerimeter(ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,743265 2967450,
743265.625 2967416,743238 2967416))', 2249));
SELECT tbl1.column1, tbl2.column1, tbl1.column2 &&& tbl2.column2 AS overlaps_3d,
tbl1.column2 && tbl2.column2 AS overlaps_2d
FROM ( VALUES
(1, 'LINESTRING Z(0 0 1, 3 3 2)'::geometry),
(2, 'LINESTRING Z(1 2 0, 0 5 -1)'::geometry)) AS tbl1,
( VALUES
(3, 'LINESTRING Z(1 2 1, 4 6 1)'::geometry)) AS tbl2;

SELECT tbl1.column1, tbl2.column1, tbl1.column2 &&& tbl2.column2 AS overlaps_3zm,
tbl1.column2 && tbl2.column2 AS overlaps_2d
FROM ( VALUES
(1, 'LINESTRING M(0 0 1, 3 3 2)'::geometry),
(2, 'LINESTRING M(1 2 0, 0 5 -1)'::geometry)) AS tbl1,
( VALUES
(3, 'LINESTRING M(1 2 1, 4 6 1)'::geometry)) AS tbl2;
SELECT ST_AsText(the_geom)
FROM
(SELECT ST_LocateAlong(
ST_GeomFromText('MULTILINESTRINGM((1 2 3, 3 4 2, 9 4 3),
(1 2 3, 5 4 5))'),3) As the_geom) As foo;
SELECT ST_AsText((ST_Dump(the_geom)).geom)
FROM
(SELECT ST_LocateAlong(
ST_GeomFromText('MULTILINESTRINGM((1 2 3, 3 4 2, 9 4 3),
(1 2 3, 5 4 5))'),3) As the_geom) As foo;
SELECT ST_AsEWKT(
ST_Node('LINESTRINGZ(0 0 0, 10 10 10, 0 10 5, 10 0 3)'::geometry)
) As output;
SELECT ST_Y(ST_GeomFromEWKT('POINT(1 2 3 4)'));
SELECT ST_Y(ST_Centroid(ST_GeomFromEWKT('LINESTRING(1 2 3 4, 1 1 1 1)')));
SELECT ST_IsRing(the_geom), ST_IsClosed(the_geom), ST_IsSimple(the_geom)
FROM (SELECT 'LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)'::geometry AS the_geom) AS foo;
SELECT ST_IsRing(the_geom), ST_IsClosed(the_geom), ST_IsSimple(the_geom)
FROM (SELECT 'LINESTRING(0 0, 0 1, 1 0, 1 1, 0 0)'::geometry AS the_geom) AS foo;

SELECT ST_AsBinary(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))',4326));
SELECT ST_AsBinary(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))',4326), 'XDR');
SELECT ST_AsText(ST_MakeEnvelope(10, 10, 11, 11, 4326));
SELECT ST_AsText(ST_Snap(poly,line,  
ST_Distance(poly,line)*1.01)) AS polysnapped
FROM (SELECT
ST_GeomFromText('MULTIPOLYGON(
((26 125, 26 200, 126 200, 126 125,  
26 125 ),
( 51 150, 101 150, 76 175, 51 150 )  
),
(( 151 100, 151 200, 176 175, 151  
100 )))') As poly,
ST_GeomFromText('LINESTRING (5  
107, 54 84, 101 100)') As line
) As foo;

SELECT ST_AsText(
ST_Snap(poly,line, ST_Distance(poly,  
line)*1.25)
) AS polysnapped
FROM (SELECT
ST_GeomFromText('MULTIPOLYGON(
(( 26 125, 26 200, 126 200, 126 125,  
26 125 ),
( 51 150, 101 150, 76 175, 51 150 )  
),
(( 151 100, 151 200, 176 175, 151  
100 )))') As poly,
ST_GeomFromText('LINESTRING (5  
107, 54 84, 101 100)') As line
) As foo;

SELECT ST_AsText(
ST_Snap(line, poly, ST_Distance(poly,  
line)*1.01)
) AS linesnapped
FROM (SELECT
ST_GeomFromText('MULTIPOLYGON(
((26 125, 26 200, 126 200, 126 125,  
26 125),
(51 150, 101 150, 76 175, 51 150 ))  
,
((151 100, 151 200, 176 175, 151  
100)))') As poly,
ST_GeomFromText('LINESTRING (5  
107, 54 84, 101 100)') As line
) As foo;

SELECT ST_AsText(
ST_Snap(line, poly, ST_Distance(poly,  
line)*1.25)
) AS linesnapped
FROM (SELECT
ST_GeomFromText('MULTIPOLYGON(
(( 26 125, 26 200, 126 200, 126 125,  
26 125 ),
(51 150, 101 150, 76 175, 51 150 ))  
,
((151 100, 151 200, 176 175, 151  
100 )))') As poly,
ST_GeomFromText('LINESTRING (5  
107, 54 84, 101 100)') As line
) As foo;

SELECT 'LINESTRING(0 0, 0 1, 1 0)'::geometry = 'LINESTRING(1 1, 0 0)'::geometry;
SELECT ST_AsText(column1)
FROM ( VALUES
('LINESTRING(0 0, 1 1)'::geometry),
('LINESTRING(1 1, 0 0)'::geometry)) AS foo;

SELECT ST_AsText(column1)
FROM ( VALUES
('LINESTRING(0 0, 1 1)'::geometry),
('LINESTRING(1 1, 0 0)'::geometry)) AS foo
GROUP BY column1;

SELECT ST_NumGeometries(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 
29.31,77.29 29.07)'));
SELECT ST_NumGeometries(ST_GeomFromEWKT('GEOMETRYCOLLECTION(MULTIPOINT(-2 3 , -2 2),
LINESTRING(5 5 ,10 10),
POLYGON((-7 4.2,-7.1 5,-7.1 4.3,-7 4.2)))'));
SELECT ST_IsValid(ST_GeomFromText('LINESTRING(0 0, 1 1)')) As good_line,
ST_IsValid(ST_GeomFromText('POLYGON((0 0, 1 1, 1 2, 1 1, 0 0))')) As bad_poly;
SELECT ST_GeomFromGML('<gml:LineString srsName="EPSG:4269"><gml:coordinates>-71.16028,42.258729 -71.160837,42.259112 -71.161143,42.25932</gml:coordinates></gml:LineString>');

SELECT ST_GeomFromGML('
<gml:LineString xmlns:gml="http://www.opengis.net/gml"
xmlns:xlink="http://www.w3.org/1999/xlink"
srsName="urn:ogc:def:crs:EPSG::4269">
<gml:pointProperty>
<gml:Point gml:id="p1"><gml:pos>42.258729 -71.16028</gml:pos></gml:Point>
</gml:pointProperty>
<gml:pos>42.259112 -71.160837</gml:pos>
<gml:pointProperty>
<gml:Point xlink:type="simple" xlink:href="#p1"/>
</gml:pointProperty>
</gml:LineString>');
SELECT ST_AsEWKT(ST_Force4D(ST_GeomFromEWKT('CIRCULARSTRING(1 1 2, 2 3 2, 4 5 2, 6 7 2, 5 6 
2)')));

SELECT ST_AsEWKT(ST_Force4D('MULTILINESTRINGM((0 0 1,0 5 2,5 0 3,0 0 4),(1 1 1,3 1 1,1 3 
1,1 1 1))'));
SELECT ST_AsText(ST_SetPoint('LINESTRING(-1 2,-1 3)', 0, 'POINT(-1 1)'));
SELECT ST_AsEWKT(ST_SetPoint(foo.the_geom, ST_NumPoints(foo.the_geom)- 1, ST_GeomFromEWKT 
('POINT(-1 1 3)')))
FROM (SELECT ST_GeomFromEWKT('LINESTRING(-1 2 3,-1 3 4, 5 6 7)') As the_geom) As foo;
SELECT ST_Length2D_Spheroid( the_geom, sph_m ) As tot_len,
ST_Length2D_Spheroid(ST_GeometryN(the_geom,1), sph_m) As len_line1,
ST_Length2D_Spheroid(ST_GeometryN(the_geom,2), sph_m) As len_line2
FROM (SELECT ST_GeomFromText('MULTILINESTRING((-118.584 38.374,-118.583 38.5),
(-71.05957 42.3589 , -71.061 43))') As the_geom,
CAST('SPHEROID["GRS_1980",6378137,298.257222101]' As spheroid) As sph_m) as foo;

SELECT ST_Length2D_Spheroid( the_geom, sph_m ) As tot_len,
ST_Length2D_Spheroid(ST_GeometryN(the_geom,1), sph_m) As len_line1,
ST_Length2D_Spheroid(ST_GeometryN(the_geom,2), sph_m) As len_line2
FROM (SELECT ST_GeomFromEWKT('MULTILINESTRING((-118.584 38.374 20,-118.583 38.5 30) 
,
(-71.05957 42.3589 75, -71.061 43 90))') As the_geom,
CAST('SPHEROID["GRS_1980",6378137,298.257222101]' As spheroid) As sph_m) as foo;
SELECT ST_PointFromText('POINT(-71.064544 42.28787)');
SELECT ST_PointFromText('POINT(-71.064544 42.28787)', 4326);

SELECT ST_CoveredBy(smallc,smallc) As smallinsmall,
ST_CoveredBy(smallc, bigc) As smallcoveredbybig,
ST_CoveredBy(ST_ExteriorRing(bigc), bigc) As exteriorcoveredbybig,
ST_Within(ST_ExteriorRing(bigc),bigc) As exeriorwithinbig
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc,
ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;

SELECT ST_AsText(the_geom)
FROM
(SELECT ST_LocateAlong(
ST_GeomFromText('MULTILINESTRINGM((1 2 3, 3 4 2, 9 4 3),(1 2 3, 5 4 5))'),3) As the_geom) As foo;

SELECT ST_AsText((ST_Dump(the_geom)).geom)
FROM
(SELECT ST_LocateAlong(
ST_GeomFromText('MULTILINESTRINGM((1 2 3, 3 4 2, 9 4 3),
(1 2 3, 5 4 5))'),3) As the_geom) As foo;

SELECT (ST_DumpPoints(ST_GeomFromText(
'MULTIPOINT(14 14,34 14,54 14,74 14,94 
14,114 14,134 14,
150 14,154 14,154 6,134 6,114 6,94 6,74 
6,54 6,34 6,
14 6,10 6,8 6,7 7,6 8,6 10,6 30,6 50,6 
70,6 90,6 110,6 130,
6 150,6 170,6 190,6 194,14 194,14 174,14 
154,14 134,14 114,
14 94,14 74,14 54,14 34,14 14)'))).geom, 1 id
INTO TABLE l_shape;
SELECT ST_ConvexHull(ST_Collect(geom))
FROM l_shape;

SELECT ST_ConcaveHull(ST_Collect(geom), 
0.99)
FROM l_shape;

SELECT ST_AsText(ST_ConvexHull(
ST_Collect(
ST_GeomFromText('MULTILINESTRING((100 190,10 8),(150 10, 20 30))'),
ST_GeomFromText('MULTIPOINT(50 5, 150 30, 50 10, 10 10)')
)) );

SELECT ST_Buffer(
ST_GeomFromText('POINT(100 90)'),
50, 'quad_segs=8');

SELECT ST_Buffer(
ST_GeomFromText('POINT(100 90)'),
50, 'quad_segs=2');

SELECT ST_Buffer(
ST_GeomFromText(
'LINESTRING(50 50,150 150,150 50)'
), 10, 'endcap=round join=round');


SELECT ST_NPoints(ST_Buffer(ST_GeomFromText('POINT(100 90)'), 50)) As 
promisingcircle_pcount,
ST_NPoints(ST_Buffer(ST_GeomFromText('POINT(100 90)'), 50, 2)) As lamecircle_pcount;

SELECT ST_AsText(ST_Buffer(
ST_Transform(
ST_SetSRID(ST_MakePoint(-71.063526, 42.35785),4269), 26986)
,100,2)) As octagon;

SELECT ST_LineCrossingDirection(foo.line1 
, foo.line2) As l1_cross_l2 ,
ST_LineCrossingDirection(foo. 
line2, foo.line1) As l2_cross_l1
FROM (
SELECT
ST_GeomFromText('LINESTRING(25 169,89 
114,40 70,86 43)') As line1,
ST_GeomFromText('LINESTRING(171 154,20 
140,71 74,161 53)') As line2
) As foo;

SELECT ST_LineCrossingDirection(foo.line1 
, foo.line2) As l1_cross_l2 ,
ST_LineCrossingDirection(foo. 
line2, foo.line1) As l2_cross_l1
FROM (
SELECT
ST_GeomFromText('LINESTRING(25 169,89 
114,40 70,86 43)') As line1,
ST_GeomFromText('LINESTRING (171 154, 
20 140, 71 74, 2.99 90.16)') As line2
) As foo;

SELECT
ST_LineCrossingDirection(foo. 
line1, foo.line2) As l1_cross_l2 ,
ST_LineCrossingDirection(foo. 
line2, foo.line1) As l2_cross_l1
FROM (
SELECT
ST_GeomFromText('LINESTRING(25 169,89 
114,40 70,86 43)') As line1,
ST_GeomFromText('LINESTRING (20 140, 71 
74, 161 53)') As line2
) As foo;

SELECT ST_LineCrossingDirection(foo.line1 
, foo.line2) As l1_cross_l2 ,
ST_LineCrossingDirection(foo. 
line2, foo.line1) As l2_cross_l1
FROM (SELECT
ST_GeomFromText('LINESTRING(25 
169,89 114,40 70,86 43)') As line1,
ST_GeomFromText('LINESTRING(2.99 
90.16,71 74,20 140,171 154)') As line2
) As foo;


SELECT ST_IsValidReason('LINESTRING(220227 150406,2220227 150407,222020 150410)');

SELECT ST_BdPolyFromText('MULTILINESTRING((0 0, 10 0, 10 10, 0 10, 0 0),(2 2, 2 4, 4 4, 4 2, 2 2),(5 5, 5 7, 7 7, 7 5, 5 5))', 3);

SELECT ST_BdMPolyFromText('MULTILINESTRING( (0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 4, 4 4, 4 2, 2 2), (5 5, 5 7, 7 7, 7 5, 5 5), (20 0,30 0,30 10,20 10,20 0), (22 2,22 4,24 4,24 2,22 2), (25 5,25 7,27 7,27 5,25 5))', 3);

select ST_BdPolyFromText('POINT(0 0)', 3);
select ST_BdMPolyFromText('POINT(0 0)', 3);

-- MultiPolygon forming input to BdPolyFromText
select ST_BdPolyFromText('MULTILINESTRING( (0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 4, 4 4, 4 2, 2 2), (5 5, 5 7, 7 7, 7 5, 5 5), (20 0,30 0,30 10,20 10,20 0), (22 2,22 4,24 4,24 2,22 2), (25 5,25 7,27 7,27 5,25 5))', 3);

-- SinglePolygon forming input to BdMPolyFromText
select 'BdMPolyFromText', ST_asewkt(ST_BdMPolyFromText('MULTILINESTRING((0 0, 10 0, 10 10, 0 10, 0 0),(2 2, 2 4, 4 4, 4 2, 2 2),(5 5, 5 7, 7 7, 7 5, 5 5))', 3));

SELECT ST_OrderingEquals(ST_GeomFromText('LINESTRING(0 0, 10 10)'),
ST_GeomFromText('LINESTRING(0 0, 5 5, 10 10)'));

SELECT ST_OrderingEquals(ST_GeomFromText('LINESTRING(0 0, 10 10)'),
ST_GeomFromText('LINESTRING(0 0, 0 0, 10 10)'));

SELECT ST_OrderingEquals(ST_Reverse(ST_GeomFromText('LINESTRING(0 0, 10 10)')),
ST_GeomFromText('LINESTRING(0 0, 0 0, 10 10)'));

SELECT ST_OrderingEquals(ST_GeomFromText('LINESTRING(5 5, 10 10, 0 0, 5 5)'),
ST_GeomFromText('LINESTRING(0 0, 5 5, 10 10, 0 0)'));

SELECT ST_OrderingEquals(ST_GeomFromText('LINESTRING(0 0, 10 10)'),ST_GeomFromText('LINESTRING(0 0, 10 10)'));
