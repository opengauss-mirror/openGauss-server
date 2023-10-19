set current_schema=postgis;
SELECT ST_Dump(the_geom) AS the_geom FROM geom_point order by id limit 10;
--Break a compound curve into its constituent linestrings and circularstrings
SELECT ST_AsEWKT(a.geom), ST_HasArc(a.geom)FROM ( SELECT (ST_Dump(p_geom)).geom AS geom FROM (SELECT ST_GeomFromEWKT('COMPOUNDCURVE(CIRCULARSTRING(0 0, 1 1, 1 0),(1 0, 0 1))') AS p_geom) AS b) AS a;
--S10.验证函数ST_DumpPoints返回组成几何图形的所有点的一组Geometry_dump值
SELECT edge_id, (dp).path[1] As index, ST_AsText((dp).geom) As wktnode FROM (SELECT 1 As edge_id, ST_DumpPoints(ST_GeomFromText('LINESTRING(1 2, 3 4, 10 10)')) AS dp UNION ALL SELECT 2 As edge_id, ST_DumpPoints(ST_GeomFromText('LINESTRING(3 5, 5 6, 9 10)')) AS dp) As foo;

--S12.验证函数ST_FlipCoordinates返回翻转X轴或Y轴的几何体
SELECT ST_AsEWKT(ST_FlipCoordinates(GeomFromEWKT('POINT(1 2)')));
--S13.验证函数ST_Intersection返回两个几何的共享部分
SELECT ST_AsText(ST_Intersection('POINT(0 0)'::geometry, 'LINESTRING ( 2 0, 0 2 )':: geometry));
--S14.验证函数ST_LineToCurve将线或多边形转换成循环或曲线
SELECT ST_AsText(ST_LineToCurve(foo.the_geom)) As curvedastext,ST_AsText(foo.the_geom) As non_curvedastext
FROM (SELECT ST_Buffer('POINT(1 3)'::geometry, 3) As the_geom) As foo;
SELECT ST_AsText(ST_LineToCurve(geom)) As curved, ST_AsText(geom) AS not_curved
FROM (SELECT ST_Translate(ST_Force3D(ST_Boundary(ST_Buffer(ST_Point(1,3), 2,2))),0,0,3) AS geom) AS foo;
--S17.验证函数ST_MinimumBoundingCircle返回可以完全包含几何的最小圆
SELECT ST_AsText(ST_MinimumBoundingCircle(
ST_Collect(
ST_GeomFromEWKT('LINESTRING(55 75,125 150)'),
ST_Point(20, 80)), 8
)) As wktmbc;
--S18.验证函数ST_Polygonize返回一个几何集合
SELECT '#3470b', ST_Area(ST_Polygonize(ARRAY[NULL, 'LINESTRING (0 0, 10 0, 10 10)', NULL, 'LINESTRING (0 0, 10 10)', NULL]::geometry[]));
--S19.验证函数ST_Node返回一组线串的节点
SELECT ST_AsText(
ST_Node('LINESTRINGZ(0 0 0, 10 10 10, 0 10 5, 10 0 3)'::geometry)
) As output;
--S20.验证函数ST_OffsetCurve给定距离和输入线一侧的偏移线
SELECT ST_AsText(ST_OffsetCurve( 
ST_GeomFromText(
'LINESTRING(164 16,144 16,124 16,104 
16,84 16,64 16,44 16,24 16,20 16,18 16,17 17,
16 18,16 20,16 40,16 60,16 80,16 
100,16 120,16 140,16 160,16 180,16 195)'),
15, 'quad_segs=4 join=round'));
--S21.验证函数ST_RemoveRepeatedPoints返回删除重复点的几何
--S22.验证函数ST_SharedPaths返回包含由两个输入线或多边形的路径的集合
SELECT ST_AsText(ST_SharedPaths(ST_GeomFromText('MULTILINESTRING((26 125,26 200,126 200,126 125,26 125),(51 150,101 150,76 175,51 150))'),ST_GeomFromText('LINESTRING(151 100,126 156.25,126 125,90 161, 76 175)'))) As wkt;
--S23.验证函数ST_ShiftLongitude在-180到180和0到360范围之间切换几何坐标
SELECT ST_AsEWKT(ST_ShiftLongitude(ST_GeomFromEWKT('SRID=4326;POINT(-118.58 38.38 10)'))) As geomA,
ST_AsEWKT(ST_ShiftLongitude(ST_GeomFromEWKT('SRID=4326;POINT(241.42 38.38 10)'))) As geomb;
--S24.验证函数ST_Simplify返回给定几何的简化几何
SELECT ST_Npoints(the_geom) As np_before, ST_NPoints(ST_Simplify(the_geom,0.1)) As np01_notbadcircle, ST_NPoints(ST_Simplify(the_geom,0.5)) As np05_notquitecircle,
ST_NPoints(ST_Simplify(the_geom,1)) As np1_octagon, ST_NPoints(ST_Simplify(the_geom,10)) As np10_triangle,
(ST_Simplify(the_geom,100) is null) As np100_geometrygoesaway
FROM (SELECT ST_Buffer('POINT(1 3)', 10,12) As the_geom) As foo;
--S25.验证函数ST_SimplifyPreserveTopology返回给定几何的简化几何
SELECT ST_Npoints(the_geom) As np_before, ST_NPoints(ST_SimplifyPreserveTopology(the_geom ,0.1)) As np01_notbadcircle, ST_NPoints(ST_SimplifyPreserveTopology(the_geom,0.5)) As np05_notquitecircle,
ST_NPoints(ST_SimplifyPreserveTopology(the_geom,1)) As np1_octagon, ST_NPoints( ST_SimplifyPreserveTopology(the_geom,10)) As np10_square,
ST_NPoints(ST_SimplifyPreserveTopology(the_geom,100)) As np100_stillsquare
FROM (SELECT ST_Buffer('POINT(1 3)', 10,12) As the_geom) As foo;
--S26.验证函数ST_Split拆分几何
SELECT ST_Split(circle, line)
FROM (SELECT
ST_MakeLine(ST_MakePoint(10, 10),ST_MakePoint(190, 190)) As line,
ST_Buffer(ST_GeomFromText('POINT(100 90)'), 50) As circle) As foo;
--S27.验证函数ST_SymDifference返回表示A和B不相交的部分几何图形
SELECT ST_AsText(
ST_SymDifference(
ST_GeomFromText('LINESTRING(50 100, 50 200)'),
ST_GeomFromText('LINESTRING(50 50, 50 150)')
)
);
--S28.验证函数ST_Union返回表示几何体的点集合并的几何
SELECT geom_point.id,ST_AsText(ST_Union(st_setsrid(geom_point.the_geom,4326),st_setsrid(geom_line.the_geom,4326))) from geom_point inner join geom_line on geom_point.id=geom_line.id order by id limit 10;
--S29.验证函数ST_UnaryUnion返回表示几何体的点集合并的几何
select ST_AsText(ST_UnaryUnion(the_geom)) from geom_point where id=1;




