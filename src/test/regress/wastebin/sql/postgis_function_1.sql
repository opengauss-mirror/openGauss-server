/*
###############################################################################
# TESTCASE NAME : postgis_operators.sql
# COMPONENT(S)  : test function operators
# PREREQUISITE  : 
# PLATFORM      : SUSE11.3
# DESCRIPTION   : 验证postgis的操作符
# TAG           : &&,&&&,&<,&<|,&>,<<,<<|,=,>>,|>>,@,~,~=,|&>,<->,<#>
# TC LEVEL      : Level 1
################################################################################
*/
set current_schema=postgis;
--S1.验证操作符&&
SELECT geom_point.the_geom && geom_line.the_geom AS result FROM geom_point, geom_line order by geom_point.name,geom_line.name limit 10;
SELECT geom_multipoint.the_geom && geom_multilinestring.the_geom AS result FROM geom_multipoint, geom_multilinestring order by geom_multipoint.name,geom_multilinestring.name limit 10;
--S2.验证操作符&&&
SELECT geom_multipoint.the_geom &&& geom_multipolygon.the_geom AS overlaps_3d from geom_multipoint, geom_multipolygon order by geom_multipoint.name,geom_multipolygon.id limit 10; 
SELECT geom_multipoint.the_geom &&& geom_point.the_geom AS overlaps_3d from geom_multipoint, geom_point order by geom_point.name,geom_multipoint.name limit 10; 
--S3.验证操作符&<
SELECT geom_polygon.the_geom &< geom_multilinestring.the_geom AS result FROM geom_polygon, geom_multilinestring order by geom_polygon.name, geom_multilinestring.id limit 10;
--S4.验证操作符&<|
SELECT geom_multipolygon.the_geom &< geom_multilinestring.the_geom AS result FROM geom_multipolygon, geom_multilinestring order by geom_multipolygon.id,geom_multilinestring.name limit 10;
--S5.验证操作符&>
SELECT geom_polygon.the_geom &> geom_multilinestring.the_geom AS result FROM geom_polygon, geom_multilinestring order by geom_polygon.name, geom_multilinestring.id limit 10;
--S6.验证操作符<<
SELECT geom_line.the_geom << geom_multilinestring.the_geom AS result from geom_line, geom_multilinestring order by geom_line.id, geom_multilinestring.id limit 10; 

--S8.验证操作符=
SELECT 'LINESTRING(0 0, 0 1, 1 0)'::geometry = 'LINESTRING(1 1, 0 0)'::geometry;
--S9.验证操作符>>
SELECT geom_line.the_geom >> geom_multilinestring.the_geom AS result from geom_line, geom_multilinestring order by geom_line.id,geom_multipolygon.id  limit 10; 
--S10.验证操作iii符@
SELECT geom_point.the_geom @ geom_multipolygon.the_geom AS result from geom_point,  geom_multipolygon order by geom_point.id,geom_multipolygon.id limit 10; 
--S11.验证操作符|&>
SELECT geom_point.the_geom |&> geom_multipolygon.the_geom AS result from geom_point,  geom_multipolygon order by geom_point.id,geom_multipolygon.id limit 10; 
--S12.验证操作符|>>
SELECT geom_point.the_geom |>> geom_multipolygon.the_geom AS result from geom_point, geom_multipolygon order by geom_point.id,geom_multipolygon.id limit 10; 
--S13.验证操作符~
SELECT geom_point.the_geom ~ geom_multipolygon.the_geom AS result from geom_point, geom_multipolygon order by geom_point.id,geom_multipolygon.id limit 10; 
--S14.验证操作符~=
SELECT geom_point.the_geom ~= geom_multipolygon.the_geom AS result from geom_point, geom_multipolygon order by geom_point.id,geom_multipolygon.id limit 10; 
--S15.验证操作符<->
SELECT geom_point.the_geom <-> geom_multipolygon.the_geom AS result from geom_point, geom_multipolygon order by geom_point.id,geom_multipolygon.id limit 10; 
--S16.验证操作符<#>
SELECT '<#>',geom_point.id, geom_point.the_geom <#> geom_multipolygon.the_geom AS result from geom_point, geom_multipolygon order by geom_point.id,geom_multipolygon.id limit 10; 


SELECT ST_AsText(ST_Union(ST_GeomFromText('POINT(1 2)'),
ST_GeomFromText('POINT(-2 3)') ) );
SELECT ST_AsText(ST_Union(ST_GeomFromText('POINT(1 2)'),
ST_GeomFromText('POINT(1 2)') ) );

SELECT ST_AsEWKT(st_union(the_geom))
FROM
(SELECT ST_GeomFromEWKT('POLYGON((-7 4.2,-7.1 4.2,-7.1 4.3,
-7 4.2))') as the_geom
UNION ALL
SELECT ST_GeomFromEWKT('POINT(5 5 5)') as the_geom
UNION ALL
SELECT ST_GeomFromEWKT('POINT(-2 3 1)') as the_geom
UNION ALL
SELECT ST_GeomFromEWKT('LINESTRING(5 5 5, 10 10 10)') as the_geom ) as foo;

SELECT ST_AsText(ST_Union(ARRAY[ST_GeomFromText('LINESTRING(1 2, 3 4)'),
ST_GeomFromText('LINESTRING(3 4, 4 5)')])) As wktunion;
SELECT ST_AsText(ST_CollectionExtract(ST_GeomFromText('GEOMETRYCOLLECTION( 
GEOMETRYCOLLECTION(POINT(0 0)))'),1));
SELECT ST_AsText(ST_CollectionExtract(ST_GeomFromText('GEOMETRYCOLLECTION( 
GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1)),LINESTRING(2 2, 3 3))'),2));
SELECT ST_AsEWKT(ST_PatchN(geom, 2)) As geomewkt
FROM (
VALUES (ST_GeomFromEWKT('POLYHEDRALSURFACE( ((0 0 0, 0 0 1, 0 1 1, 0 1 0, 0 0 0)),
((0 0 0, 0 1 0, 1 1 0, 1 0 0, 0 0 0)), ((0 0 0, 1 0 0, 1 0 1, 0 0 1, 0 0 0)),
((1 1 0, 1 1 1, 1 0 1, 1 0 0, 1 1 0)),
((0 1 0, 0 1 1, 1 1 1, 1 1 0, 0 1 0)), ((0 0 1, 1 0 1, 1 1 1, 0 1 1, 0 0 1)) )')) ) As 
foo(geom);
SELECT
ST_AsText(
ST_PointFromWKB(
ST_AsEWKB('POINT(2 5)'::geometry)
)
);
SELECT ST_3DMakeBox(ST_MakePoint(-989502.1875, 528439.5625, 10),
ST_MakePoint(-987121.375 ,529933.1875, 10)) As abb3d;
SELECT ST_AsEWKT(ST_Force2D(ST_GeomFromEWKT('CIRCULARSTRING(1 1 2, 2 3 2, 4 5 2, 6 7 2, 5 6 
2)')));
SELECT ST_AsEWKT(ST_Force2D('POLYGON((0 0 2,0 5 2,5 0 2,0 0 2),(1 1 2,3 1 2,1 3 2,1 1 2)) 
'));
SELECT tbl1.column1, tbl2.column1, tbl1.column2 ~ tbl2.column2 AS contains
FROM
( VALUES
(1, 'LINESTRING (0 0, 3 3)'::geometry)) AS tbl1,
( VALUES
(2, 'LINESTRING (0 0, 4 4)'::geometry),
(3, 'LINESTRING (1 1, 2 2)'::geometry),
(4, 'LINESTRING (0 0, 3 3)'::geometry)) AS tbl2;
SELECT ST_AsGML(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))',4326));
SELECT ST_AsGML(3, ST_GeomFromText('POINT(5.234234233242 6.34534534534)',4326), 5, 17);
SELECT ST_AsGML(3, ST_GeomFromText('LINESTRING(1 2, 3 4, 10 20)',4326), 5, 32);
SELECT ST_AsGML(3, ST_GeomFromText('LINESTRING(1 2, 3 4, 10 20)',4326), 5, 49);
SELECT ST_AsText(
ST_LongestLine('POINT(100 100)':: 
geometry,
'LINESTRING (20 80, 98 
190, 110 180, 50 75 )'::geometry)
) As lline;

SELECT ST_AsText(
ST_LongestLine(
ST_GeomFromText('POLYGON 
((175 150, 20 40,
50 60, 125 100, 
175 150))'),
ST_Buffer(ST_GeomFromText 
('POINT(110 170)'), 20)
)
) As llinewkt;

SELECT ST_AsText(ST_LongestLine(c.the_geom, c.the_geom)) As llinewkt,
ST_MaxDistance(c.the_geom,c.the_geom) As max_dist,
ST_Length(ST_LongestLine(c.the_geom, c.the_geom)) As lenll
FROM (SELECT ST_BuildArea(ST_Collect(the_geom)) As the_geom
FROM (SELECT ST_Translate(ST_SnapToGrid(ST_Buffer(ST_Point(50 ,generate_series 
(50,190, 50)
),40, 'quad_segs=2'),1), x, 0) As the_geom
FROM generate_series(1,100,50) As x) AS foo
) As c;
SELECT ST_AsEWKT(ST_LocateBetweenElevations(
ST_GeomFromEWKT('LINESTRING(1 2 3, 4 5 6)'),2,4)) As ewelev;
SELECT ST_AsEWKT(ST_LocateBetweenElevations(
ST_GeomFromEWKT('LINESTRING(1 2 6, 4 5 -1, 7 8 9)'),6,9)) As ewelev;
SELECT ST_AsEWKT((ST_Dump(the_geom)).geom)
FROM
(SELECT ST_LocateBetweenElevations(
ST_GeomFromEWKT('LINESTRING(1 2 6, 4 5 -1, 7 8 9)'),6,9) As the_geom) As foo;
SELECT ST_LineFromText('LINESTRING(1 2, 3 4)') AS aline, ST_LineFromText('POINT(1 2)') AS 
null_return;
SELECT ST_astext(ST_GeomFromEWKB(E'\\001\\002\\000\\000 \\255\\020\\000\\000\\003\\000\\000\\000\\344J=\\013B\\312Q\\300n\\303(\\010\\036!E@''\\277E''K\\312Q\\300\\366{b\\235*!E@\\225|\\354.P\\312Q\\300p\\231\\323e1!E@'));
SELECT Box3D(ST_GeomFromEWKT('LINESTRING(1 2 3, 3 4 5, 5 6 5)'));
SELECT Box3D(ST_GeomFromEWKT('CIRCULARSTRING(220268 150415 1,220227 150505 1,220227 
150406 1)'));
SELECT tbl1.column1, tbl2.column1, tbl1.column2 &> tbl2.column2 AS overright
FROM
( VALUES
(1, 'LINESTRING(1 2, 4 6)'::geometry)) AS tbl1,
( VALUES
(2, 'LINESTRING(0 0, 3 3)'::geometry),
(3, 'LINESTRING(0 1, 0 5)'::geometry),
(4, 'LINESTRING(6 0, 6 1)'::geometry)) AS tbl2;
SELECT ST_AsEWKT(ST_LineFromMultiPoint(ST_GeomFromEWKT('MULTIPOINT(1 2 3, 4 5 6, 7 8 9)')));
SELECT ST_AsText(ST_GeomFromGeoHash('9qqj7nmxncgyy4d0dbxqz0'));
SELECT ST_AsText(ST_GeomFromGeoHash('9qqj7nmxncgyy4d0dbxqz0', 4));
SELECT ST_AsText(ST_GeomFromGeoHash('9qqj7nmxncgyy4d0dbxqz0', 10));
SELECT ST_AsEWKT(ST_RotateX(ST_GeomFromEWKT('LINESTRING(1 2 3, 1 1 1)'), pi()/2));
SELECT ST_AsEWKT(ST_Shift_Longitude(ST_GeomFromEWKT('SRID=4326;POINT(-118.58 38.38 10)'))) 
As geomA,
ST_AsEWKT(ST_Shift_Longitude(ST_GeomFromEWKT('SRID=4326;POINT(241.42 38.38 10)'))) As 
geomb;

SELECT ST_AsText(ST_Shift_Longitude(ST_GeomFromText('LINESTRING(-118.58 38.38, -118.20 
38.45)')));

SELECT ST_Within(smallc,smallc) As smallinsmall,
ST_Within(smallc, bigc) As smallinbig,
ST_Within(bigc,smallc) As biginsmall,
ST_Within(ST_Union(smallc, bigc), bigc) as unioninbig,
ST_Within(bigc, ST_Union(smallc, bigc)) as biginunion,
ST_Equals(bigc, ST_Union(smallc, bigc)) as bigisunion
FROM
(
SELECT ST_Buffer(ST_GeomFromText('POINT(50 50)'), 20) As smallc,
ST_Buffer(ST_GeomFromText('POINT(50 50)'), 40) As bigc) As foo;

SELECT ST_MPolyFromText('MULTIPOLYGON(((0 0 1,20 0 1,20 20 1,0 20 1,0 0 1),(5 5 3,5 7 3,7 7 
3,7 5 3,5 5 3)))');
--I2.processing join测试
--S1.验证函数ST_BuildArea join功能
SELECT geom_line.id,ST_AsText(ST_BuildArea(ST_Collect(geom_line.the_geom,geom_polygon.the_geom))) from geom_line inner join geom_polygon on geom_line.id=geom_polygon.id order by id limit 20;
--S2.验证函数ST_collect join功能
SELECT geom_point.id, ST_AsText(geom_point.the_geom), ST_AsText(geom_line.the_geom),ST_Astext(ST_Collect(ST_setsrid(geom_point.the_geom,4326), st_setsrid(geom_line.the_geom,4326))) from geom_point inner join geom_line on geom_point.id=geom_line.id order by id limit 10;
--S5.验证函数ST_Intersection返回两个几何的共享部分
SELECT geom_point.id, ST_AsText(ST_Intersection(ST_setsrid(geom_point.the_geom,4326), st_setsrid(geom_line.the_geom,4326))) from geom_point join geom_line on geom_point.id=geom_line.id order by id limit 10;
--S6.验证函数ST_SharedPaths返回包含由两个输入线或多边形的路径的集合
SELECT ST_AsText(
ST_SharedPaths(
ST_GeomFromText('LINESTRING(76 175,90 161,126 125,126 156.25,151 100)'),
ST_GeomFromText('MULTILINESTRING((26 125,26 200,126 200,126 125,26 125),
(51 150,101 150,76 175,51 150))')
)) As wkt;


SELECT ST_AsEWKT(ST_GeometryN(p_geom,3)) As geom_ewkt
FROM (SELECT ST_GeomFromEWKT('POLYHEDRALSURFACE(
((0 0 0, 0 0 1, 0 1 1, 0 1 0, 0 0 0)),
((0 0 0, 0 1 0, 1 1 0, 1 0 0, 0 0 0)),
((0 0 0, 1 0 0, 1 0 1, 0 0 1, 0 0 0)),
((1 1 0, 1 1 1, 1 0 1, 1 0 0, 1 1 0)),
((0 1 0, 0 1 1, 1 1 1, 1 1 0, 0 1 0)),
((0 0 1, 1 0 1, 1 1 1, 0 1 1, 0 0 1))
)') AS p_geom ) AS a;
SELECT ST_GeoHash(ST_SetSRID(ST_MakePoint(-126,48),4326));
SELECT ST_GeoHash(ST_SetSRID(ST_MakePoint(-126,48),4326),5);
SELECT ST_Relate(ST_GeometryFromText('POINT(1 2)'), ST_Buffer(ST_GeometryFromText('POINT(1 
2)'),2));
SELECT ST_Relate(ST_GeometryFromText('LINESTRING(1 2, 3 4)'), ST_GeometryFromText(' 
LINESTRING(5 6, 7 8)'));

SELECT ST_Relate(ST_GeometryFromText('POINT(1 2)'), ST_Buffer(ST_GeometryFromText('POINT(1 
2)'),2), '0FFFFF212');
SELECT ST_Relate(ST_GeometryFromText('POINT(1 2)'), ST_Buffer(ST_GeometryFromText('POINT(1 
2)'),2), '*FF*FF212');
SELECT ST_InterpolatePoint('LINESTRING M (0 0 0, 10 0 20)', 'POINT(5 5)');
SELECT ST_Extent(foo.the_geom) As b3extent
FROM (SELECT ST_Translate(ST_Force_2D(ST_LineToCurve(ST_Buffer(ST_MakePoint(x,y),1))),0,0) As the_geom
FROM generate_series(1,3) As x
CROSS JOIN generate_series(1,2) As y
) As foo;

SELECT ST_Extent(foo.the_geom) As b3extent
FROM (SELECT ST_MakePoint(x,y) As the_geom
FROM generate_series(1,3) As x
CROSS JOIN generate_series(1,2) As y
) As foo;
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

SELECT ST_GMLToSQL('
<gml:LineString srsName="EPSG:4269">
<gml:coordinates>
-71.16028,42.258729 -71.160837,42.259112 -71.161143,42.25932
</gml:coordinates>
</gml:LineString>');
SELECT
ST_ConcaveHull(
ST_Union(ST_GeomFromText  
('POLYGON((175 150, 20 40,
50 60, 125 100,  
175 150))'),
ST_Buffer(ST_GeomFromText  
('POINT(110 170)'), 20)
), 1)
As convexhull;

SELECT
ST_ConcaveHull(
ST_Union(ST_GeomFromText  
('POLYGON((175 150, 20 40,
50 60, 125 100,  
175 150))'),
ST_Buffer(ST_GeomFromText  
('POINT(110 170)'), 20)
), 0.9)
As target_90;

SELECT ST_ConcaveHull(ST_Collect(geom),  
0.99)
FROM (SELECT (ST_DumpPoints(ST_GeomFromText(
'MULTIPOINT(14 14,34 14,54 14,74 14,94  
14,114 14,134 14,
150 14,154 14,154 6,134 6,114 6,94 6,74  
6,54 6,34 6,
14 6,10 6,8 6,7 7,6 8,6 10,6 30,6 50,6  
70,6 90,6 110,6 130,
6 150,6 170,6 190,6 194,14 194,14 174,14  
154,14 134,14 114,
14 94,14 74,14 54,14 34,14 14)'))).geom ) foo;

SELECT ST_AsEWKT('0103000020E61000000100000005000000000000000000000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000'::geometry);
SELECT ST_AsEWKT('0108000080030000000000000060 E30A4100000000785C0241000000000000F03F0000000018E20A4100000000485F024100000000000000400000000018E20A4100000000305C02410000000000000840');
SELECT ST_AsText(ST_LineToCurve(foo.the_geom)) As curvedastext,ST_AsText(foo.the_geom) As 
non_curvedastext
FROM (SELECT ST_Buffer('POINT(1 3)'::geometry, 3) As the_geom) As foo;

SELECT ST_AsEWKT(ST_LineToCurve(ST_GeomFromEWKT('LINESTRING(1 2 3, 3 4 8, 5 6 4, 7 8 4, 9 
10 4)')));
SELECT ST_BuildArea(ST_Collect(smallc,bigc))
FROM (SELECT
ST_Buffer(
ST_GeomFromText('POINT(100 90)'), 25) As smallc,
ST_Buffer(ST_GeomFromText('POINT(100 90)'), 50) As bigc) As foo;

SELECT ST_BuildArea(ST_Collect(line,circle))
FROM (SELECT
ST_Buffer(
ST_MakeLine(ST_MakePoint(10, 10),ST_MakePoint(190, 190)),
5) As line,
ST_Buffer(ST_GeomFromText('POINT(100 90)'), 50) As circle) As foo;

SELECT ST_BuildArea(
ST_Collect(ST_ExteriorRing(line),ST_ExteriorRing(circle))
)
FROM (SELECT ST_Buffer(
ST_MakeLine(ST_MakePoint(10, 10),ST_MakePoint(190, 190))
,5) As line,
ST_Buffer(ST_GeomFromText('POINT(100 90)'), 50) As circle) As foo;

SELECT ST_AsText(ST_MinimumBoundingCircle(
ST_Collect(
ST_GeomFromEWKT('LINESTRING(55 75,125 150)'),
ST_Point(20, 80)), 8
)) As wktmbc;
SELECT ST_AsEWKT(
ST_ExteriorRing(
ST_GeomFromEWKT('POLYGON((0 0 1, 1 1 1, 1 2 1, 1 1 1, 0 0 1))')
)
);

SELECT tbl1.column1, tbl2.column1, tbl1.column2 <<| tbl2.column2 AS below
FROM
( VALUES
(1, 'LINESTRING (0 0, 4 3)'::geometry)) AS tbl1,
( VALUES
(2, 'LINESTRING (1 4, 1 7)'::geometry),
(3, 'LINESTRING (6 1, 6 5)'::geometry),
(4, 'LINESTRING (2 3, 5 6)'::geometry)) AS tbl2;

SELECT ST_IsValidReason('LINESTRING(220227 150406,2220227 150407,222020 150410)');
SELECT ST_AsEWKT(a.geom), ST_HasArc(a.geom)
FROM ( SELECT (ST_Dump(p_geom)).geom AS geom
FROM (SELECT ST_GeomFromEWKT('COMPOUNDCURVE(CIRCULARSTRING(0 0, 1 1, 1 0),(1 0, 0 
1))') AS p_geom) AS b
) AS a;

SELECT (a.p_geom).path[1] As path, ST_AsEWKT((a.p_geom).geom) As geom_ewkt
FROM (SELECT ST_Dump(ST_GeomFromEWKT('POLYHEDRALSURFACE(
((0 0 0, 0 0 1, 0 1 1, 0 1 0, 0 0 0)),
((0 0 0, 0 1 0, 1 1 0, 1 0 0, 0 0 0)), ((0 0 0, 1 0 0, 1 0 1, 0 0 1, 0 0 0)), ((1 1 0, 1 1 
1, 1 0 1, 1 0 0, 1 1 0)),
((0 1 0, 0 1 1, 1 1 1, 1 1 0, 0 1 0)), ((0 0 1, 1 0 1, 1 1 1, 0 1 1, 0 0 1)))') ) AS p_geom ) AS a;

SELECT (g.gdump).path, ST_AsEWKT((g.gdump).geom) as wkt
FROM
(SELECT
ST_Dump( ST_GeomFromEWKT('TIN (((
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
)') ) AS gdump
) AS g;
SELECT ST_Polygon(ST_GeomFromText('LINESTRING(75.15 29.53,77 29,77.6 29.5, 75.15 29.53)'), 
4326);
SELECT ST_AsEWKT(ST_Polygon(ST_GeomFromEWKT('LINESTRING(75.15 29.53 1,77 29 1,77.6 29.5 1, 
75.15 29.53 1)'), 4326));
SELECT ST_3DDWithin(
ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 4)'),2163),
ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 
20)'),2163),
126.8
) As within_dist_3d,
ST_DWithin(
ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 4)'),2163),
ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 
20)'),2163),
126.8
) As within_dist_2d;
SELECT round(CAST(ST_Distance_Sphere(ST_Centroid(the_geom), ST_GeomFromText('POINT(-118 38)',4326)) As numeric),2) As dist_meters,
round(CAST(ST_Distance(ST_Transform(ST_Centroid(the_geom),32611),
ST_Transform(ST_GeomFromText('POINT(-118 38)', 4326),32611)) As numeric),2) As 
dist_utm11_meters,
round(CAST(ST_Distance(ST_Centroid(the_geom), ST_GeomFromText('POINT(-118 38)', 4326)) As 
numeric),5) As dist_degrees,
round(CAST(ST_Distance(ST_Transform(the_geom,32611),
ST_Transform(ST_GeomFromText('POINT(-118 38)', 4326),32611)) As numeric),2) As 
min_dist_line_point_meters
FROM
(SELECT ST_GeomFromText('LINESTRING(-118.584 38.374,-118.583 38.5)', 4326) As the_geom) 
as foo;
SELECT ST_AsEWKT(ST_RotateZ(ST_GeomFromEWKT('LINESTRING(1 2 3, 1 1 1)'), pi()/2));
SELECT ST_AsEWKT(ST_RotateZ(the_geom, pi()/2))
FROM (SELECT ST_LineToCurve(ST_Buffer(ST_GeomFromText('POINT(234 567)'), 3)) As the_geom) 
As foo;
SELECT ST_NumPoints(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 
29.07)'));
SELECT ST_ContainsProperly(smallc, bigc) As smallcontainspropbig,
ST_ContainsProperly(bigc,smallc) As bigcontainspropsmall,
ST_ContainsProperly(bigc, ST_Union(smallc, bigc)) as bigcontainspropunion,
ST_Equals(bigc, ST_Union(smallc, bigc)) as bigisunion,
ST_Covers(bigc, ST_ExteriorRing(bigc)) As bigcoversexterior,
ST_ContainsProperly(bigc, ST_ExteriorRing(bigc)) As bigcontainsexterior
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc,
ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;
