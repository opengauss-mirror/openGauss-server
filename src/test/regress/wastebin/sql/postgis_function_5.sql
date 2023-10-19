set current_schema=postgis;
SELECT ST_SetSRID(ST_Point(-71.1043443253471, 42.3150676015829),4326);
select PostGIS_Full_Version();
SELECT ST_AsKML(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))',4326));
SELECT ST_AsHEXEWKB(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))',4326));
SELECT ST_Area(the_geom) As sqft, ST_Area(the_geom)*POWER(0.3048,2) As sqm
FROM (SELECT
ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,
743265 2967450,743265.625 2967416,743238 2967416))',2249) ) As foo(the_geom);

SELECT ST_Area(the_geom) As sqft, ST_Area(ST_Transform(the_geom,26986)) As sqm
FROM (SELECT
ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,
743265 2967450,743265.625 2967416,743238 2967416))',2249) ) As foo(the_geom);

SELECT ST_Area(the_geog)/POWER(0.3048,2) As sqft_spheroid, ST_Area(the_geog,false)/POWER 
(0.3048,2) As sqft_sphere, ST_Area(the_geog) As sqm_spheroid
FROM (SELECT
geography(
ST_Transform(
ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,743265 2967450,743265.625 
2967416,743238 2967416))',
2249
) ,4326
)
)
) As foo(the_geog);
SELECT ST_Disjoint('POINT(0 0)'::geometry, 'LINESTRING ( 0 0, 0 2 )'::geometry);

SELECT ST_Disjoint('POINT(0 0)'::geometry, 'LINESTRING ( 2 0, 0 2 )'::geometry);

SELECT ST_AsEWKT(ST_3DShortestLine(line,pt)) AS shl3d_line_pt,
ST_AsEWKT(ST_ShortestLine(line,pt)) As shl2d_line_pt
FROM (SELECT 'POINT(100 100 30)'::geometry As pt,
'LINESTRING (20 80 20, 98 190 1, 110 180 3, 50 75 1000)':: 
geometry As line
) As foo;


SELECT ST_AsEWKT(ST_3DShortestLine(line,pt)) AS shl3d_line_pt,
ST_AsEWKT(ST_ShortestLine(line,pt)) As shl2d_line_pt
FROM (SELECT 'MULTIPOINT(100 100 30, 50 74 1000)'::geometry As pt,
'LINESTRING (20 80 20, 98 190 1, 110 180 3, 50 75 900)':: 
geometry As line
) As foo;
SELECT ST_3DIntersects(pt, line), ST_Intersects(pt,line)
FROM (SELECT 'POINT(0 0 2)'::geometry As pt,
'LINESTRING (0 0 1, 0 2 3 )'::geometry As line) As foo;
SELECT ST_AsText(ST_PointFromGeoHash('9qqj7nmxncgyy4d0dbxqz0'));
SELECT ST_AsText(ST_PointFromGeoHash('9qqj7nmxncgyy4d0dbxqz0'));
SELECT '#1719.1', ST_AsEWKT(ST_MakeValid('POINT(0 0)'));
SELECT '#1719.2', ST_AsEWKT(ST_MakeValid('GEOMETRYCOLLECTION(POINT(0 0),MULTIPOINT(3 4,5 2),LINESTRING(4 4, 4 4),POLYGON((0 0,10 10,0 10,10 0,0 0)))'));
SELECT '#1719.3', ST_AsEWKT(ST_MakeValid('MULTIPOINT(3 4,5 2)'));
SELECT 'numInteriorRings01', ST_numInteriorRings(ST_Geomfromewkt('CURVEPOLYGON(CIRCULARSTRING(
                -2 0,
                -1 -1,
                0 0,
                1 -1,
                2 0,
                0 2,
                -2 0),
                (-1 0,
                0 0.5,
                1 0,
                0 1,
                -1 0))')) ;
SELECT 'numInteriorRings02', ST_numInteriorRings(ST_Geomfromewkt('CURVEPOLYGONM(CIRCULARSTRING(
                -2 0 0,
                -1 -1 2,
                0 0 4,
                1 -1 6,
                2 0 8,
                0 2 4,
                -2 0 0),
                (-1 0 2,
                0 0.5 4,
                1 0 6,
                0 1 4,
                -1 0 2))')) ;
SELECT 'numInteriorRings03', ST_numInteriorRings(ST_Geomfromewkt('CURVEPOLYGON(CIRCULARSTRING(
                -2 0 0,
                -1 -1 1,
                0 0 2,
                1 -1 3,
                2 0 4,
                0 2 2,
                -2 0 0),
                (-1 0 1,
                0 0.5 2,
                1 0 3,
                0 1 3,
                -1 0 1))'));

SELECT ST_SetSRID(ST_Point(-123.365556, 48.428611),4326) As wgs84long_lat;
SELECT ST_Transform(ST_SetSRID(ST_Point(-123.365556, 48.428611),4326),3785) As spere_merc;
SELECT tbl1.column1, tbl2.column1, tbl1.column2 && tbl2.column2 AS overlaps
FROM ( VALUES
(1, 'LINESTRING(0 0, 3 3)'::geometry),
(2, 'LINESTRING(0 1, 0 5)'::geometry)) AS tbl1,
( VALUES
(3, 'LINESTRING(1 2, 4 6)'::geometry)) AS tbl2;
SELECT ST_Azimuth(ST_Point(25,45), ST_Point(75,100))/(2*pi())*360 as degA_B,
ST_Azimuth(ST_Point(75,100), ST_Point(25,45))/(2*pi())*360 As degB_A;


SELECT degrees( ST_Azimuth(ST_Point(25,45), ST_Point(75,100)) ) as degA_B,
degrees( ST_Azimuth(ST_Point(75,100), ST_Point(25,45)) ) As degB_A;

SELECT round(CAST(
ST_Distance_Spheroid(ST_Centroid(the_geom), ST_GeomFromText('POINT(-118 38)',4326), 'SPHEROID["WGS 84",6378137,298.257223563]')
As numeric),2) As dist_meters_spheroid,
round(CAST(ST_Distance_Sphere(ST_Centroid(the_geom), ST_GeomFromText('POINT(-118 38) 
',4326)) As numeric),2) As dist_meters_sphere,
round(CAST(ST_Distance(ST_Transform(ST_Centroid(the_geom),32611),
ST_Transform(ST_GeomFromText('POINT(-118 38)', 4326),32611)) As numeric),2) As 
dist_utm11_meters
FROM
(SELECT ST_GeomFromText('LINESTRING(-118.584 38.374,-118.583 38.5)', 4326) As the_geom) 
as foo;
SELECT ST_GeometryFromText('CURVEPOLYGON(CIRCULARSTRING(4 2,3 -1.0,1 -1,-1.0 4,4 2))');
SELECT ST_GeometryFromText('CURVEPOLYGON(CIRCULARSTRING(4 2,3 -1.0,1 -1,-1.0 4,4 2))',4326);
SELECT ST_GeomFromText('LINESTRING(-71.160281 42.258729,-71.160837 42.259113,-71.161144 
42.25932)');
SELECT ST_GeomFromText('LINESTRING(-71.160281 42.258729,-71.160837 42.259113,-71.161144 
42.25932)',4269);
SELECT ST_GeomFromText('MULTILINESTRING((-71.160281 42.258729,-71.160837 
42.259113,-71.161144 42.25932))');
SELECT ST_GeomFromText('POINT(-71.064544 42.28787)');
SELECT ST_GeomFromText('POLYGON((-71.1776585052917 42.3902909739571,-71.1776820268866 
42.3903701743239,
-71.1776063012595 42.3903825660754,-71.1775826583081 42.3903033653531,-71.1776585052917 
42.3902909739571))');
SELECT ST_AsEWKT(ST_TransScale(ST_GeomFromEWKT('LINESTRING(1 2 3, 1 1 1)'), 0.5, 1, 1, 2));
SELECT ST_AsText(ST_Transscale(ST_LineToCurve(ST_Buffer('POINT(234 567)', 3)),1,2,3,4));
SELECT ST_SetSRID(ST_MakePoint(-71.1043443253471, 42.3150676015829),4326);
SELECT ST_Split(circle, line)
FROM (SELECT
ST_MakeLine(ST_MakePoint(10, 10),ST_MakePoint(190, 190)) As line,
ST_Buffer(ST_GeomFromText('POINT(100 90)'), 50) As circle) As foo;

SELECT ST_AsText((ST_Dump(ST_Split(circle, line))).geom) As wkt
FROM (SELECT
ST_MakeLine(ST_MakePoint(10, 10),ST_MakePoint(190, 190)) As line,
ST_Buffer(ST_GeomFromText('POINT(100 90)'), 50) As circle) As foo;
SELECT tbl1.column1, tbl2.column1, tbl1.column2 &< tbl2.column2 AS overleft
FROM
( VALUES
(1, 'LINESTRING(1 2, 4 6)'::geometry)) AS tbl1,
( VALUES
(2, 'LINESTRING(0 0, 3 3)'::geometry),
(3, 'LINESTRING(0 1, 0 5)'::geometry),
(4, 'LINESTRING(6 0, 6 1)'::geometry)) AS tbl2;
SELECT ST_Point_Inside_Circle(ST_Point(1,2), 0.5, 2, 3);
SELECT
ST_LineStringFromWKB(
ST_AsBinary(ST_GeomFromText('LINESTRING(1 2, 3 4)'))
) AS aline,
ST_LinestringFromWKB(
ST_AsBinary(ST_GeomFromText('POINT(1 2)'))
) IS NULL AS null_return;
SELECT ST_AsText(ST_GeomFromGeoJSON('{"type":"Point","coordinates":[-48.23456,20.12345]}')) 
As wkt;
SELECT ST_AsText(ST_GeomFromGeoJSON('{"type":"LineString","coordinates":[[1,2,3],[4,5,6],[7,8,9]]}')) As wkt;

SELECT ST_AsText(ST_LineMerge(
ST_GeomFromText('MULTILINESTRING((-29 -27,-30 -29.7,-36 -31,-45 -33),(-45 -33,-46 -32))')
)
);
SELECT ST_AsText(ST_LineMerge(
ST_GeomFromText('MULTILINESTRING((-29 -27,-30 -29.7,-36 -31,-45 -33),(-45.2 -33.2,-46 -32)) 
')
)
);
SELECT ST_YMax('BOX3D(1 2 3, 4 5 6)');
SELECT ST_YMax(ST_GeomFromText('LINESTRING(1 3 4, 5 6 7)'));

SELECT ST_Equals(ST_GeomFromText('LINESTRING(0 0, 10 10)'),
ST_GeomFromText('LINESTRING(0 0, 5 5, 10 10)'));

SELECT ST_Equals(ST_Reverse(ST_GeomFromText('LINESTRING(0 0, 10 10)')),
ST_GeomFromText('LINESTRING(0 0, 5 5, 10 10)'));

SELECT ST_WKTToSQL('LINESTRING(-71.160281 42.258729,-71.160837 42.259113,-71.161144 
42.25932)');
SELECT ST_WKTToSQL('MULTILINESTRING((-71.160281 42.258729,-71.160837 
42.259113,-71.161144 42.25932))');
SELECT ST_WKTToSQL('POINT(-71.064544 42.28787)');
SELECT ST_WKTToSQL('POLYGON((-71.1776585052917 42.3902909739571,-71.1776820268866 
42.3903701743239,
-71.1776063012595 42.3903825660754,-71.1775826583081 42.3903033653531,-71.1776585052917 
42.3902909739571))');
SELECT tbl1.column1, tbl2.column1, tbl1.column2 &<| tbl2.column2 AS overbelow
FROM
( VALUES
(1, 'LINESTRING(6 0, 6 4)'::geometry)) AS tbl1,
( VALUES
(2, 'LINESTRING(0 0, 3 3)'::geometry),(3, 'LINESTRING(0 1, 0 5)'::geometry),
(4, 'LINESTRING(1 2, 4 6)'::geometry)) AS tbl2;
SELECT ST_ZMin('BOX3D(1 2 3, 4 5 6)');
SELECT ST_ZMin(ST_GeomFromEWKT('LINESTRING(1 3 4, 5 6 7)'));
SELECT ST_ZMin('BOX3D(-3 2 1, 3 4 1)' );

SELECT ST_Distance(
ST_GeomFromText('POINT(-72.1235 42.3521)',4326),
ST_GeomFromText('LINESTRING(-72.1260 42.45, -72.123 42.1546)', 4326)
);

SELECT ST_Distance(
ST_Transform(ST_GeomFromText('POINT(-72.1235 42.3521)',4326),26986),
ST_Transform(ST_GeomFromText('LINESTRING(-72.1260 42.45, -72.123 42.1546)', 4326) 
,26986)
);

SELECT ST_Distance(
ST_Transform(ST_GeomFromText('POINT(-72.1235 42.3521)',4326),2163),
ST_Transform(ST_GeomFromText('LINESTRING(-72.1260 42.45, -72.123 42.1546)', 4326) 
,2163)
);

SELECT ST_Distance(gg1, gg2) As spheroid_dist, ST_Distance(gg1, gg2, false) As sphere_dist
FROM (SELECT
ST_GeographyFromText('SRID=4326;POINT(-72.1235 42.3521)') As gg1,
ST_GeographyFromText('SRID=4326;LINESTRING(-72.1260 42.45, -72.123 42.1546)') As gg2
) As foo ;
SELECT 1, ST_AsText(ST_UnaryUnion('MULTILINESTRING((0 0, 10 0), (5 -5, 5 5))'));
SELECT 2, ST_AsText(ST_UnaryUnion('GEOMETRYCOLLECTION(POLYGON((0 0, 10 0, 10 10, 0 10, 0 0)),POLYGON((5 5, 15 5, 15 15, 5 15, 5 5)))'));
SELECT 3, ST_AsText(ST_UnaryUnion('GEOMETRYCOLLECTION(POLYGON((0 0, 10 0, 10 10, 0 10, 0 0)),POLYGON((5 5, 15 5, 15 15, 5 15, 5 5)), MULTIPOINT(5 4, -5 4),LINESTRING(2 -10, 2 20))'));
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

SELECT ST_Buffer(
ST_GeomFromText(
'LINESTRING(50 50,150 150,150 50)'
), 10, 'endcap=square join=round');

SELECT ST_Buffer(
ST_GeomFromText(
'LINESTRING(50 50,150 150,150 50)'
), 10, 'join=bevel');

SELECT ST_Buffer(
ST_GeomFromText(
'LINESTRING(50 50,150 150,150 50)'
), 10, 'join=mitre mitre_limit=5.0');

SELECT ST_NPoints(ST_Buffer(ST_GeomFromText('POINT(100 90)'), 50)) As 
promisingcircle_pcount,
ST_NPoints(ST_Buffer(ST_GeomFromText('POINT(100 90)'), 50, 2)) As lamecircle_pcount;

SELECT ST_AsText(ST_Buffer(
ST_Transform(
ST_SetSRID(ST_MakePoint(-71.063526, 42.35785),4269), 26986)
,100,2)) As octagon;
----------------------

SELECT ST_Dimension('GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))');
SELECT ST_XMin('BOX3D(1 2 3, 4 5 6)');
SELECT ST_XMin(ST_GeomFromText('LINESTRING(1 3 4, 5 6 7)'));
SELECT ST_XMin(CAST('BOX(-3 2, 3 4)' As box2d));
SELECT ST_Covers(smallc,smallc) As smallinsmall,
ST_Covers(smallc, bigc) As smallcoversbig,
ST_Covers(bigc, ST_ExteriorRing(bigc)) As bigcoversexterior,
ST_Contains(bigc, ST_ExteriorRing(bigc)) As bigcontainsexterior
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc,
ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;

SELECT ST_Covers(geog_poly, geog_pt) As poly_covers_pt,
ST_Covers(ST_Buffer(geog_pt,10), geog_pt) As buff_10m_covers_cent
FROM (SELECT ST_Buffer(ST_GeogFromText('SRID=4326;POINT(-99.327 31.4821)'), 300) As 
geog_poly,
ST_GeogFromText('SRID=4326;POINT(-99.33 31.483)') As geog_pt ) As foo;

SELECT 'crosses', ST_crosses('LINESTRING(0 10, 0 -10)'::geometry, 'LINESTRING(0 0, 1 1)'::geometry);
SELECT 'crosses', ST_crosses('LINESTRING(0 10, 0 -10)'::geometry, 'LINESTRING(-4 0, 1 1)'::geometry);
SELECT ST_AsEWKT(ST_RotateY(ST_GeomFromEWKT('LINESTRING(1 2 3, 1 1 1)'), pi()/2));
SELECT ST_AsEWKT(ST_Scale(ST_GeomFromEWKT('LINESTRING(1 2 3, 1 1 1)'), 0.5, 0.75, 0.8));
SELECT ST_AsEWKT(ST_Scale(ST_GeomFromEWKT('LINESTRING(1 2 3, 1 1 1)'), 0.5, 0.75));
SELECT ST_AsEWKT(ST_Force3DM(ST_GeomFromEWKT('CIRCULARSTRING(1 1 2, 2 3 2, 4 5 2, 6 7 2, 5 
6 2)')));
SELECT ST_AsEWKT(ST_Force3DM('POLYGON((0 0 1,0 5 1,5 0 1,0 0 1),(1 1 1,3 1 1,1 3 1,1 1 1)) 
'));
SELECT n, ST_AsEWKT(ST_GeometryN(the_geom, n)) As geomewkt
FROM (
VALUES (ST_GeomFromEWKT('MULTIPOINT(1 2 7, 3 4 7, 5 6 7, 8 9 10)') ),
( ST_GeomFromEWKT('MULTICURVE(CIRCULARSTRING(2.5 2.5,4.5 2.5, 3.5 3.5), (10 11, 12 11))') )
)As foo(the_geom)
CROSS JOIN generate_series(1,100) n
WHERE n <= ST_NumGeometries(the_geom);


