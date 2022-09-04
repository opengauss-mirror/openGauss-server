set current_schema=postgis;
/*
###############################################################################
# TESTCASE NAME : postgis_realation_measure.sql
# COMPONENT(S)  : test function realtionship_measure
# PREREQUISITE  : 
# PLATFORM      : SUSE11.3
# DESCRIPTION   : 验证postgis几何对象关系和测量函数
# TAG           : ST_3DClosestPoint,ST_3DDistance
# TC LEVEL      : Level 1
################################################################################
*/

--S1.验证函数ST_3DClosestPoint返回两个几何最接近的点
SELECT ST_AsEWKT(ST_3DClosestPoint(line,pt)) AS cp3d_line_pt,ST_AsEWKT(ST_ClosestPoint(line,pt)) As cp2d_line_pt 
FROM (SELECT 
'POINT(100 100 30)'::geometry As pt,
'LINESTRING (20 80 20, 98 190 1, 110 180 3, 50 75 1000)':: geometry As line
) As foo;
--S2.验证函数ST_3DDistance返回两个几何三维笛卡尔最小距离
SELECT ST_3DDistance(
ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 4)'),2163),
ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 20)'),2163)
) As dist_3d,
ST_Distance(
ST_Transform(ST_GeomFromText('POINT(-72.1235 42.3521)',4326),2163),
ST_Transform(ST_GeomFromText('LINESTRING(-72.1260 42.45, -72.123 42.1546)', 4326) ,2163)
) As dist_2d;
--S3.验证函数ST_3DDWithin如果两个几何在3D距离单位内返回true
SELECT ST_3DDWithin(
ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 4)'),2163),
ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 20)'),2163),
126.8
) As within_dist_3d,
ST_DWithin(
ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 4)'),2163),
ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 20)'),2163),
126.8
) As within_dist_2d;
--S4.验证函数T_3DDFullyWithin如果所有几何都在指定范围内返回true 
SELECT ST_3DDFullyWithin(geom_a, geom_b, 10) as D3DFullyWithin10, ST_3DDWithin(geom_a, geom_b, 10) as D3DWithin10,
ST_DFullyWithin(geom_a, geom_b, 20) as D2DFullyWithin20,
ST_3DDFullyWithin(geom_a, geom_b, 20) as D3DFullyWithin20 from
(select ST_GeomFromEWKT('POINT(1 1 2)') as geom_a,
ST_GeomFromEWKT('LINESTRING(1 5 2, 2 7 20, 1 9 100, 14 12 3)') as geom_b) t1;
--S5.验证函数ST_3DIntersects如果几何图形在空间相交返回true
SELECT ST_3DIntersects(pt, line), ST_Intersects(pt,line)
FROM (SELECT 'POINT(0 0 2)'::geometry As pt,
'LINESTRING (0 0 1, 0 2 3 )'::geometry As line) As foo;
--S6.验证函数ST_3DLongestLine返回两个几何间三维最长线
SELECT ST_AsEWKT(ST_3DLongestLine(line,pt)) AS lol3d_line_pt,
ST_AsEWKT(ST_LongestLine(line,pt)) As lol2d_line_pt
FROM (SELECT 'POINT(100 100 30)'::geometry As pt,
'LINESTRING (20 80 20, 98 190 1, 110 180 3, 50 75 1000)':: geometry As line
) As foo;
--S7.验证函数ST_3DMaxDistance返回两个几何三维笛卡尔最大距离
SELECT ST_3DMaxDistance(
ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 10000)'),2163),
ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 20)'),2163)
) As dist_3d,
ST_MaxDistance(
ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 10000)'),2163),
ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 20)'),2163)
) As dist_2d;
--S8.验证函数ST_3DShortestLine返回两个几何间三维最短线
SELECT ST_AsEWKT(ST_3DShortestLine(line,pt)) AS shl3d_line_pt,
ST_AsEWKT(ST_ShortestLine(line,pt)) As shl2d_line_pt
FROM (SELECT 'POINT(100 100 30)'::geometry As pt,
'LINESTRING (20 80 20, 98 190 1, 110 180 3, 50 75 1000)':: geometry As line
) As foo;
--S9.验证函数ST_Area返回区域面积
SELECT ST_Area(the_geom) As sqft, ST_Area(the_geom)*POWER(0.3048,2) As sqm
FROM (SELECT
ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,
743265 2967450,743265.625 2967416,743238 2967416))',2249) ) As foo(the_geom);
--S10.验证函数ST_Azimuth返回给予北方位的方位角
SELECT degrees(ST_Azimuth(ST_Point(25, 45), ST_Point(75, 100))) AS degA_B,
degrees(ST_Azimuth(ST_Point(75, 100), ST_Point(25, 45))) AS degB_A;
--S11.验证函数ST_Centroid返回几何体的几何中心
SELECT ST_AsText(ST_Centroid('MULTIPOINT ( -1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6 )'));
--S12.验证函数ST_ClosestPoint返回最接近给定几何的二维点
SELECT ST_AsText(ST_ClosestPoint(pt,line) ) AS cp_pt_line,
ST_AsText(ST_ClosestPoint(line,pt )) As cp_line_pt
FROM (SELECT 'POINT(100 100)'::geometry As pt,
'LINESTRING (20 80, 98 190, 110 180, 50 75 )'::geometry As line
) As foo;
--S13.验证函数ST_Contains几何A在几何B的内部
SELECT ST_Contains(smallc, bigc) As smallcontainsbig,
ST_Contains(bigc,smallc) As bigcontainssmall,
ST_Contains(bigc, ST_Union(smallc, bigc)) as bigcontainsunion,
ST_Equals(bigc, ST_Union(smallc, bigc)) as bigisunion,
ST_Covers(bigc, ST_ExteriorRing(bigc)) As bigcoversexterior,
ST_Contains(bigc, ST_ExteriorRing(bigc)) As bigcontainsexterior
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc,
ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;
--S14.验证函数ST_ContainsProperly几何A在几何B的内部且不与B的边界相交
SELECT ST_ContainsProperly(smallc, bigc) As smallcontainspropbig,
ST_ContainsProperly(bigc,smallc) As bigcontainspropsmall,
ST_ContainsProperly(bigc, ST_Union(smallc, bigc)) as bigcontainspropunion,
ST_Equals(bigc, ST_Union(smallc, bigc)) as bigisunion,
ST_Covers(bigc, ST_ExteriorRing(bigc)) As bigcoversexterior,
ST_ContainsProperly(bigc, ST_ExteriorRing(bigc)) As bigcontainsexterior
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc,
ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;
--S15.验证函数ST_Covers几何B的点不在几何A之外
SELECT ST_Covers(smallc,smallc) As smallinsmall,
ST_Covers(smallc, bigc) As smallcoversbig,
ST_Covers(bigc, ST_ExteriorRing(bigc)) As bigcoversexterior,
ST_Contains(bigc, ST_ExteriorRing(bigc)) As bigcontainsexterior
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc,
ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;
--S16.验证函数ST_CoveredBy几何B的点不在几何A之外
SELECT ST_CoveredBy(smallc,smallc) As smallinsmall,
ST_CoveredBy(smallc, bigc) As smallcoveredbybig,
ST_CoveredBy(ST_ExteriorRing(bigc), bigc) As exteriorcoveredbybig,
ST_Within(ST_ExteriorRing(bigc),bigc) As exeriorwithinbig
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc,
ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;
--S17.验证函数ST_Crosses两个几何相交但不重合
SELECT 'crosses', ST_crosses('LINESTRING(0 10, 0 -10)'::geometry, 'LINESTRING(-4 0, 1 1)'::geometry);
--S18.验证函数ST_LineCrossingDirection返回两个线段的交叉行为
SELECT ST_LineCrossingDirection(foo.line1, foo.line2) As l1_cross_l2 ,
ST_LineCrossingDirection(foo.line2, foo.line1) As l2_cross_l1
FROM (
SELECT
ST_GeomFromText('LINESTRING(25 169,89 114,40 70,86 43)') As line1,
ST_GeomFromText('LINESTRING(171 154,20 140,71 74,161 53)') As line2
) As foo;
--S19.验证函数ST_Disjoint两个几何不共享任何空间
SELECT ST_Disjoint('POINT(0 0)'::geometry, 'LINESTRING ( 0 0, 0 2 )'::geometry);
--S20.验证函数ST_Distance返回两个几何之间的2D笛卡尔距离
SELECT ST_Distance(
ST_GeomFromText('POINT(-72.1235 42.3521)',4326),
ST_GeomFromText('LINESTRING(-72.1260 42.45, -72.123 42.1546)', 4326)
);
--S20.验证函数ST_HausdorffDistance返回两个几何体之间的Hausdorff距离
SELECT ST_HausdorffDistance(
'LINESTRING (0 0, 2 0)'::geometry,
'MULTIPOINT (0 1, 1 0, 2 1)'::geometry);
--S21.验证函数ST_MaxDistance返回投影单位中两个几何之间的最大距离
SELECT ST_MaxDistance('POINT(0 0)'::geometry, 'LINESTRING ( 2 0, 0 2 )'::geometry );
--S22.验证函数ST_DistanceSphere返回两个几何图形之间的最小距离
SELECT round(CAST(ST_DistanceSphere(ST_Centroid(the_geom), ST_GeomFromText('POINT(-118 38) ',4326)) As numeric),2) As dist_meters,
round(CAST(ST_Distance(ST_Transform(ST_Centroid(the_geom),32611),
ST_Transform(ST_GeomFromText('POINT(-118 38)', 4326),32611)) As numeric),2) As dist_utm11_meters,
round(CAST(ST_Distance(ST_Centroid(the_geom), ST_GeomFromText('POINT(-118 38)', 4326)) As numeric),5) As dist_degrees,
round(CAST(ST_Distance(ST_Transform(the_geom,32611),
ST_Transform(ST_GeomFromText('POINT(-118 38)', 4326),32611)) As numeric),2) As min_dist_line_point_meters
FROM
(SELECT ST_GeomFromText('LINESTRING(-118.584 38.374,-118.583 38.5)', 4326) As the_geom) as foo;
--S23.验证函数ST_DistanceSpheroid返回给定特定椭球体的几何图形最小距离
--S24.验证函数ST_DFullyWithin所有几何图形都在彼此距离内
SELECT ST_DFullyWithin(geom_a, geom_b, 10) as DFullyWithin10, ST_DWithin(geom_a, geom_b, 10) as DWithin10, ST_DFullyWithin(geom_a, geom_b, 20) as DFullyWithin20 from
(select ST_GeomFromText('POINT(1 1)') as geom_a,ST_GeomFromText('LINESTRING(1 5, 2 7, 1 9, 14 12)') as geom_b) t1;
--S25.验证函数ST_DWithin几何图形在彼此指定距离
SELECT '#1264', ST_DWithin('POLYGON((-10 -10, -10 10, 10 10, 10 -10, -10 -10))'::geography, 'POINT(0 0)'::geography, 0);
--S26.验证函数ST_Equals给定的几何体相同
SELECT ST_Equals(ST_GeomFromText('LINESTRING(0 0, 10 10)'),
ST_GeomFromText('LINESTRING(0 0, 5 5, 10 10)'));
--S27.验证函数ST_HasArc包含圆形字符串
SELECT ST_HasArc(ST_Collect('LINESTRING(1 2, 3 4, 5 6)', 'CIRCULARSTRING(1 1, 2 3, 4 5, 6 7, 5 6)'));
--S28.验证函数ST_Intersects几何在2D空间中相交
SELECT ST_Intersects('POINT(0 0)'::geometry, 'LINESTRING ( 2 0, 0 2 )'::geometry);
--S29.验证函数ST_Length返回几何图形的2D距离
SELECT ST_Length(ST_GeomFromText('LINESTRING(743238 2967416,743238 2967450,743265 2967450,
743265.625 2967416,743238 2967416)',2249));
--S30.验证函数ST_Length2D返回几何图形的2D距离

--S31.验证函数ST_3DLength返回几何图形的3D距离
SELECT ST_3DLength(ST_GeomFromText('LINESTRING(743238 2967416 1,743238 2967450 1,743265 2967450 3,
743265.625 2967416 3,743238 2967416 3)',2249));
--S33.验证函数ST_Length2D_Spheroid计算椭球体上几何图形的2D长度
SELECT ST_Length2D_Spheroid( the_geom, sph_m ) As tot_len,
ST_Length2D_Spheroid(ST_GeometryN(the_geom,1), sph_m) As len_line1,
ST_Length2D_Spheroid(ST_GeometryN(the_geom,2), sph_m) As len_line2
FROM (SELECT ST_GeomFromText('MULTILINESTRING((-118.584 38.374,-118.583 38.5),
(-71.05957 42.3589 , -71.061 43))') As the_geom,
CAST('SPHEROID["GRS_1980",6378137,298.257222101]' As spheroid) As sph_m) as foo;
--S34.验证函数ST_LongestLine返回两个几何图形的最长线条点
SELECT ST_AsText(
ST_LongestLine('POINT(100 100)':: geometry,
'LINESTRING (20 80, 98 190, 110 180, 50 75 )'::geometry)
) As lline;
--S35.验证函数ST_OrderingEquals给定几何体相同，且点的方向也相同
SELECT ST_OrderingEquals(ST_GeomFromText('LINESTRING(0 0, 10 10)'),
ST_GeomFromText('LINESTRING(0 0, 5 5, 10 10)'));
--S36.验证函数ST_Overlaps几何体共享空间
SELECT ST_Overlaps(a,b) As a_overlap_b,
ST_Crosses(a,b) As a_crosses_b,
ST_Intersects(a, b) As a_intersects_b, ST_Contains(b,a) As b_contains_a
FROM (SELECT ST_GeomFromText('POINT(1 0.5)') As a, ST_GeomFromText('LINESTRING(1 0, 1 1, 3 5)') As b)
As foo;
--S37.验证函数ST_Perimeter返回几何边界的长度
SELECT ST_Perimeter(ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,743265 2967450,
743265.625 2967416,743238 2967416))', 2249));
--S38.验证函数ST_Perimeter2D返回多边形的二维边界

--S39.验证函数ST_3DPerimeter返回多边形的三维边界
SELECT ST_3DPerimeter(the_geom), ST_Perimeter2d(the_geom), ST_Perimeter(the_geom) FROM
(SELECT ST_GeomFromEWKT('SRID=2249;POLYGON((743238 2967416 2,743238 2967450 1,
743265.625 2967416 1,743238 2967416 2))') As the_geom) As foo;
--S40.验证函数ST_PointOnSurface返回一个位于表面的点
SELECT ST_AsText(ST_PointOnSurface('POINT(0 5)'::geometry));
--S41.验证函数ST_Project返回从起点投影的点
SELECT ST_AsText(ST_Project('POINT(0 0)'::geography, 100000, radians(45.0)));
--S42.验证函数ST_Relate几何体相关
SELECT ST_Relate(ST_GeometryFromText('POINT(1 2)'), ST_Buffer(ST_GeometryFromText('POINT(1 
2)'),2), '*FF*FF212');
--S43.验证函数ST_RelateMatch几何体相关匹配
SELECT ST_RelateMatch('101202FFF', 'TTTTTTFFF') ;
--S44.验证函数T_ShortestLine返回两个几何间的二维最短线
SELECT ST_AsText(
ST_ShortestLine('POINT(100 100) '::geometry,
'LINESTRING (20 80, 98 190, 110 180, 50 75 )'::geometry)
) As sline;
--S45.验证函数ST_Touches几何体表面接触
SELECT ST_Touches('LINESTRING(0 0, 1 1, 0 2)'::geometry, 'POINT(1 1)'::geometry);
--S46.验证函数ST_Within几何A完全位于几何B内
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

--I2.join测试
--S1.验证函数ST_3DClosestPoint join功能
SELECT ST_AsEWKT(ST_3DClosestPoint(ST_setsrid(geom_point.the_geom,4326), st_setsrid(geom_line.the_geom,4326))) from geom_point join geom_line on geom_point.id=15 and geom_line.id=7;  
--S2.验证函数ST_3DDistance join功能
SELECT ST_3DDistance(ST_setsrid(geom_point.the_geom,4326), st_setsrid(geom_line.the_geom,4326)) from geom_line join geom_point on geom_point.id=geom_line.id order by geom_point.id limit 10;
--S3.验证函数ST_3DDWithin如果两个几何在3D距离单位内返回true
SELECT ST_3DDWithin(ST_setsrid(geom_line.the_geom,4326), st_setsrid(geom_polygon.the_geom,4326),10) from geom_line join geom_polygon on geom_polygon.id=geom_line.id order by geom_line.id limit 10;

--增加
select ST_FindExtent('geom_polygon','the_geom');
select ST_Find_Extent('postgis','geom_polygon','the_geom');

select DropGeometryColumn('public2','cities');

select UpdateGeometrySRID('postgis','cities',3453);

CREATE TABLE cities ( id int4, name varchar(50) );
SELECT AddGeometryColumn('cities', 'the_geom', 4326, 'POINT', 2);
INSERT INTO cities (id, the_geom, name) VALUES (1,ST_GeomFromText('POINT(-0.1257 51.508)',4326),'London, England');
SELECT UpdateGeometrySRID('cities','the_geom',4333);
SELECT find_srid('postgis','cities', 'the_geom');
SELECT DROPGeometryColumn('cities', 'the_geom');
SELECT DropGeometryTable ('cities');
SELECT AddGeometryColumn('cities', 'the_geom', 4326, 'POINT', 2);
SELECT DropGeometryTable ('cities');

CREATE TABLE cities ( id int4, name varchar(50) );
SELECT AddGeometryColumn('postgis', 'cities', 'the_geom', 4326, 'POINT', 2);
INSERT INTO cities (id, the_geom, name) VALUES (1,ST_GeomFromText('POINT(-0.1257 51.508)',4326),'London, England');
SELECT UpdateGeometrySRID('postgis','cities','the_geom',4333);
SELECT find_srid('postgis','cities', 'the_geom');
SELECT DROPGeometryColumn('postgis','cities', 'the_geom');
SELECT DropGeometryTable ('postgis','cities');
SELECT AddGeometryColumn('cities', 'the_geom', 4326, 'POINT', 2);
SELECT DropGeometryTable ('cities');

CREATE SCHEMA cities_schema;
CREATE TABLE cities_schema.cities ( id int4, name varchar(50) );
SELECT AddGeometryColumn('cities_schema', 'cities', 'the_geom', 4326, 'POINT', 2);
INSERT INTO cities_schema.cities (id, the_geom, name) VALUES (1,ST_GeomFromText('POINT(-0.1257 51.508)',4326),'London, England');
SELECT UpdateGeometrySRID('cities_schema','cities','the_geom',4333);
SELECT find_srid('cities_schema','cities', 'the_geom');

SELECT DROPGeometryColumn('cities_schema','cities', 'the_geom');
SELECT DropGeometryTable ('cities_schema','cities');
drop schema cities_schema cascade;

CREATE TABLE postgis.myspatial_table_cs(gid serial, geom geometry);
INSERT INTO myspatial_table_cs(geom) VALUES(ST_GeomFromText('LINESTRING(1 2, 3 4)',4326) );
SELECT Populate_Geometry_Columns('postgis.myspatial_table_cs'::regclass, false);
\d myspatial_table_cs 
DROP TABLE postgis.myspatial_table_cs;

CREATE TABLE postgis.myspatial_table_cs(gid serial, geom geometry);
INSERT INTO myspatial_table_cs(geom) VALUES(ST_GeomFromText('LINESTRING(1 2, 3 4)',4326) );
SELECT Populate_Geometry_Columns( false);
\d myspatial_table_cs
DROP TABLE postgis.myspatial_table_cs;
