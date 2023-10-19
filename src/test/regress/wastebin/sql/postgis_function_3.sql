set current_schema=postgis;
SELECT ST_GeometryType(geomA) As geomtype, ST_Contains(geomA,geomA) AS acontainsa, 
ST_ContainsProperly(geomA, geomA) AS acontainspropa,
ST_Contains(geomA, ST_Boundary(geomA)) As acontainsba, ST_ContainsProperly(geomA, 
ST_Boundary(geomA)) As acontainspropba
FROM (VALUES ( ST_Buffer(ST_Point(1,1), 5,1) ),
( ST_MakeLine(ST_Point(1,1), ST_Point(-1,-1) ) ),
( ST_Point(1,1) )
) As foo(geomA);
SELECT ST_3DPerimeter(the_geom), ST_Perimeter2d(the_geom), ST_Perimeter(the_geom) FROM
(SELECT ST_GeomFromEWKT('SRID=2249;POLYGON((743238 2967416 2,743238 2967450 1,
743265.625 2967416 1,743238 2967416 2))') As the_geom) As foo;
SELECT ST_Contains(smallc, bigc) As smallcontainsbig,
ST_Contains(bigc,smallc) As bigcontainssmall,
ST_Contains(bigc, ST_Union(smallc, bigc)) as bigcontainsunion,
ST_Equals(bigc, ST_Union(smallc, bigc)) as bigisunion,
ST_Covers(bigc, ST_ExteriorRing(bigc)) As bigcoversexterior,
ST_Contains(bigc, ST_ExteriorRing(bigc)) As bigcontainsexterior
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc,
ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;

SELECT ST_GeometryType(geomA) As geomtype, ST_Contains(geomA,geomA) AS acontainsa, 
ST_ContainsProperly(geomA, geomA) AS acontainspropa,
ST_Contains(geomA, ST_Boundary(geomA)) As acontainsba, ST_ContainsProperly(geomA, 
ST_Boundary(geomA)) As acontainspropba
FROM (VALUES ( ST_Buffer(ST_Point(1,1), 5,1) ),
( ST_MakeLine(ST_Point(1,1), ST_Point(-1,-1) ) ),
( ST_Point(1,1) )
) As foo(geomA);

SELECT ST_GeometryType(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 
29.31,77.29 29.07)'));

SELECT ST_GeometryType(ST_GeomFromEWKT('POLYHEDRALSURFACE( ((0 0 0, 0 0 1, 0 1 1, 0 1 0, 0 
0 0)),
((0 0 0, 0 1 0, 1 1 0, 1 0 0, 0 0 0)), ((0 0 0, 1 0 0, 1 0 1, 0 0 1, 0 0 0)),
((1 1 0, 1 1 1, 1 0 1, 1 0 0, 1 1 0)),
((0 1 0, 0 1 1, 1 1 1, 1 1 0, 0 1 0)), ((0 0 1, 1 0 1, 1 1 1, 0 1 1, 0 0 1)) )'));

SELECT ST_IsSimple(ST_GeomFromText('POLYGON((1 2, 3 4, 5 6, 1 2))'));
SELECT ST_IsSimple(ST_GeomFromText('LINESTRING(1 1,2 2,2 3.5,1 3,1 2,2 1)'));
SELECT tbl1.column1, tbl2.column1, tbl1.column2 >> tbl2.column2 AS right
FROM
( VALUES
(1, 'LINESTRING (2 3, 5 6)'::geometry)) AS tbl1,
( VALUES
(2, 'LINESTRING (1 4, 1 7)'::geometry),
(3, 'LINESTRING (6 1, 6 5)'::geometry),
(4, 'LINESTRING (0 0, 4 3)'::geometry)) AS tbl2;
SELECT ST_asewkt(ST_removepoint('LINESTRING(0 0 0 0, 1 1 1 1, 2 2 2 2)', 0));

SELECT ST_asewkt(ST_removepoint('LINESTRING(0 0 0 0, 1 1 1 1, 2 2 2 2)', 2));

SELECT ST_RemovePoint('POINT(-11.1111111 40)'::geometry, 1);
SELECT ST_AsEWKT(
ST_GeomFromWKB(E'\\001\\002\\000\\000\\000\\002\\000\\000\\000\\037\\205\\353Q\\270~\\\\\\300\\323Mb\\020X\\231C@\\020X9\\264\\310~\\\\\\300)\\\\\\217\\302\\365\\230C@',4326)
);

SELECT
ST_AsText(
ST_GeomFromWKB(
ST_AsEWKB('POINT(2 5)'::geometry)
)
);
SELECT 8, ST_AsText(ST_RemoveRepeatedPoints('POINT(0 0)'));

SELECT 10, ST_AsText(ST_RemoveRepeatedPoints('LINESTRING(0 0, 0 0)'));
SELECT 11, ST_AsText(ST_RemoveRepeatedPoints('LINESTRING(0 0, 0 0, 0 0, 0 0, 0 0)'));
SELECT 12, ST_SRID(ST_RemoveRepeatedPoints('SRID=3;LINESTRING(0 0, 0 0, 0 0, 0 0, 0 0)'));
SELECT 13, ST_AsText(ST_RemoveRepeatedPoints('LINESTRING(0 0, 1 0, 2 0, 3 0, 4 0)',1.5));
SELECT 14, ST_AsText(ST_RemoveRepeatedPoints('LINESTRING(10 0,10 9,10 10)', 2));
SELECT 15, ST_AsText(ST_RemoveRepeatedPoints('MULTIPOINT(0 0, 0 0, 1 1, 2 2)'::geometry));
SELECT 16, ST_AsText(ST_RemoveRepeatedPoints('MULTIPOINT(0 0, 0 0, 1 1, 2 2)'::geometry,0.1));
SELECT 17, ST_AsText(ST_RemoveRepeatedPoints('MULTIPOINT(0 0, 0 0, 1 1, 4 4)'::geometry,2));
SELECT ST_NPoints(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 
29.07)'));
SELECT ST_NPoints(ST_GeomFromEWKT('LINESTRING(77.29 29.07 1,77.42 29.26 0,77.27 29.31 
-1,77.29 29.07 3)'));
SELECT ST_AsEWKT(ST_AddPoint(ST_GeomFromEWKT('LINESTRING(0 0 1, 1 1 1)'), ST_MakePoint 
(1, 2, 3)));
SELECT CAST(ST_Expand(ST_GeomFromText('LINESTRING(2312980 110676,2312923 110701,2312892 
110714)', 2163),10) As box2d);

SELECT ST_Expand(CAST('BOX3D(778783 2951741 1,794875 2970042.61545891 10)' As box3d),10);

SELECT ST_AsEWKT(ST_Expand(ST_GeomFromEWKT('SRID=2163;POINT(2312980 110676)'),10));
SELECT ST_AsText(the_geom)
FROM
(SELECT ST_LocateBetween(
ST_GeomFromText('MULTILINESTRING M ((1 2 3, 3 4 2, 9 4 3),
(1 2 3, 5 4 5))'),1.5, 3) As the_geom) As foo;
SELECT ST_AsText((ST_Dump(the_geom)).geom)
FROM
(SELECT ST_LocateBetween(
ST_GeomFromText('MULTILINESTRING M ((1 2 3, 3 4 2, 9 4 3),
(1 2 3, 5 4 5))'),1.5, 3) As the_geom) As foo;
SELECT ST_AsEWKT(
ST_ForceRHR(
'POLYGON((0 0 2, 5 0 2, 0 5 2, 0 0 2),(1 1 2, 1 3 2, 3 1 2, 1 1 2))'
)
);
SELECT ST_HasArc(ST_Collect('LINESTRING(1 2, 3 4, 5 6)', 'CIRCULARSTRING(1 1, 2 3, 4 5, 6 
7, 5 6)'));
select 'cd1', 'LINESTRING(0 0,0 10)'::geometry <->
              'LINESTRING(4 0,4 10)'::geometry;
SELECT ST_AsEWKT(ST_Force3DZ(ST_GeomFromEWKT('CIRCULARSTRING(1 1 2, 2 3 2, 4 5 2, 6 7 2, 5 
6 2)')));
SELECT ST_AsEWKT(ST_Force3DZ('POLYGON((0 0,0 5,5 0,0 0),(1 1,3 1,1 3,1 1))'));
SELECT ST_X(ST_GeomFromEWKT('POINT(1 2 3 4)'));

SELECT ST_Y(ST_Centroid(ST_GeomFromEWKT('LINESTRING(1 2 3 4, 1 1 1 1)')));
SELECT ST_AsText(ST_ConvexHull(
ST_Collect(
ST_GeomFromText('MULTILINESTRING((100 190,10 8),(150 10, 20 30))'),
ST_GeomFromText('MULTIPOINT(50 5, 150 30, 50 10, 10 10)')
)) );
SELECT ST_3DExtent(foo.the_geom) As b3extent
FROM (SELECT ST_MakePoint(x,y,z) As the_geom
FROM generate_series(1,3) As x
CROSS JOIN generate_series(1,2) As y
CROSS JOIN generate_series(0,2) As Z) As foo;


SELECT ST_3DExtent(foo.the_geom) As b3extent
FROM (SELECT ST_Translate(ST_Force_3DZ(ST_LineToCurve(ST_Buffer(ST_MakePoint(x,y),1))),0,0, 
z) As the_geom
FROM generate_series(1,3) As x
CROSS JOIN generate_series(1,2) As y
CROSS JOIN generate_series(0,2) As Z) As foo;
SELECT ST_3DMaxDistance(
ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 10000)'),2163),
ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 
20)'),2163)
) As dist_3d,
ST_MaxDistance(
ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 10000)'),2163),
ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 
20)'),2163)
) As dist_2d;

SELECT ST_Zmflag(ST_GeomFromEWKT('LINESTRING(1 2, 3 4)'));
SELECT ST_Zmflag(ST_GeomFromEWKT('CIRCULARSTRING(1 2 3, 3 4 3, 5 6 3)'));
SELECT ST_Zmflag(ST_GeomFromEWKT('POINT(1 2 3 4)'));
SELECT ST_AsText(ST_OffsetCurve( 
ST_GeomFromText(
'LINESTRING(164 16,144 16,124 16,104 
16,84 16,64 16,
44 16,24 16,20 16,18 16,17 17,
16 18,16 20,16 40,16 60,16 80,16 
100,
16 120,16 140,16 160,16 180,16 
195)'),
15, 'quad_segs=4 join=round'));


SELECT ST_AsText(ST_Collect(
ST_OffsetCurve(geom, 15, 'quad_segs=4 join=round'),
ST_OffsetCurve(ST_OffsetCurve( 
geom,
-30, 'quad_segs=4 join=round'), 
-15, 'quad_segs=4 join=round')
)
) As parallel_curves
FROM ST_GeomFromText(
'LINESTRING(164 16,144 16,124 16,104 
16,84 16,64 16,
44 16,24 16,20 16,18 16,17 17,
16 18,16 20,16 40,16 60,16 80,16 
100,
16 120,16 140,16 160,16 180,16 
195)') As geom;
SELECT ST_AsText(
ST_PointN(
column1,
generate_series(1, ST_NPoints(column1))
))
FROM ( VALUES ('LINESTRING(0 0, 1 1, 2 2)'::geometry) ) AS foo;

SELECT ST_AsText(ST_PointN(ST_GeomFromText('CIRCULARSTRING(1 2, 3 2, 1 2)'),2));
SELECT 'valid wkb compound curve 1', ST_asEWKT(ST_GeomFromEWKB(decode('0109000000020000000102000000030000009FE5797057376340E09398B1B2373BC05AAE0A165F0963409F6760A2493D3DC0DB6286DFB057634082D8A1B32F843EC0010200000004000000DB6286DFB057634082D8A1B32F843EC075B4E4D0C60C634031FA5D1A371540C0D7197CED9B636340A3CB59A7630A41C050F4A72AC0FB6240974769FCE3CF41C0', 'hex')));

SELECT 'valid ewkb curve polygon 5', ST_AsEWKT(ST_GeomFromEWKB(decode('010a00000002000000010200000007000000ccdf061ad9f3614054093e6d99093ec0ab9085dbb6dd614081540229216040c0ebd7a828c33e62409bf026782a7e41c0000000c06bb2624000000020adb440c08e632f616ead6240c9f7b0bf1dd33dc09011eec0de4362407dd6672f76323ec0ccdf061ad9f3614054093e6d99093ec00109000000030000000108000000030000006844c4fe011b6240342e2993e0423fc0d45daf9d93066240c4a0c305d62240c000000080ac31624000000020fbbe40c001020000000200000000000080ac31624000000020fbbe40c0000000e0107f6240000000c0a10440c0010800000003000000000000e0107f6240000000c0a10440c04e1c0c14624c6240bf3fb6405c793fc06844c4fe011b6240342e2993e0423fc0', 'hex')));
SELECT ST_AsEWKT(ST_ForceCollection('POLYGON((0 0 1,0 5 1,5 0 1,0 0 1),(1 1 1,3 1 1,1 3 
1,1 1 1))'));

SELECT ST_AsText(ST_ForceCollection('CIRCULARSTRING(220227 150406,2220227 150407,220227 
150406)'));
SELECT ST_AsText(house_loc) As as_text_house_loc,
startstreet_num +
CAST( (endstreet_num - startstreet_num)
* ST_LineLocatePoint(street_line, house_loc) As integer) As street_num
FROM
(SELECT ST_GeomFromText('LINESTRING(1 2, 3 4)') As street_line,
ST_MakePoint(x*1.01,y*1.03) As house_loc, 10 As startstreet_num,
20 As endstreet_num
FROM generate_series(1,3) x CROSS JOIN generate_series(2,4) As y)
As foo
WHERE ST_DWithin(street_line, house_loc, 0.2);

SELECT ST_AsText(ST_LineInterpolatePoint(foo.the_line, ST_LineLocatePoint(foo.the_line, 
ST_GeomFromText('POINT(4 3)'))))
FROM (SELECT ST_GeomFromText('LINESTRING(1 2, 4 5, 6 7)') As the_line) As foo;
SELECT ST_Perimeter(ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,743265 2967450,
743265.625 2967416,743238 2967416))', 2249));

SELECT ST_Perimeter(ST_GeomFromText('MULTIPOLYGON(((763104.471273676 2949418.44119003,
763104.477769673 2949418.42538203,
763104.189609677 2949418.22343004,763104.471273676 2949418.44119003)),
((763104.471273676 2949418.44119003,763095.804579742 2949436.33850239,
763086.132105649 2949451.46730207,763078.452329651 2949462.11549407,
763075.354136904 2949466.17407812,763064.362142565 2949477.64291974,
763059.953961626 2949481.28983009,762994.637609571 2949532.04103014,
762990.568508415 2949535.06640477,762986.710889563 2949539.61421415,
763117.237897679 2949709.50493431,763235.236617789 2949617.95619822,
763287.718121842 2949562.20592617,763111.553321674 2949423.91664605,
763104.471273676 2949418.44119003)))', 2249));

SELECT ST_Perimeter(geog) As per_meters, ST_Perimeter(geog)/0.3048 As per_ft
FROM ST_GeogFromText('POLYGON(( 71.1776848522251 42.3902896512902, 71.1776843766326  
42.3903829478009,
 71.1775844305465 42.3903826677917, 71.1775825927231 42.3902893647987, 71.1776848522251  
42.3902896512902))') As geog;

SELECT ST_Perimeter(geog) As per_meters, ST_Perimeter(geog,false) As per_sphere_meters,  
ST_Perimeter(geog)/0.3048 As per_ft
FROM ST_GeogFromText('MULTIPOLYGON((( 71.1044543107478 42.340674480411, 71.1044542869917  
42.3406744369506,
 71.1044553562977 42.340673886454, 71.1044543107478 42.340674480411)),
(( 71.1044543107478 42.340674480411, 71.1044860600303 42.3407237015564, 71.1045215770124  
42.3407653385914,
 71.1045498002983 42.3407946553165, 71.1045611902745 42.3408058316308, 71.1046016507427  
42.340837442371,
 71.104617893173 42.3408475056957, 71.1048586153981 42.3409875993595, 71.1048736143677  
42.3409959528211,
 71.1048878050242 42.3410084812078, 71.1044020965803 42.3414730072048,
 71.1039672113619 42.3412202916693, 71.1037740497748 42.3410666421308,
 71.1044280218456 42.3406894151355, 71.1044543107478 42.340674480411)))') As geog;
 SELECT ST_IsCollection('LINESTRING(0 0, 1 1)'::geometry);
SELECT ST_IsCollection('MULTIPOINT EMPTY'::geometry);
SELECT ST_IsCollection('MULTIPOINT((0 0), (42 42))'::geometry);
SELECT ST_3DLength(ST_GeomFromText('LINESTRING(743238 2967416 1,743238 2967450 1,743265 
2967450 3,
743265.625 2967416 3,743238 2967416 3)',2249));
SELECT 'numInteriorRing01', ST_numInteriorRing(ST_Geomfromewkt('CURVEPOLYGON(CIRCULARSTRING(
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
SELECT 'numInteriorRing02', ST_numInteriorRing(ST_Geomfromewkt('CURVEPOLYGONM(CIRCULARSTRING(
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
SELECT 'numInteriorRing03', ST_numInteriorRing(ST_Geomfromewkt('CURVEPOLYGON(CIRCULARSTRING(
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

SELECT ST_AsEWKT(
ST_ForceRHR(
'POLYGON((0 0 2, 5 0 2, 0 5 2, 0 0 2),(1 1 2, 1 3 2, 3 1 2, 1 1 2))'
)
);

SELECT ST_AsText(ST_InteriorRingN(the_geom, 1)) As the_geom
FROM (SELECT ST_BuildArea(
ST_Collect(ST_Buffer(ST_Point(1,2), 20,3),
ST_Buffer(ST_Point(1, 2), 10,3))) As the_geom
) as foo;
SELECT ST_IsEmpty(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'));
SELECT ST_IsEmpty(ST_GeomFromText('POLYGON EMPTY'));
SELECT ST_IsEmpty(ST_GeomFromText('POLYGON((1 2, 3 4, 5 6, 1 2))'));
SELECT ST_IsEmpty(ST_GeomFromText('POLYGON((1 2, 3 4, 5 6, 1 2))')) = false;
SELECT ST_3DDFullyWithin(geom_a, geom_b, 10) as D3DFullyWithin10, ST_3DDWithin(geom_a,
geom_b, 10) as D3DWithin10,
ST_DFullyWithin(geom_a, geom_b, 20) as D2DFullyWithin20,
ST_3DDFullyWithin(geom_a, geom_b, 20) as D3DFullyWithin20 from
(select ST_GeomFromEWKT('POINT(1 1 2)') as geom_a,
ST_GeomFromEWKT('LINESTRING(1 5 2, 2 7 20, 1 9 100, 14 12 3)') as geom_b) t1;
SELECT ST_NRings(the_geom) As Nrings, ST_NumInteriorRings(the_geom) As ninterrings
FROM (SELECT ST_GeomFromText('POLYGON((1 2, 3 4, 5 6, 1 2))') As the_geom) As foo 
;
SELECT ST_Touches('LINESTRING(0 0, 1 1, 0 2)'::geometry, 'POINT(1 1)'::geometry);

SELECT ST_Touches('LINESTRING(0 0, 1 1, 0 2)'::geometry, 'POINT(0 2)'::geometry);

SELECT ST_AsText(ST_StartPoint('LINESTRING(0 1, 0 2)'::geometry));
SELECT ST_StartPoint('POINT(0 1)'::geometry) IS NULL AS is_null;
SELECT ST_AsEWKT(ST_StartPoint('LINESTRING(0 1 1, 0 2 2)'::geometry));
SELECT ST_AsText(
ST_GeogFromWKB(E'\\001\\002\\000\\000\\000\\002\\000\\000\\000\\037\\205\\353Q\\270~\\\\\\300\\323Mb\\020X\\231C@\\020X9\\264\\310~\\\\\\300)\\\\\\217\\302\\365\\230C@')
);
SELECT ST_AsSVG(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))',4326));
SELECT ST_AsText(ST_CurveToLine(ST_GeomFromText('CIRCULARSTRING(220268 150415,220227 
150505,220227 150406)')));
SELECT ST_AsEWKT(ST_CurveToLine(ST_GeomFromEWKT('CIRCULARSTRING(220268 150415 1,220227 
150505 2,220227 150406 3)')));


SELECT ST_AsText(ST_CurveToLine(ST_GeomFromText('CIRCULARSTRING(220268 150415,220227 
150505,220227 150406)'),2));
SELECT ST_AsText(ST_Collect(ST_GeomFromText('POINT(1 2)'),
ST_GeomFromText('POINT(-2 3)') ));

SELECT ST_AsText(ST_Collect(ST_GeomFromText('POINT(1 2)'),
ST_GeomFromText('POINT(1 2)') ) );

SELECT ST_AsEWKT(ST_Collect(ST_GeomFromEWKT('POINT(1 2 3)'),
ST_GeomFromEWKT('POINT(1 2 4)') ) );

SELECT ST_AsText(ST_Collect(ST_GeomFromText('CIRCULARSTRING(220268 150415,220227 
150505,220227 150406)'),
ST_GeomFromText('CIRCULARSTRING(220227 150406,2220227 150407,220227 150406)')));

SELECT ST_AsText(ST_Collect(ARRAY[ST_GeomFromText('LINESTRING(1 2, 3 4)'),
ST_GeomFromText('LINESTRING(3 4, 4 5)')])) As wktcollect;
SELECT ST_GeomFromKML('
<LineString>
<coordinates>-71.1663,42.2614
-71.1667,42.2616</coordinates>
</LineString>');
SELECT ST_OrderingEquals(ST_GeomFromText('LINESTRING(0 0, 10 10)'),
ST_GeomFromText('LINESTRING(0 0, 5 5, 10 10)'));

SELECT ST_OrderingEquals(ST_GeomFromText('LINESTRING(0 0, 10 10)'),
ST_GeomFromText('LINESTRING(0 0, 0 0, 10 10)'));

SELECT ST_OrderingEquals(ST_Reverse(ST_GeomFromText('LINESTRING(0 0, 10 10)')),
ST_GeomFromText('LINESTRING(0 0, 0 0, 10 10)'));


SELECT ST_CoordDim('CIRCULARSTRING(1 2 3, 1 3 4, 5 6 7, 8 9 10, 11 12 13)');
SELECT ST_CoordDim(ST_Point(1,2));
select ST_makebox2d('SRID=3;POINT(0 0)', 'SRID=3;POINT(1 1)');
select ST_makebox2d('POINT(0 0)', 'SRID=3;POINT(1 1)');
SELECT '#178a', ST_XMin(ST_MakeBox2D(ST_Point(5, 5), ST_Point(0, 0)));
SELECT '#178b', ST_XMax(ST_MakeBox2D(ST_Point(5, 5), ST_Point(0, 0)));
SELECT ST_NumPatches(ST_GeomFromEWKT('POLYHEDRALSURFACE( ((0 0 0, 0 0 1, 0 1 1, 0 1 0, 0 0 
0)),
((0 0 0, 0 1 0, 1 1 0, 1 0 0, 0 0 0)), ((0 0 0, 1 0 0, 1 0 1, 0 0 1, 0 0 0)),
((1 1 0, 1 1 1, 1 0 1, 1 0 0, 1 1 0)),
((0 1 0, 0 1 1, 1 1 1, 1 1 0, 0 1 0)), ((0 0 1, 1 0 1, 1 1 1, 0 1 1, 0 0 1)) )'));
SELECT ST_AsText(ST_Intersection('POINT(0 0)'::geometry, 'LINESTRING ( 2 0, 0 2 )':: 
geometry));

SELECT ST_AsText(ST_Intersection('POINT(0 0)'::geometry, 'LINESTRING ( 0 0, 0 2 )':: 
geometry));
SELECT ST_AsEWKT(geom) As the_geom, path
FROM ST_DumpRings(
ST_GeomFromEWKT('POLYGON((-8149064 5133092 1,-8149064 5132986 1,-8148996 5132839 
1,-8148972 5132767 1,-8148958 5132508 1,-8148941 5132466 1,-8148924 5132394 1,
-8148903 5132210 1,-8148930 5131967 1,-8148992 5131978 1,-8149237 5132093 1,-8149404 
5132211 1,-8149647 5132310 1,-8149757 5132394 1,
-8150305 5132788 1,-8149064 5133092 1),
(-8149362 5132394 1,-8149446 5132501 1,-8149548 5132597 1,-8149695 5132675 1,-8149362 
5132394 1))')
) as foo;

SELECT ST_IsClosed('LINESTRING(0 0, 1 1)'::geometry);
SELECT ST_IsClosed('LINESTRING(0 0, 0 1, 1 1, 0 0)'::geometry);
SELECT ST_AsText(ST_PointOnSurface('POINT(0 5)'::geometry));
SELECT ST_AsText(ST_PointOnSurface('LINESTRING(0 5, 0 10)'::geometry));
SELECT ST_AsText(ST_PointOnSurface('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'::geometry));
SELECT ST_AsEWKT(ST_PointOnSurface(ST_GeomFromEWKT('LINESTRING(0 5 1, 0 0 1, 0 10 2)')));
SELECT ST_GeomFromEWKT('SRID=4269;LINESTRING(-71.160281 42.258729,-71.160837 
42.259113,-71.161144 42.25932)');
SELECT ST_GeomFromEWKT('SRID=4269;MULTILINESTRING((-71.160281 42.258729,-71.160837 
42.259113,-71.161144 42.25932))');
SELECT ST_RelateMatch('101202FFF', 'TTTTTTFFF') ;

SELECT mat.name, pat.name, ST_RelateMatch(mat.val, pat.val) As satisfied
FROM
( VALUES ('Equality', 'T1FF1FFF1'),
('Overlaps', 'T*T***T**'),
('Within', 'T*F**F***'),
('Disjoint', 'FF*FF****') As pat(name,val)
CROSS JOIN
( VALUES ('Self intersections (invalid)', '111111111'),
('IE2_BI1_BB0_BE1_EI1_EE2', 'FF2101102'),
('IB1_IE1_BB0_BE0_EI2_EI1_EE2', 'F11F00212')
) As mat(name,val);

SELECT ST_AsEWKT(ST_FlipCoordinates(GeomFromEWKT('POINT(1 2)')));
SELECT ST_AsText('01030000000100000005000000000000000000
000000000000000000000000000000000000000000000000
F03F000000000000F03F000000000000F03F000000000000F03
F000000000000000000000000000000000000000000000000');
SELECT ST_NDims(ST_GeomFromText('POINT(1 1)')) As d2point,
ST_NDims(ST_GeomFromEWKT('POINT(1 1 2)')) As d3point,
ST_NDims(ST_GeomFromEWKT('POINTM(1 1 0.5)')) As d2pointm;
SELECT ST_Box2dFromGeoHash('9qqj7nmxncgyy4d0dbxqz0');
SELECT ST_Box2dFromGeoHash('9qqj7nmxncgyy4d0dbxqz0', 0);
SELECT ST_Box2dFromGeoHash('9qqj7nmxncgyy4d0dbxqz0', 10);
SELECT ST_AsText(
ST_ShortestLine('POINT(100 100) 
'::geometry,
'LINESTRING (20 80, 98 
190, 110 180, 50 75 )'::geometry)
) As sline;

SELECT ST_AsEWKT(ST_3DShortestLine(line,pt)) AS shl3d_line_pt,
ST_AsEWKT(ST_ShortestLine(line,pt)) As shl2d_line_pt
FROM (SELECT 'MULTIPOINT(100 100 30, 50 74 1000)'::geometry As pt,
'LINESTRING (20 80 20, 98 190 1, 110 180 3, 50 75 900)':: 
geometry As line
) As foo;

SELECT ST_Npoints(the_geom) As np_before, ST_NPoints(ST_SimplifyPreserveTopology(the_geom 
,0.1)) As np01_notbadcircle, ST_NPoints(ST_SimplifyPreserveTopology(the_geom,0.5)) As 
np05_notquitecircle,
ST_NPoints(ST_SimplifyPreserveTopology(the_geom,1)) As np1_octagon, ST_NPoints( 
ST_SimplifyPreserveTopology(the_geom,10)) As np10_square,
ST_NPoints(ST_SimplifyPreserveTopology(the_geom,100)) As np100_stillsquare
FROM (SELECT ST_Buffer('POINT(1 3)', 10,12) As the_geom) As foo;
SELECT ST_AsText(ST_Segmentize(
ST_GeomFromText('MULTILINESTRING((-29 -27,-30 -29.7,-36 -31,-45 -33),(-45 -33,-46 -32))')
,5)
);
SELECT ST_AsText(ST_Segmentize(ST_GeomFromText('POLYGON((-29 28, -30 40, -29 28))'),10));
SELECT ST_AsEWKT(ST_3DLongestLine(line,pt)) AS lol3d_line_pt,
ST_AsEWKT(ST_LongestLine(line,pt)) As lol2d_line_pt
FROM (SELECT 'POINT(100 100 30)'::geometry As pt,
'LINESTRING (20 80 20, 98 190 1, 110 180 3, 50 75 1000)':: 
geometry As line
) As foo;

SELECT ST_AsEWKT(ST_3DLongestLine(line,pt)) AS lol3d_line_pt,
ST_AsEWKT(ST_LongestLine(line,pt)) As lol2d_line_pt
FROM (SELECT 'MULTIPOINT(100 100 30, 50 74 1000)'::geometry As pt,
'LINESTRING (20 80 20, 98 190 1, 110 180 3, 50 75 900)':: 
geometry As line
) As foo;

SELECT ST_AsEWKT(ST_3DLongestLine(poly, mline)) As lol3d,
ST_AsEWKT(ST_LongestLine(poly, mline)) As lol2d
FROM (SELECT ST_GeomFromEWKT('POLYGON((175 150 5, 20 40 5, 35 45 5, 50 60 5, 
100 100 5, 175 150 5))') As poly,
ST_GeomFromEWKT('MULTILINESTRING((175 155 2, 20 40 20, 50 60 -2, 125 
100 1, 175 155 1),
(1 10 2, 5 20 1))') As mline ) As foo;
select PostGIS_GEOS_Version();
SELECT ST_AsText(the_geom) as line, ST_AsText(ST_Reverse(the_geom)) As reverseline
FROM
(SELECT ST_MakeLine(ST_MakePoint(1,2),
ST_MakePoint(1,10)) As the_geom) as foo;
SELECT tbl1.column1, tbl2.column1, tbl1.column2 @ tbl2.column2 AS contained
FROM
( VALUES
(1, 'LINESTRING (1 1, 3 3)'::geometry)) AS tbl1,
( VALUES
(2, 'LINESTRING (0 0, 4 4)'::geometry),
(3, 'LINESTRING (2 2, 4 4)'::geometry),
(4, 'LINESTRING (1 1, 3 3)'::geometry)) AS tbl2;
SELECT ST_AsText(
ST_SharedPaths(
ST_GeomFromText('MULTILINESTRING((26 125,26 200,126 200,126 125,26 125),
(51 150,101 150,76 175,51 150))'),
ST_GeomFromText('LINESTRING(151 100,126 156.25,126 125,90 161, 76 175)')
)
) As wkt;

SELECT ST_AsText(
ST_SharedPaths(
ST_GeomFromText('LINESTRING(76 175,90 161,126 125,126 156.25,151 100)'),
ST_GeomFromText('MULTILINESTRING((26 125,26 200,126 200,126 125,26 125),
(51 150,101 150,76 175,51 150))')
)
) As wkt;

select 'bd1', 'LINESTRING(0 0,0 10,10 10)'::geometry <#>
              'LINESTRING(6 2,6 8)'::geometry; 
select 'bd2', 'LINESTRING(0 0,0 10,10 10)'::geometry <#>
              'LINESTRING(11 0,19 10)'::geometry; SELECT edge_id, (dp).path[1] As index, ST_AsText((dp).geom) As wktnode
FROM (SELECT 1 As edge_id
, ST_DumpPoints(ST_GeomFromText('LINESTRING(1 2, 3 4, 10 10)')) AS dp
UNION ALL
SELECT 2 As edge_id
, ST_DumpPoints(ST_GeomFromText('LINESTRING(3 5, 5 6, 9 10)')) AS dp
) As foo;

SELECT path, ST_AsText(geom)
FROM (
SELECT (ST_DumpPoints(g.geom)).*
FROM
(SELECT
'GEOMETRYCOLLECTION(
POINT ( 0 1 ),
LINESTRING ( 0 3, 3 4 ),
POLYGON (( 2 0, 2 3, 0 2, 2 0 )),
POLYGON (( 3 0, 3 3, 6 3, 6 0, 3 0 ),
( 5 1, 4 2, 5 2, 5 1 )),
MULTIPOLYGON (
(( 0 5, 0 8, 4 8, 4 5, 0 5 ),
( 1 6, 3 6, 2 7, 1 6 )),
(( 5 4, 5 8, 6 7, 5 4 ))
)
)'::geometry AS geom
) AS g
) j;

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
SELECT gid, ST_IsValidReason(the_geom) as validity_info
FROM
(SELECT ST_MakePolygon(ST_ExteriorRing(e.buff), ST_Accum(f.line)) As the_geom, gid
FROM (SELECT ST_Buffer(ST_MakePoint(x1*10,y1), z1) As buff, x1*10 + y1*100 + z1*1000 As gid
FROM generate_series(-4,6) x1
CROSS JOIN generate_series(2,5) y1
CROSS JOIN generate_series(1,8) z1
WHERE x1 > y1*0.5 AND z1 < x1*y1) As e
INNER JOIN (SELECT ST_Translate(ST_ExteriorRing(ST_Buffer(ST_MakePoint(x1*10,y1), z1)),y1 
*1, z1*2) As line
FROM generate_series(-3,6) x1
CROSS JOIN generate_series(2,5) y1
CROSS JOIN generate_series(1,10) z1
WHERE x1 > y1*0.75 AND z1 < x1*y1) As f
ON (ST_Area(e.buff) > 78 AND ST_Contains(e.buff, f.line))
GROUP BY gid, e.buff) As quintuplet_experiment
WHERE ST_IsValid(the_geom) = false
ORDER BY gid
LIMIT 3;

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
