create schema hw_function_p_2;
set search_path to hw_function_p_2;

create function test_fun_7(
a0 varchar, b0 int, c0 int, d0 int, e0 int, f0 int, g0 int, h0 int, i0 int, j0 int,
a1 int, b1 int, c1 int, d1 int, e1 int, f1 int, g1 int, h1 int, i1 int, j1 int,
a2 int, b2 int, c2 int, d2 int, e2 int, f2 int, g2 int, h2 int, i2 int, j2 int,
a3 int, b3 int, c3 int, d3 int, e3 int, f3 int, g3 int, h3 int, i3 int, j3 int,
a4 int, b4 int, c4 int, d4 int, e4 int, f4 int, g4 int, h4 int, i4 int, j4 int,
a5 int, b5 int, c5 int, d5 int, e5 int, f5 int, g5 int, h5 int, i5 int, j5 int,
a6 int, b6 int, c6 int, d6 int, e6 int, f6 int, g6 int, h6 int, i6 int, j6 int,
a7 int, b7 int, c7 int, d7 int, e7 int, f7 int, g7 int, h7 int, i7 int, j7 int,
a8 int, b8 int, c8 int, d8 int, e8 int, f8 int, g8 int, h8 int, i8 int, j8 int,
a9 int, b9 int, c9 int, d9 int, e9 int, f9 int, g9 int, h9 int, i9 int, j9 int,
a0_1 int, b0_1 int, c0_1 int, d0_1 int, e0_1 int, f0_1 int, g0_1 int, h0_1 int, i0_1 int, j0_1 int,
a1_1 int, b1_1 int, c1_1 int, d1_1 int, e1_1 int, f1_1 int, g1_1 int, h1_1 int, i1_1 int, j1_1 int,
a2_1 int, b2_1 int, c2_1 int, d2_1 int, e2_1 int, f2_1 int, g2_1 int, h2_1 int, i2_1 int, j2_1 int,
a3_1 int, b3_1 int, c3_1 int, d3_1 int, e3_1 int, f3_1 int, g3_1 int, h3_1 int, i3_1 int, j3_1 int,
a4_1 int, b4_1 int, c4_1 int, d4_1 int, e4_1 int, f4_1 int, g4_1 int, h4_1 int, i4_1 int, j4_1 int,
a5_1 int, b5_1 int, c5_1 int, d5_1 int, e5_1 int, f5_1 int, g5_1 int, h5_1 int, i5_1 int, j5_1 int,
a6_1 int, b6_1 int, c6_1 int, d6_1 int, e6_1 int, f6_1 int, g6_1 int, h6_1 int, i6_1 int, j6_1 int,
a7_1 int, b7_1 int, c7_1 int, d7_1 int, e7_1 int, f7_1 int, g7_1 int, h7_1 int, i7_1 int, j7_1 int,
a8_1 int, b8_1 int, c8_1 int, d8_1 int, e8_1 int, f8_1 int, g8_1 int, h8_1 int, i8_1 int, j8_1 int,
a9_1 int, b9_1 int, c9_1 int, d9_1 int, e9_1 int, f9_1 int, g9_1 int, h9_1 int, i9_1 int, j9_1 int,
a0_2 int, b0_2 int, c0_2 int, d0_2 int, e0_2 int, f0_2 int, g0_2 int, h0_2 int, i0_2 int, j0_2 int,
a1_2 int, b1_2 int, c1_2 int, d1_2 int, e1_2 int, f1_2 int, g1_2 int, h1_2 int, i1_2 int, j1_2 int,
a2_2 int, b2_2 int, c2_2 int, d2_2 int, e2_2 int, f2_2 int, g2_2 int, h2_2 int, i2_2 int, j2_2 int,
a3_2 int, b3_2 int, c3_2 int, d3_2 int, e3_2 int, f3_2 int, g3_2 int, h3_2 int, i3_2 int, j3_2 int,
a4_2 int, b4_2 int, c4_2 int, d4_2 int, e4_2 int, f4_2 int, g4_2 int, h4_2 int, i4_2 int, j4_2 int,
a5_2 int, b5_2 int, c5_2 int, d5_2 int, e5_2 int, f5_2 int, g5_2 int, h5_2 int, i5_2 int, j5_2 int,
a6_2 int, b6_2 int, c6_2 int, d6_2 int, e6_2 int, f6_2 int, g6_2 int, h6_2 int, i6_2 int, j6_2 int,
a7_2 int, b7_2 int, c7_2 int, d7_2 int, e7_2 int, f7_2 int, g7_2 int, h7_2 int, i7_2 int, j7_2 int,
a8_2 int, b8_2 int, c8_2 int, d8_2 int, e8_2 int, f8_2 int, g8_2 int, h8_2 int, i8_2 int, j8_2 int,
a9_2 int, b9_2 int, c9_2 int, d9_2 int, e9_2 int, f9_2 int, g9_2 int, h9_2 int, i9_2 int, j9_2 int,
a0_3 int, b0_3 int, c0_3 int, d0_3 int, e0_3 int, f0_3 int, g0_3 int, h0_3 int, i0_3 int, j0_3 int,
a1_3 int, b1_3 int, c1_3 int, d1_3 int, e1_3 int, f1_3 int, g1_3 int, h1_3 int, i1_3 int, j1_3 int,
a2_3 int, b2_3 int, c2_3 int, d2_3 int, e2_3 int, f2_3 int, g2_3 int, h2_3 int, i2_3 int, j2_3 int,
a3_3 int, b3_3 int, c3_3 int, d3_3 int, e3_3 int, f3_3 int, g3_3 int, h3_3 int, i3_3 int, j3_3 int,
a4_3 int, b4_3 int, c4_3 int, d4_3 int, e4_3 int, f4_3 int, g4_3 int, h4_3 int, i4_3 int, j4_3 int,
a5_3 int, b5_3 int, c5_3 int, d5_3 int, e5_3 int, f5_3 int, g5_3 int, h5_3 int, i5_3 int, j5_3 int,
a6_3 int, b6_3 int, c6_3 int, d6_3 int, e6_3 int, f6_3 int, g6_3 int, h6_3 int, i6_3 int, j6_3 int,
a7_3 int, b7_3 int, c7_3 int, d7_3 int, e7_3 int, f7_3 int, g7_3 int, h7_3 int, i7_3 int, j7_3 int,
a8_3 int, b8_3 int, c8_3 int, d8_3 int, e8_3 int, f8_3 int, g8_3 int, h8_3 int, i8_3 int, j8_3 int,
a9_3 int, b9_3 int, c9_3 int, d9_3 int, e9_3 int, f9_3 int, g9_3 int, h9_3 int, i9_3 int, j9_3 int,
a0_4 int, b0_4 int, c0_4 int, d0_4 int, e0_4 int, f0_4 int, g0_4 int, h0_4 int, i0_4 int, j0_4 int,
a1_4 int, b1_4 int, c1_4 int, d1_4 int, e1_4 int, f1_4 int, g1_4 int, h1_4 int, i1_4 int, j1_4 int,
a2_4 int, b2_4 int, c2_4 int, d2_4 int, e2_4 int, f2_4 int, g2_4 int, h2_4 int, i2_4 int, j2_4 int,
a3_4 int, b3_4 int, c3_4 int, d3_4 int, e3_4 int, f3_4 int, g3_4 int, h3_4 int, i3_4 int, j3_4 int,
a4_4 int, b4_4 int, c4_4 int, d4_4 int, e4_4 int, f4_4 int, g4_4 int, h4_4 int, i4_4 int, j4_4 int,
a5_4 int, b5_4 int, c5_4 int, d5_4 int, e5_4 int, f5_4 int, g5_4 int, h5_4 int, i5_4 int, j5_4 int,
a6_4 int, b6_4 int, c6_4 int, d6_4 int, e6_4 int, f6_4 int, g6_4 int, h6_4 int, i6_4 int, j6_4 int,
a7_4 int, b7_4 int, c7_4 int, d7_4 int, e7_4 int, f7_4 int, g7_4 int, h7_4 int, i7_4 int, j7_4 int,
a8_4 int, b8_4 int, c8_4 int, d8_4 int, e8_4 int, f8_4 int, g8_4 int, h8_4 int, i8_4 int, j8_4 int,
a9_4 int, b9_4 int, c9_4 int, d9_4 int, e9_4 int, f9_4 int, g9_4 int, h9_4 int, i9_4 int, j9_4 int,
a0_5 int, b0_5 int, c0_5 int, d0_5 int, e0_5 int, f0_5 int, g0_5 int, h0_5 int, i0_5 int, j0_5 int,
a1_5 int, b1_5 int, c1_5 int, d1_5 int, e1_5 int, f1_5 int, g1_5 int, h1_5 int, i1_5 int, j1_5 int,
a2_5 int, b2_5 int, c2_5 int, d2_5 int, e2_5 int, f2_5 int, g2_5 int, h2_5 int, i2_5 int, j2_5 int,
a3_5 int, b3_5 int, c3_5 int, d3_5 int, e3_5 int, f3_5 int, g3_5 int, h3_5 int, i3_5 int, j3_5 int,
a4_5 int, b4_5 int, c4_5 int, d4_5 int, e4_5 int, f4_5 int, g4_5 int, h4_5 int, i4_5 int, j4_5 int,
a5_5 int, b5_5 int, c5_5 int, d5_5 int, e5_5 int, f5_5 int, g5_5 int, h5_5 int, i5_5 int, j5_5 int,
a6_5 int, b6_5 int, c6_5 int, d6_5 int, e6_5 int, f6_5 int, g6_5 int, h6_5 int, i6_5 int, j6_5 int,
a7_5 int, b7_5 int, c7_5 int, d7_5 int, e7_5 int, f7_5 int, g7_5 int, h7_5 int, i7_5 int, j7_5 int,
a8_5 int, b8_5 int, c8_5 int, d8_5 int, e8_5 int, f8_5 int, g8_5 int, h8_5 int, i8_5 int, j8_5 int,
a9_5 int, b9_5 int, c9_5 int, d9_5 int, e9_5 int, f9_5 int, g9_5 int, h9_5 int, i9_5 int, j9_5 int,
a0_6 int, b0_6 int, c0_6 int, d0_6 int, e0_6 int, f0_6 int, g0_6 int, h0_6 int, i0_6 int, j0_6 int,
a1_6 int, b1_6 int, c1_6 int, d1_6 int, e1_6 int, f1_6 int, g1_6 int, h1_6 int, i1_6 int, j1_6 int,
a2_6 int, b2_6 int, c2_6 int, d2_6 int, e2_6 int, f2_6 int, g2_6 int, h2_6 int, i2_6 int, j2_6 int,
a3_6 int, b3_6 int, c3_6 int, d3_6 int, e3_6 int, f3_6 int, g3_6 int, h3_6 int, i3_6 int, j3_6 int,
a4_6 int, b4_6 int, c4_6 int, d4_6 int, e4_6 int, f4_6 int, g4_6 int, h4_6 int, i4_6 int, j4_6 int,
a5_6 int, b5_6 int, c5_6 int, d5_6 int, e5_6 int, f5_6 int, g5_6 int, h5_6 int, i5_6 int, j5_6 int,
a0_7 int, b0_7 int, c0_7 int, d0_7 int) returns integer
as $$
begin
        raise info '%', a0;
	return 0;
end;
$$language plpgsql;
select test_fun_7('test para is 666',
1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,
2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,
3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,
3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,
4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,
4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,
5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,
5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,
6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,
6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,
7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
7,7,7,7,7,7,7,7,7,7,7,7,7,7);
--drop function test_fun_7;
create function test_fun_8(
a0 varchar, b0 int, c0 int, d0 int, e0 int, f0 int, g0 int, h0 int, i0 int, j0 int,
a1 int, b1 int, c1 int, d1 int, e1 int, f1 int, g1 int, h1 int, i1 int, j1 int,
a2 int, b2 int, c2 int, d2 int, e2 int, f2 int, g2 int, h2 int, i2 int, j2 int,
a3 int, b3 int, c3 int, d3 int, e3 int, f3 int, g3 int, h3 int, i3 int, j3 int,
a4 int, b4 int, c4 int, d4 int, e4 int, f4 int, g4 int, h4 int, i4 int, j4 int,
a5 int, b5 int, c5 int, d5 int, e5 int, f5 int, g5 int, h5 int, i5 int, j5 int,
a6 int, b6 int, c6 int, d6 int, e6 int, f6 int, g6 int, h6 int, i6 int, j6 int,
a7 int, b7 int, c7 int, d7 int, e7 int, f7 int, g7 int, h7 int, i7 int, j7 int,
a8 int, b8 int, c8 int, d8 int, e8 int, f8 int, g8 int, h8 int, i8 int, j8 int,
a9 int, b9 int, c9 int, d9 int, e9 int, f9 int, g9 int, h9 int, i9 int, j9 int,
a0_1 int, b0_1 int, c0_1 int, d0_1 int, e0_1 int, f0_1 int, g0_1 int, h0_1 int, i0_1 int, j0_1 int,
a1_1 int, b1_1 int, c1_1 int, d1_1 int, e1_1 int, f1_1 int, g1_1 int, h1_1 int, i1_1 int, j1_1 int,
a2_1 int, b2_1 int, c2_1 int, d2_1 int, e2_1 int, f2_1 int, g2_1 int, h2_1 int, i2_1 int, j2_1 int,
a3_1 int, b3_1 int, c3_1 int, d3_1 int, e3_1 int, f3_1 int, g3_1 int, h3_1 int, i3_1 int, j3_1 int,
a4_1 int, b4_1 int, c4_1 int, d4_1 int, e4_1 int, f4_1 int, g4_1 int, h4_1 int, i4_1 int, j4_1 int,
a5_1 int, b5_1 int, c5_1 int, d5_1 int, e5_1 int, f5_1 int, g5_1 int, h5_1 int, i5_1 int, j5_1 int,
a6_1 int, b6_1 int, c6_1 int, d6_1 int, e6_1 int, f6_1 int, g6_1 int, h6_1 int, i6_1 int, j6_1 int,
a7_1 int, b7_1 int, c7_1 int, d7_1 int, e7_1 int, f7_1 int, g7_1 int, h7_1 int, i7_1 int, j7_1 int,
a8_1 int, b8_1 int, c8_1 int, d8_1 int, e8_1 int, f8_1 int, g8_1 int, h8_1 int, i8_1 int, j8_1 int,
a9_1 int, b9_1 int, c9_1 int, d9_1 int, e9_1 int, f9_1 int, g9_1 int, h9_1 int, i9_1 int, j9_1 int,
a0_2 int, b0_2 int, c0_2 int, d0_2 int, e0_2 int, f0_2 int, g0_2 int, h0_2 int, i0_2 int, j0_2 int,
a1_2 int, b1_2 int, c1_2 int, d1_2 int, e1_2 int, f1_2 int, g1_2 int, h1_2 int, i1_2 int, j1_2 int,
a2_2 int, b2_2 int, c2_2 int, d2_2 int, e2_2 int, f2_2 int, g2_2 int, h2_2 int, i2_2 int, j2_2 int,
a3_2 int, b3_2 int, c3_2 int, d3_2 int, e3_2 int, f3_2 int, g3_2 int, h3_2 int, i3_2 int, j3_2 int,
a4_2 int, b4_2 int, c4_2 int, d4_2 int, e4_2 int, f4_2 int, g4_2 int, h4_2 int, i4_2 int, j4_2 int,
a5_2 int, b5_2 int, c5_2 int, d5_2 int, e5_2 int, f5_2 int, g5_2 int, h5_2 int, i5_2 int, j5_2 int,
a6_2 int, b6_2 int, c6_2 int, d6_2 int, e6_2 int, f6_2 int, g6_2 int, h6_2 int, i6_2 int, j6_2 int,
a7_2 int, b7_2 int, c7_2 int, d7_2 int, e7_2 int, f7_2 int, g7_2 int, h7_2 int, i7_2 int, j7_2 int,
a8_2 int, b8_2 int, c8_2 int, d8_2 int, e8_2 int, f8_2 int, g8_2 int, h8_2 int, i8_2 int, j8_2 int,
a9_2 int, b9_2 int, c9_2 int, d9_2 int, e9_2 int, f9_2 int, g9_2 int, h9_2 int, i9_2 int, j9_2 int,
a0_3 int, b0_3 int, c0_3 int, d0_3 int, e0_3 int, f0_3 int, g0_3 int, h0_3 int, i0_3 int, j0_3 int,
a1_3 int, b1_3 int, c1_3 int, d1_3 int, e1_3 int, f1_3 int, g1_3 int, h1_3 int, i1_3 int, j1_3 int,
a2_3 int, b2_3 int, c2_3 int, d2_3 int, e2_3 int, f2_3 int, g2_3 int, h2_3 int, i2_3 int, j2_3 int,
a3_3 int, b3_3 int, c3_3 int, d3_3 int, e3_3 int, f3_3 int, g3_3 int, h3_3 int, i3_3 int, j3_3 int,
a4_3 int, b4_3 int, c4_3 int, d4_3 int, e4_3 int, f4_3 int, g4_3 int, h4_3 int, i4_3 int, j4_3 int,
a5_3 int, b5_3 int, c5_3 int, d5_3 int, e5_3 int, f5_3 int, g5_3 int, h5_3 int, i5_3 int, j5_3 int,
a6_3 int, b6_3 int, c6_3 int, d6_3 int, e6_3 int, f6_3 int, g6_3 int, h6_3 int, i6_3 int, j6_3 int,
a7_3 int, b7_3 int, c7_3 int, d7_3 int, e7_3 int, f7_3 int, g7_3 int, h7_3 int, i7_3 int, j7_3 int,
a8_3 int, b8_3 int, c8_3 int, d8_3 int, e8_3 int, f8_3 int, g8_3 int, h8_3 int, i8_3 int, j8_3 int,
a9_3 int, b9_3 int, c9_3 int, d9_3 int, e9_3 int, f9_3 int, g9_3 int, h9_3 int, i9_3 int, j9_3 int,
a0_4 int, b0_4 int, c0_4 int, d0_4 int, e0_4 int, f0_4 int, g0_4 int, h0_4 int, i0_4 int, j0_4 int,
a1_4 int, b1_4 int, c1_4 int, d1_4 int, e1_4 int, f1_4 int, g1_4 int, h1_4 int, i1_4 int, j1_4 int,
a2_4 int, b2_4 int, c2_4 int, d2_4 int, e2_4 int, f2_4 int, g2_4 int, h2_4 int, i2_4 int, j2_4 int,
a3_4 int, b3_4 int, c3_4 int, d3_4 int, e3_4 int, f3_4 int, g3_4 int, h3_4 int, i3_4 int, j3_4 int,
a4_4 int, b4_4 int, c4_4 int, d4_4 int, e4_4 int, f4_4 int, g4_4 int, h4_4 int, i4_4 int, j4_4 int,
a5_4 int, b5_4 int, c5_4 int, d5_4 int, e5_4 int, f5_4 int, g5_4 int, h5_4 int, i5_4 int, j5_4 int,
a6_4 int, b6_4 int, c6_4 int, d6_4 int, e6_4 int, f6_4 int, g6_4 int, h6_4 int, i6_4 int, j6_4 int,
a7_4 int, b7_4 int, c7_4 int, d7_4 int, e7_4 int, f7_4 int, g7_4 int, h7_4 int, i7_4 int, j7_4 int,
a8_4 int, b8_4 int, c8_4 int, d8_4 int, e8_4 int, f8_4 int, g8_4 int, h8_4 int, i8_4 int, j8_4 int,
a9_4 int, b9_4 int, c9_4 int, d9_4 int, e9_4 int, f9_4 int, g9_4 int, h9_4 int, i9_4 int, j9_4 int,
a0_5 int, b0_5 int, c0_5 int, d0_5 int, e0_5 int, f0_5 int, g0_5 int, h0_5 int, i0_5 int, j0_5 int,
a1_5 int, b1_5 int, c1_5 int, d1_5 int, e1_5 int, f1_5 int, g1_5 int, h1_5 int, i1_5 int, j1_5 int,
a2_5 int, b2_5 int, c2_5 int, d2_5 int, e2_5 int, f2_5 int, g2_5 int, h2_5 int, i2_5 int, j2_5 int,
a3_5 int, b3_5 int, c3_5 int, d3_5 int, e3_5 int, f3_5 int, g3_5 int, h3_5 int, i3_5 int, j3_5 int,
a4_5 int, b4_5 int, c4_5 int, d4_5 int, e4_5 int, f4_5 int, g4_5 int, h4_5 int, i4_5 int, j4_5 int,
a5_5 int, b5_5 int, c5_5 int, d5_5 int, e5_5 int, f5_5 int, g5_5 int, h5_5 int, i5_5 int, j5_5 int,
a6_5 int, b6_5 int, c6_5 int, d6_5 int, e6_5 int, f6_5 int, g6_5 int, h6_5 int, i6_5 int, j6_5 int,
a7_5 int, b7_5 int, c7_5 int, d7_5 int, e7_5 int, f7_5 int, g7_5 int, h7_5 int, i7_5 int, j7_5 int,
a8_5 int, b8_5 int, c8_5 int, d8_5 int, e8_5 int, f8_5 int, g8_5 int, h8_5 int, i8_5 int, j8_5 int,
a9_5 int, b9_5 int, c9_5 int, d9_5 int, e9_5 int, f9_5 int, g9_5 int, h9_5 int, i9_5 int, j9_5 int,
a0_6 int, b0_6 int, c0_6 int, d0_6 int, e0_6 int, f0_6 int, g0_6 int, h0_6 int, i0_6 int, j0_6 int,
a1_6 int, b1_6 int, c1_6 int, d1_6 int, e1_6 int, f1_6 int, g1_6 int, h1_6 int, i1_6 int, j1_6 int,
a2_6 int, b2_6 int, c2_6 int, d2_6 int, e2_6 int, f2_6 int, g2_6 int, h2_6 int, i2_6 int, j2_6 int,
a3_6 int, b3_6 int, c3_6 int, d3_6 int, e3_6 int, f3_6 int, g3_6 int, h3_6 int, i3_6 int, j3_6 int,
a4_6 int, b4_6 int, c4_6 int, d4_6 int, e4_6 int, f4_6 int, g4_6 int, h4_6 int, i4_6 int, j4_6 int,
a5_6 int, b5_6 int, c5_6 int, d5_6 int, e5_6 int, f5_6 int, g5_6 int, h5_6 int, i5_6 int, j5_6 int,
a0_7 int, b0_7 int, c0_7 int, d0_7 int, e0_7 int, f0_7 int, g0_7 int) returns integer
as $$
begin
        raise info '%', a0;
	return 0;
end;
$$language plpgsql;

--test decode
select decode('00112233445566778899aabbccddeeff') from dual;
select DECODE(100,1,123,10,456,'default') from dual;
select decode('00112233445566778899aabbccddeeff', 'hex') from dual;
select DECODE(null,1,'123',10,'456','default') from dual;
select DECODE(1,1,'123',10,'456','default') from dual;
select DECODE(10,1,'123',10,'456','default') from dual;
select DECODE(100,1,'123',10,'456','default') from dual;
select DECODE(10,1,'123','10','456','default') from dual;

--test left and right function
SELECT right('我们是好人',2) AS RESULT;
SELECT left('甘肃中滩',1)AS RESULT;
SELECT left('甘肃中滩',-1)AS RESULT;
SELECT right('我们很好',5);
SELECT left('我们很好',5);
SELECT left('我们很好',-3);
SELECT right('我们很好',-3);
SELECT right('我们很好',0);
SELECT right('我们很好',4);
SELECT right('高斯 开 发部 三',3) AS RESULT;
SELECT right('过年happy啦',3) AS RESULT;
SELECT right('贴对year联哦',4) AS RESULT;
SELECT right('快乐%$生活',3) AS RESULT;
SELECT right('平安   健康',1) AS RESULT;
SELECT left(' 找找 找 啊',4) AS RESULT;
SELECT left('过年happy啦',3) AS RESULT;
SELECT left('快乐%$生活',3) AS RESULT;
SELECT right('删除   此处    的    空格  ',7);
SELECT left('删除   此处    的    空格  ',7);
SELECT left('快乐%$生活',-10) AS RESULT;
SELECT right('快乐%$生活',10) AS RESULT;
SELECT left('快乐%$生活',-1) AS RESULT;
SELECT right('快乐%$生活',1) AS RESULT;
SELECT left('快乐%$生活',-1) AS RESULT;
SELECT left('快乐%$生活',0) AS RESULT;
SELECT left('快乐%$生活',0.9) AS RESULT;
SELECT left('快乐%$生活',1.4) AS RESULT;
SELECT left('快乐%$生活',1.5) AS RESULT;
SELECT right('删除456 字符           忽然 !',10);
SELECT right('  感      恩      你 好 节  34546 * 你好 ！ ',100);


--bitand
\df bitand
create table position_bit(a bit(10),b bit(10));
insert into position_bit values(B'1000000001',B'0000000111');
insert into position_bit values(B'1000000100',B'0000000111');
select bitand(a,b) from position_bit order by a;
drop table position_bit;
create table position_bigint(a bigint,b bigint);
insert into position_bigint values(9223372036854775807,9223372036854775807);
insert into position_bigint values(-9223372036854775808,-9223372036854775808);
insert into position_bigint values(-9223372036854775808,9223372036854775807);
insert into position_bigint values(-9223372036854775808,1);
insert into position_bigint values(9223372036854775807,1);
insert into position_bigint values(4,1);
select bitand(a,b) from position_bigint order by a, 1;
drop table position_bigint;

create table agg_test_t1 (a integer);
insert into agg_test_t1 values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10); 
create table agg_test_t2 as select avg(a) from agg_test_t1;
select * from agg_test_t2;
drop table agg_test_t2;

create table agg_test_t2 as select avg(a) as col1, avg(a)/sum(a) as col2 , avg(a)/max(a) as col3, avg(a)/min(a) as col4 from agg_test_t1;
select * from agg_test_t2;
drop table agg_test_t2;

create table agg_test_t2 as select avg(a)/avg(a) as col1, avg(a) * avg(a) as col2, avg(a) + avg(a) as col3 , avg(a) - avg(a) as col4 from agg_test_t1;
select * from agg_test_t2;
drop table agg_test_t2;
/*
 * substrb
 */
--substrb(text, integer)
--边界测试
select substrb(null,1) from dual;
select substrb('高斯Gauss开发部',0) from dual;
select substrb('高斯Gauss开发部',1) from dual;
select rawtohex(substrb('高斯Gauss开发部',20)) from dual;
select rawtohex(substrb('高斯Gauss开发部',21)) from dual;
select rawtohex(substrb('高斯Gauss开发部',-1)) from dual;
select substrb('高斯Gauss开发部',-20) from dual;
select substrb('高斯Gauss开发部',-21) from dual;
--正常情况
select rawtohex(substrb('高斯Gauss开发部',2)) from dual;
select substrb('高斯Gauss开发部',7) from dual;
select rawtohex(substrb('高斯Gauss开发部',-2)) from dual;
select substrb('高斯Gauss开发部',-14) from dual;

--substrb(text,integer,integer)
--边界测试
select substrb(null,1,1) from dual;
select substrb('高斯Gauss开发部',0,3) from dual;
select substrb('高斯Gauss开发部',1,3) from dual;
select rawtohex(substrb('高斯Gauss开发部',20,3)) from dual;
select substrb('高斯Gauss开发部',21,3) from dual;
select rawtohex(substrb('高斯Gauss开发部',-1,3)) from dual;
select substrb('高斯Gauss开发部',-20,3) from dual;
select substrb('高斯Gauss开发部',-21,3) from dual;
select substrb('高斯Gauss开发部',1,-1) from dual;
select substrb('高斯Gauss开发部',1,20) from dual;
select substrb('高斯Gauss开发部',1,21) from dual;
--正常情况
select rawtohex(substrb('高斯Gauss开发部',1,1)) from dual;
select rawtohex(substrb('高斯Gauss开发部',2,2)) from dual;
select rawtohex(substrb('高斯Gauss开发部',2,5)) from dual;
select rawtohex(substrb('高斯Gauss开发部',2,10)) from dual;
select substrb('高斯Gauss开发部',7,5) from dual;
select rawtohex(substrb('高斯Gauss开发部',-1,1)) from dual;
select rawtohex(substrb('高斯Gauss开发部',-3,1)) from dual;
select rawtohex(substrb('高斯Gauss开发部',-3,2)) from dual;
select substrb('高斯Gauss开发部',-3,3) from dual;
select substrb('高斯Gauss开发部',-14,5) from dual;
select substrb('高斯Gauss开发部',-14,8) from dual;


select rawtohex(null) from dual;
select rawtohex('') from dual;
select rawtohex('1234567890abcdefABCDEF') from dual;
select rawtohex('hello,world,2012!') from dual;

select to_date('2009-8-1 19:01:01');
select to_date('2012-12-21');
select to_date('2012 - 12 - 16');
select to_date('2012:12: 16');
select to_date('2012:12:16 10:10:11');
select to_date('2012:12:16 10:10: 11');
select to_date('2012:12:16 10:10:11:123456');
select to_date('2012:12');
select to_date('2012: 12');
select to_date('2012:13:11');
select to_date('2012:12:35');
select to_date('2012:12:11 10:11:65');
select to_date('2012:12:11 13:61:20');
select to_date('2015 01   01','yyyy mm   dd');
select to_date('2015   01 01','yyyy   mm dd');
select to_timestamp('20150101  232323','yyyymmdd          hh24miss');
select to_timestamp('23  01  01','hh24  mi  ss');

SELECT PG_SIZE_PRETTY(-9223372036854775808);
SELECT PG_SIZE_PRETTY(-1);


CREATE TABLE toast_table_001(COL_TEXT TEXT) ;
INSERT INTO TOAST_TABLE_001 VALUES(LPAD('A',409600,'B'));
select pg_relation_size('TOAST_TABLE_001', 'main');
select pg_relation_size('TOAST_TABLE_001', 'MAIN');
select pg_relation_size((select oid from pg_class where upper(relname)='TOAST_TABLE_001'))=pg_relation_size((select reltoastrelid from pg_class where upper(relname)='TOAST_TABLE_001'));
select pg_column_size(col_text)>2048 from TOAST_TABLE_001;
select pg_table_size((select oid from pg_class where upper(relname)='TOAST_TABLE_001')) > 0;
select pg_table_size((select reltoastrelid from pg_class where upper(relname)='TOAST_TABLE_001')) > 0;
select pg_indexes_size((select oid from pg_class where upper(relname)='TOAST_TABLE_001'));
select pg_indexes_size((select reltoastrelid from pg_class where upper(relname)='TOAST_TABLE_001')) > 0;
select pg_total_relation_size((select oid from pg_class where upper(relname)='TOAST_TABLE_001')) > 0;
select pg_total_relation_size((select reltoastrelid from pg_class where upper(relname)='TOAST_TABLE_001')) > 0;
select pg_total_relation_size((select oid from pg_class where upper(relname)='TOAST_TABLE_001'))=pg_table_size((select oid from pg_class where upper(relname)='TOAST_TABLE_001'))+pg_indexes_size((select oid from pg_class where upper(relname)='TOAST_TABLE_001'));
select pg_total_relation_size((select reltoastrelid from pg_class where upper(relname)='TOAST_TABLE_001'))= (pg_table_size((select reltoastrelid from pg_class where upper(relname)='TOAST_TABLE_001'))+pg_indexes_size((select reltoastrelid from pg_class where upper(relname)='TOAST_TABLE_001')));

create table toast_tab(c1 text);
insert into toast_tab values (lpad('a', 4409600, 'b'));
select pg_relation_size((select oid from pg_class where relname='toast_tab'));
select pg_relation_size((select reltoastrelid from pg_class where relname = 'toast_tab'));
select pg_table_size((select oid from pg_class where upper(relname)='TOAST_TAB')) > 0;
select pg_table_size((select reltoastrelid from pg_class where upper(relname)='TOAST_TAB'));
select pg_indexes_size((select oid from pg_class where upper(relname)='TOAST_TAB'));
select pg_indexes_size((select reltoastrelid from pg_class where upper(relname)='TOAST_TAB')) > 0;
select pg_total_relation_size((select oid from pg_class where upper(relname)='TOAST_TAB')) > 0;
select pg_total_relation_size((select reltoastrelid from pg_class where upper(relname)='TOAST_TAB')) > 0;
select pg_total_relation_size((select oid from pg_class where upper(relname)='TOAST_TAB'))=pg_table_size((select oid from pg_class where upper(relname)='TOAST_TAB'))+pg_indexes_size((select oid from pg_class where upper(relname)='TOAST_TAB'));
select pg_total_relation_size((select reltoastrelid from pg_class where upper(relname)='TOAST_TAB'))= (pg_table_size((select reltoastrelid from pg_class where upper(relname)='TOAST_TAB'))+pg_indexes_size((select reltoastrelid from pg_class where upper(relname)='TOAST_TAB')));


--added for llt
--test to_char
select to_char(-1234,'abcd');
select to_char(-1234,'abcd');
select to_char(3, 'EEEE');

--test get_utility_query_def 
drop table test;
create table test(a int) compress;
create table test_a as select * from test;
create table test_b compress as select * from test;
drop table test_a;
drop table test_b;
--test texttoraw
 select hextoraw('a');
 --byteaSetBit
 SELECT set_bit('0101011000100100'::bytea,15, 1);
--byteale
select byteale('01','10');
--rawcat
select rawcat('abcd','ab');
--setseed
select setseed(-2);
select setseed(0.6) is NULL;
--int2_bool
select int2_bool(0);
select int2_bool(1);
--DCH_check
 select to_timestamp('13' ,'HH');
 select to_timestamp('25','HH24');
 select to_timestamp('62','MI');
 select to_timestamp('62','SS');
 select to_timestamp('86400','SSSSS');
 select to_timestamp('8','D');
 select to_timestamp('32','DD');
 select to_timestamp('367','DDD');
 select to_timestamp('36','MM');
 select to_timestamp('1000','MS');
 select to_timestamp('54','WW');
 select to_timestamp('7','W');
 select to_timestamp('5373485','J');
 select to_timestamp('-1','US'); 
 select to_timestamp('10000','SYYYY');
 select to_timestamp('10000','RR');
 
--test to_clob
SELECT to_clob(null);
SELECT to_clob(CAST( 'ABCDEF' AS RAW(10))) AS raw2clob;
SELECT to_clob(CAST( 'hello111' AS CHAR(15))) AS char2clob;
SELECT to_clob(CAST( repeat('Pg1', 5) AS CHARACTER(20))) AS character_clob;
SELECT to_clob(CAST( repeat('Pg2', 100) AS VARCHAR(300))) AS varchar_clob;
SELECT to_clob(CAST( repeat('Pg3', 100) AS VARCHAR2(300))) AS varchar2_clob;
SELECT to_clob(CAST( repeat('Pg4', 100) AS CHARACTER VARYING(300))) AS character_varying_clob;
SELECT to_clob(CAST( repeat('Pg5', 100) AS NCHAR(300))) AS nchar_clob;
SELECT to_clob(CAST( repeat('Pg6', 100) AS NVARCHAR2(300))) AS nvarchar2_clob;
SELECT to_clob(CAST( repeat('Pg7', 100) AS CLOB)) AS clob_clob;
SELECT to_clob(repeat('Pg', 100)) AS text_clob;
 
 --test llt
select regprocedurein('"test"');

create or replace procedure proc_test_llt(a int)
as
begin
end;
/
select regprocedurein('proc_test_llt(2');
select regprocedurein('proc_test_llt(int,)');
select regprocedurein('proc_test_llt("int",)');
select regprocedurein('proc_test_llt([1,2])');
select regprocedurein('proc_test_llt("int)');
select regoperatorin('proc_test_llt(none)');
select regprocedurein('proc_test_llt(int   )');


--test ts_query

CREATE TEXT SEARCH CONFIGURATION spell_tst_llt(
						COPY=english);
select to_tsquery ('spell_tst_llt',':abcd');
select to_tsquery ('spell_tst_llt',' ');

-- test current_timestamp
select current_timestamp=current_timestamp;

set datestyle = 'iso, ymd';

select to_date('2,01a-12-31 13:23:44.000000', 'Y,YYY-MM-DD HH24:MI:SS.FF');
select to_date('2004 MAYa 07 13:23:44','YYYY MONTH DD HH24:MI:SS.FF');
select to_date('2004 Maya 07 13:23:44','YYYY Month DD HH24:MI:SS.FF');
select to_date('2004 maya 07 13:23:44', 'YYYY month DD HH24:MI:SS.FF');
select to_date('2004 MAYa 10 13:23:44', 'YYYY MON DD HH24:MI:SS.FF');
select to_date('2004 05a 07 13:23:44', 'YYYY MM DD HH24:MI:SS.FF');

select to_date('2,001-12-31 13:23:44.000000', 'Y,YYY-MM-DD HH24:MI:SS.FF');
select to_date('2004 MAY 07 13:23:44','YYYY MONTH DD HH24:MI:SS.FF');
select to_date('2004 May 07 13:23:44','YYYY Month DD HH24:MI:SS.FF');
select to_date('2004 may 07 13:23:44', 'YYYY month DD HH24:MI:SS.FF');
select to_date('2004 MAY 10 13:23:44', 'YYYY MON DD HH24:MI:SS.FF');
select to_date('2004 05 07 13:23:44', 'YYYY MM DD HH24:MI:SS.FF');
reset datestyle;

select cast(date_part('month' ,interval '2 years 3 months') as varchar(20)) ;

create table implicit_convert_t1(a smallint,  b float4, c float8);
insert into implicit_convert_t1 values(2, 2, 2);
select cast(a as varchar), cast(b as varchar), cast(c as varchar) from implicit_convert_t1;
--drop table implicit_convert_t1;

--text pgxc pg_table_size, pg_relation_size, pg_total_relation_size
create table test_tbl_size (a int);
create index test_tbl_size_index on test_tbl_size (a);
--table size
select pg_table_size('test_tbl_size');
--index size
select pg_relation_size('test_tbl_size_index');
--total size
select pg_total_relation_size('test_tbl_size');

insert into test_tbl_size values (generate_series(1, 100));
--table size
select pg_table_size('test_tbl_size');
--index size
select pg_relation_size('test_tbl_size_index');
--total size
select pg_total_relation_size('test_tbl_size');
select  pg_total_relation_size('test_tbl_size') = pg_table_size('test_tbl_size') + pg_relation_size('test_tbl_size_index');
drop table test_tbl_size;

--test substring
select substring('abcd',1,-1);
select substring('abcd',-2,1) is null;
select substring('abcd',1,0) is null;

--pgxc pg_table_size, pg_relation_size, pg_indexes_size, pg_total_relation_size for row/column-store table
create table test_cstore_tbl_size(a int) with(orientation = column);
create table test_tbl_size_2(a int);
create index  test_cstore_tbl_size_index on test_cstore_tbl_size(a);
create index test_tbl_size_index_2 on test_tbl_size_2(a);

--cstore table size 
select pg_table_size('test_cstore_tbl_size')>=0;
select pg_table_size('test_tbl_size_2')>=0;

--cstore relation size
select pg_relation_size('test_cstore_tbl_size')>=0;
select pg_relation_size('test_tbl_size_2')>=0;

--cstore index size
select pg_table_size('test_cstore_tbl_size_index')>=0;
select pg_table_size('test_tbl_size_index_2')>=0;

--csotre index relation size
select pg_relation_size('test_cstore_tbl_size_index')>=0;
select pg_relation_size('test_tbl_size_index_2')>=0;

--cstore indexes size for table
select pg_indexes_size('test_cstore_tbl_size')>=0;
select pg_indexes_size('test_tbl_size_2')>=0;

--total size
select pg_total_relation_size('test_cstore_tbl_size') = pg_table_size('test_cstore_tbl_size') + pg_indexes_size('test_cstore_tbl_size');
select pg_total_relation_size('test_tbl_size_2') = pg_table_size('test_tbl_size_2') + pg_indexes_size('test_tbl_size_2');


select pg_database_size('postgres')>=0;

drop schema hw_function_p_2 cascade;
