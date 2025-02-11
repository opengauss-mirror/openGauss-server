%option prefix="pgtsql_core_yy"

%s		tsql
%x      xbr

xbrstart        \[
xbrstop         \]
xbrinside       [^\]]+
