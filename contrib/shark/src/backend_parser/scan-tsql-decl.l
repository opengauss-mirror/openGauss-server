%option prefix="pgtsql_core_yy"

%x      xbr

xbrstart        \[
xbrstop         \]
xbrinside       [^\]]+

hex_cont [0-9A-Za-z]

%s		tsql
tsql_hex		0[xX]{hex_cont}*
