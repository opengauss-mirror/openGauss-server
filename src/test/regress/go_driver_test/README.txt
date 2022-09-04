This directory is using for go driver test
such way:
make gocheck p=8000 -sj

Install db information:
datapath : current_dir/data
inituser: current os login user
inituser password: Gauss_234
default databse:  postgres
install port: default 5432

support parameter:
p： install db port  (default 5432)
u: go driver login user (default gauss)
w: go driver login user password (default Gauss_234)
h: go driver login IP address (default 127.0.0.1)
d: go driver login user database (default gauss)