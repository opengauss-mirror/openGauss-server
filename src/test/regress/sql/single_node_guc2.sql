-- test unsupported guc value
show replconninfo1;
set replconninfo5='localhost=127.0.0.1 localport=10400 localheartbeatport=10402 localservice=10401 remotehost=127.0.0.1 remoteport=10412 remoteheartbeatport=10414 remoteservice=10413';
set replconninfo6='localhost=127.0.0.1 localport=10400 localheartbeatport=10402 localservice=10401 remotehost=127.0.0.1 remoteport=10412 remoteheartbeatport=10414 remoteservice=10413';
set replconninfo7='localhost=127.0.0.1 localport=10400 localheartbeatport=10402 localservice=10401 remotehost=127.0.0.1 remoteport=10412 remoteheartbeatport=10414 remoteservice=10413';
show replconninfo5;
show replconninfo6;
show replconninfo7;