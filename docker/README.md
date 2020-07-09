# ARCHITECTURE AND OS VERSION

x86-64 CentOS7.6  
ARM64 openEuler 20.03 LTS

# BUILD IMAGE

```console
docker build -t opengauss:1.0 .
```

# START INSTANCE

```console
$ docker run --name opengauss --privileged=true -d -e GS_PASSWORD=secretpassword@123 opengauss:1.0
```

# CONNECT TO THE CONTAINER DATABASE FROM OS

```console
$ docker run −−name opengauss −−privileged=true −d −e GSPASSWORD=secretpassword@123 \
  −p8888:5432 opengauss:1.0 gsql -d postgres -U gaussdb -W'secretpassword@123' \
  -h your-host-ip -p8888
```

# PERSIST DATA

```console
$ docker run --name opengauss --privileged=true -d -e GS_PASSWORD=secretpassword@123 \
  -v /opengauss:/var/lib/opengauss opengauss:1.0
```

# TODO
primary standby install
