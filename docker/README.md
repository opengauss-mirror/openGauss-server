# Architecture And Os Version

x86-64 CentOS7.6  
ARM64 openEuler 20.03 LTS

# Build Image

```console
docker build -t opengauss:1.0 .
```

# Start Instance

```console
$ docker run --name opengauss --privileged=true -d -e GS_PASSWORD=secretpassword@123 opengauss:1.0
```

# Connect To The Container Database From Os

```console
$ docker run −−name opengauss −−privileged=true −d −e GSPASSWORD=secretpassword@123 \
  −p8888:5432 opengauss:1.0 gsql -d postgres -U gaussdb -W'secretpassword@123' \
  -h your-host-ip -p8888
```

# Persist Data

```console
$ docker run --name opengauss --privileged=true -d -e GS_PASSWORD=secretpassword@123 \
  -v /opengauss:/var/lib/opengauss opengauss:1.0
```

# Todo
primary standby install
