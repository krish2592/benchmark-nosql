# Manual setup need to automate with docker

```
docker pull cassandra:latest
```

```
docker network create cassandra
```

```
docker run --rm -d --name cassandra --hostname cassandra --network cassandra cassandra
```
