# Project Setup


### Pre-requisite

1) Maven should be install in the system
2) Docker should be install in the system


1. Run maven clean install to install the packages

```
 mvn clean install
```

```
mvn compile
```

### Initial setup

2.  Run this script to download the data for shoping cart

```
cd benchmark-nosql/src/scripts/cassandra/ShopingCart/
```

```
./shopingcart-setup-initial-data.sh
```


3. Change to the directory and run the script.

```
cd benchmark-nosql/src/scripts/cassandra/ShopingCart/
```

```
./initialize-cassandra-db.sh 
```

4. 

```
cd benchmark-nosql/src/scripts/cassandra/ShopingCart/db-scripts
```

```
./copy-shop_db-file-docker.sh
```

```
./create-shop_db-schema.sh
```

```
./load-shop_db-data.sh
```



