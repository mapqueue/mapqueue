#! /bin/sh
# start bigtable-emulator
# docker run -d -p 9035:9035 shopify/bigtable-emulator
# start dynamodb-emulator
# docker run -d -p 8000:8000 amazon/dynamodb-local
# start mysql
# mysql.server start
# start postgres
# pg_ctl -D /usr/local/var/postgres start
# start redis
# redis-server /usr/local/etc/redis.conf
python py/test.py
rm mapqueue.db
rm data.mdb
rm lock.mdb