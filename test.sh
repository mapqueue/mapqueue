#! /bin/sh
# start bigtable-emulator
# docker run -d -p 9035:9035 shopify/bigtable-emulator
# start dynamodb-emulator
# docker run -p 8000:8000 amazon/dynamodb-local
python3 py/test.py
rm mapqueue.db
rm data.mdb
rm lock.mdb