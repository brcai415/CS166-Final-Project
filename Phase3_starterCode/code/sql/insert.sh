#!/bin/bash
psql -h localhost -p $PGPORT $USER"_DB" < create.sql
sleep 5
psql -h localhost -p $PGPORT $USER"_DB" < create_indexes.sql
