#!/bin/bash

# Ejecucion local en 1 cores
#/opt/spark/bin/spark-submit --class es.devcircus.simplesqlapp.JSimpleSqlApp --master local target/spark-simple-sql-app-0.0.1-SNAPSHOT.jar

# Ejecucion local en 2 cores
#/opt/spark/bin/spark-submit --class es.devcircus.simplesqlapp.JSimpleSqlApp --master local[2] target/spark-simple-sql-app-0.0.1-SNAPSHOT.jar

# Ejecucion local en 4 cores
/opt/spark/bin/spark-submit --class es.devcircus.simplesqlapp.JSimpleSqlApp --master local[4] target/spark-simple-sql-app-0.0.1-SNAPSHOT.jar