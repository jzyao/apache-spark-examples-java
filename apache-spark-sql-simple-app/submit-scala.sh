#!/bin/bash

# Ejecucion local en 1 cores
#spark-submit --class es.devcircus.simplesqlapp.SSimpleSqlApp --master local target/spark-sql-simple-app-0.0.1-SNAPSHOT.jar

# Ejecucion local en 2 cores
#spark-submit --class es.devcircus.simplesqlapp.SSimpleSqlApp --master local[2] target/spark-sql-simple-app-0.0.1-SNAPSHOT.jar

# Ejecucion local en 4 cores
spark-submit --class es.devcircus.simplesqlapp.SSimpleSqlApp --master local[4] target/spark-sql-simple-app-0.0.1-SNAPSHOT.jar
