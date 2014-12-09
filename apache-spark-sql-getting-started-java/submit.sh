#!/bin/bash

/opt/spark/bin/spark-submit --class es.devcircus.sqlgettingstarted.J00InferringSchReflection --master local target/spark-sql-getting-started-java-0.0.1-SNAPSHOT.jar
/opt/spark/bin/spark-submit --class es.devcircus.sqlgettingstarted.J01ProgSpecifyingSch --master local target/spark-sql-getting-started-java-0.0.1-SNAPSHOT.jar
/opt/spark/bin/spark-submit --class es.devcircus.sqlgettingstarted.J02ParquetFiles --master local target/spark-sql-getting-started-java-0.0.1-SNAPSHOT.jar
/opt/spark/bin/spark-submit --class es.devcircus.sqlgettingstarted.J03JSONDatasets --master local target/spark-sql-getting-started-java-0.0.1-SNAPSHOT.jar