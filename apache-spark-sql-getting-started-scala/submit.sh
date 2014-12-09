#!/bin/bash

/opt/spark/bin/spark-submit --class es.devcircus.sqlgettingstarted.S00InferringSchReflection --master local target/spark-sql-getting-started-scala-0.0.1-SNAPSHOT.jar
/opt/spark/bin/spark-submit --class es.devcircus.sqlgettingstarted.S01ProgSpecifyingSch --master local target/spark-sql-getting-started-scala-0.0.1-SNAPSHOT.jar
/opt/spark/bin/spark-submit --class es.devcircus.sqlgettingstarted.S02ParquetFiles --master local target/spark-sql-getting-started-scala-0.0.1-SNAPSHOT.jar
/opt/spark/bin/spark-submit --class es.devcircus.sqlgettingstarted.S03JSONDatasets --master local target/spark-sql-getting-started-scala-0.0.1-SNAPSHOT.jar