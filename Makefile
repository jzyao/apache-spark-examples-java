#
# Makefile to manage the program.
# Author: Adrian Novegil <adrian.novegil@gmail.com>
#
.DEFAULT_GOAL:=help

# Global variables
SPARK_SUBMIT=spark-submit
SPARK_PARAMS=--master local

# HDFS variables
HADOOP=hadoop
HDFS=fs
EXPERIMETNS_FOLDER=spark-examples
BASE_EXPERIMETNS_PATH=/user/$(USER)/$(EXPERIMETNS_FOLDER)
	
# Show some help
help:
	@echo ''
	@echo '  Usage:'
	@echo '    make <target>'
	@echo ' '
	@echo '  Targets:'
	@echo '    build                            Build the project'
	@echo '    load-hdfs-data                   Copy data from local filesystem to the HDFS cluster'
	@echo '    clear-hdfs-data                  Remove the experiments data folder in the HDFS cluster'
	@echo '    clear-and-load-hdfs-data         clear-hdfs-data and load-hdfs-data'
	@echo ' '
	@echo '  Experiments:'
	@echo '    wordcount                        Word count example in Java'	
	@echo '    pi                               Calculate pi example in Java'		
	@echo '    sql-inferring-sch-reflection     Basic SQL example in Java'	
	@echo '    sql-prog-specifying-sch          Basic SQL example in Java'	
	@echo '    sql-parquet-files                Basic SQL example in Java'	
	@echo '    sql-json-datasets                Basic SQL example in Java'		
	@echo '    sql-simple-app                   Simple app with SQL'	
	@echo ''
	
# General targets
	
build:
	@mvn clean install

load-hdfs-data:
	# apache-spark-wordcount-java
	$(HADOOP) $(HDFS) -mkdir -p $(BASE_EXPERIMETNS_PATH)/apache-spark-wordcount-java
	$(HADOOP) $(HDFS) -copyFromLocal -f ./apache-spark-wordcount-java/data  $(BASE_EXPERIMETNS_PATH)/apache-spark-wordcount-java
	# apache-spark-sql-getting-started-java
	$(HADOOP) $(HDFS) -mkdir -p $(BASE_EXPERIMETNS_PATH)/apache-spark-sql-getting-started-java
	$(HADOOP) $(HDFS) -copyFromLocal -f ./apache-spark-sql-getting-started-java/data  $(BASE_EXPERIMETNS_PATH)/apache-spark-sql-getting-started-java
	# apache-spark-sql-simple-app
	$(HADOOP) $(HDFS) -mkdir -p $(BASE_EXPERIMETNS_PATH)/apache-spark-sql-simple-app
	$(HADOOP) $(HDFS) -copyFromLocal -f ./apache-spark-sql-simple-app/data  $(BASE_EXPERIMETNS_PATH)/apache-spark-sql-simple-app
	
clear-hdfs-data:		
	$(HADOOP) $(HDFS) -rm -r $(EXPERIMETNS_FOLDER)	
	
clear-and-load-hdfs-data: clear-hdfs-data load-hdfs-data
	
# Experiments

wordcount:
	$(SPARK_SUBMIT) $(SPARK_PARAMS) --class es.devcircus.sparkwordcount.JavaWordCount apache-spark-wordcount-java/target/spark-wordcount-java.jar spark-examples/apache-spark-wordcount-java/data/inputfile.txt 2

pi:
	$(SPARK_SUBMIT) $(SPARK_PARAMS) --class es.devcircus.sparkpi.JavaPiCalculator apache-spark-pi-java/target/spark-pi-java.jar 10

sql-inferring-sch-reflection:
	$(SPARK_SUBMIT) $(SPARK_PARAMS) --class es.devcircus.sqlgettingstarted.J00InferringSchReflection apache-spark-sql-getting-started-java/target/spark-sql-getting-started-java.jar spark-examples/apache-spark-sql-getting-started-java/data/people.txt
	
sql-prog-specifying-sch:
	$(SPARK_SUBMIT) $(SPARK_PARAMS) --class es.devcircus.sqlgettingstarted.J01ProgSpecifyingSch apache-spark-sql-getting-started-java/target/spark-sql-getting-started-java.jar spark-examples/apache-spark-sql-getting-started-java/data/people.txt

sql-parquet-files:
	$(SPARK_SUBMIT) $(SPARK_PARAMS) --class es.devcircus.sqlgettingstarted.J02ParquetFiles apache-spark-sql-getting-started-java/target/spark-sql-getting-started-java.jar spark-examples/apache-spark-sql-getting-started-java/data/people.json

sql-json-datasets:
	$(SPARK_SUBMIT) $(SPARK_PARAMS) --class es.devcircus.sqlgettingstarted.J03JSONDatasets apache-spark-sql-getting-started-java/target/spark-sql-getting-started-java.jar spark-examples/apache-spark-sql-getting-started-java/data/people.json

sql-simple-app:
	$(SPARK_SUBMIT) $(SPARK_PARAMS) --class es.devcircus.simplesqlapp.JSimpleSqlApp apache-spark-sql-simple-app/target/spark-sql-simple-app-java.jar
