#!/bin/bash

/opt/spark/bin/spark-submit --class es.devcircus.sparkwordcount.JavaWordCount --master local target/spark-wordcount-java-0.0.1-SNAPSHOT.jar data/inputfile.txt 2