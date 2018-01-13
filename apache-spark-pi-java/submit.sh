#!/bin/bash

spark-submit --class es.devcircus.sparkpi.JavaPiCalculator --master local target/spark-pi-java-0.0.1-SNAPSHOT.jar data/inputfile.txt 2
