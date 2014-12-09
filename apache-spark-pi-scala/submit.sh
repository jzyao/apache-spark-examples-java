#!/bin/bash

/opt/spark/bin/spark-submit --class es.devcircus.sparkpi.ScalaPiCalculator --master local target/spark-pi-scala-0.0.1-SNAPSHOT.jar data/inputfile.txt 2