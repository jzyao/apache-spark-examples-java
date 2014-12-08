#!/bin/bash

/opt/spark/bin/spark-submit --class es.devcircus.sparkwordcount.ScalaWordCount --master local target/spark-wordcount-scala-0.0.1-SNAPSHOT.jar data/inputfile.txt 2