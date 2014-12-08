/**
 * This file is part of apache-spark-examples.
 *
 * apache-spark-examples is free software; you can redistribute it and/or modify 
 * it under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2, or (at your option) any later version.
 *
 * apache-spark-examples is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; see the file COPYING. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package es.devcircus.sparkwordcount

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
object ScalaWordCount {
  
  def main(args: Array[String]) {
    
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    
    val threshold = args(1).toInt
    
    // split each document into words
    val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))
    
    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
    
    // filter out words with less than threshold occurrences
    val filtered = wordCounts.filter(_._2 >= threshold)
    
    // count characters
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)
    
    // print results
    System.out.println(charCounts.collect().mkString(", "))

  }
}
