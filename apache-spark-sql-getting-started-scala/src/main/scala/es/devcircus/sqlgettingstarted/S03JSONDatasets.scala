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
package es.devcircus.sqlgettingstarted

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
 * Example based on the Scala/Java/Python code from
 * https://spark.apache.org/docs/latest/sql-programming-guide.html
 * 
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
object S03JSONDatasets {
  
  /**
   * Método principal.
   *
   * @param args Argumentos que le pasamos al programa.
   */
  def main(args: Array[String]) {
    
    val sparkConf = new SparkConf().setAppName("JSON Datasets")
    // sc is an existing SparkContext.
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    
    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files.
    val path = "data/people.json"
    // Create a SchemaRDD from the file(s) pointed to by path
    val people = sqlContext.jsonFile(path)

    // The inferred schema can be visualized using the printSchema() method.
    people.printSchema()
    // root
    //  |-- age: integer (nullable = true)
    //  |-- name: string (nullable = true)

    // Register this SchemaRDD as a table.
    people.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    // Alternatively, a SchemaRDD can be created for a JSON dataset represented by
    // an RDD[String] storing one JSON object per string.
    val anotherPeopleRDD = sc.parallelize(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)
    
  }
}
