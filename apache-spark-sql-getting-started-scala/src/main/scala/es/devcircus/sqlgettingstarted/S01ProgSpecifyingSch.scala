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
object S01ProgSpecifyingSch {
  
  /**
   * Método principal.
   *
   * @param args Argumentos que le pasamos al programa.
   */
  def main(args: Array[String]) {
    
    val sparkConf = new SparkConf().setAppName("Programmatically Specifying the Schema")
    // sc is an existing SparkContext.
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    
    // Create an RDD
    val people = sc.textFile("data/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Import Spark SQL data types and Row.
    import org.apache.spark.sql._

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (people) to Rows.
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // Apply the schema to the RDD.
    val peopleSchemaRDD = sqlContext.applySchema(rowRDD, schema)

    // Register the SchemaRDD as a table.
    peopleSchemaRDD.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    // val results = sqlContext.sql("SELECT name FROM people")
    val results = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    results.map(t => "Name: " + t(0)).collect().foreach(println)
    
  }
}
