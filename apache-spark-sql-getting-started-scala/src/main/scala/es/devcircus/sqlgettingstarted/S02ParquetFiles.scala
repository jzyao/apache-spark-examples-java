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
object S02ParquetFiles {
  
  // Define the schema using a case class.
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface.
  case class Person(name: String, age: Int)
  
  /**
   * Método principal.
   *
   * @param args Argumentos que le pasamos al programa.
   */
  def main(args: Array[String]) {
    
    val sparkConf = new SparkConf().setAppName("Parquet Files")
    // sc is an existing SparkContext.
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    
    // sqlContext from the previous example is used in this example.
    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.createSchemaRDD

    // Create an RDD
    val people = sc.textFile("data/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))

    // The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, allowing it to be stored using Parquet.
    people.saveAsParquetFile("people.parquet")

    // Read in the parquet file created above.  Parquet files are self-describing so the schema is preserved.
    // The result of loading a Parquet file is also a SchemaRDD.
    val parquetFile = sqlContext.parquetFile("people.parquet")

    //Parquet files can also be registered as tables and then used in SQL statements.
    parquetFile.registerTempTable("parquetFile")
    val teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

  }
}
