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
package es.devcircus.simplesqlapp

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * Example based on the Scala/Python code from
 * https://databricks-training.s3.amazonaws.com/data-exploration-using-spark-sql.html
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
object SSimpleSqlApp {
  
  /**
   * Método principal.
   *
   * @param args Argumentos que le pasamos al programa.
   */
  def main(args: Array[String]) {
    
    /**
     * Once you have launched the Spark shell, the next step is to create a
     * SQLContext. A SQLConext wraps the SparkContext, which you used in the
     * previous lesson, and adds functions for working with structured data.
     */
    // Seteamos el nombre del programa. Este nombre se usara en el cluster
    // para su ejecución.
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    /**
     * Now we can load a set of data in that is stored in the Parquet
     * format. Parquet is a self-describing columnar format. Since it is
     * self-describing, Spark SQL will automatically be able to infer all of
     * the column names and their datatypes. For this exercise we have
     * provided a set of data that contains all of the pages on wikipedia
     * that contain the word “berkeley”. You can load this data using the
     * parquetFile method provided by the SQLContext.
     */
    val wikiData = sqlContext.parquetFile("data/wiki_parquet")
    
    /**
     * The result of loading in a parquet file is a SchemaRDD. A SchemaRDD
     * has all of the functions of a normal RDD. For example, lets figure
     * out how many records are in the data set.
     */
    wikiData.count()
    
    /**
     * In addition to standard RDD operatrions, SchemaRDDs also have extra
     * information about the names and types of the columns in the dataset.
     * This extra schema information makes it possible to run SQL queries
     * against the data after you have registered it as a table. Below is an
     * example of counting the number of records using a SQL query. Elmétodo
     * registerAsTable se ha deprecado y se ha substituido por el método
     * registerTempTable.
     * http://mail-archives.apache.org/mod_mbox/spark-commits/201408.mbox/%3C540f0c8a261b4d1c88241e854f367258@git.apache.org%3E
     */
    // wikiData.registerAsTable("wikiData")
    wikiData.registerTempTable("wikiData")
    val countResult = sqlContext.sql("SELECT COUNT(*) FROM wikiData").collect()
    
    /**
     * The result of SQL queries is always a collection of Row objects. From
     * a row object you can access the individual columns of the result.
     */
    val sqlCount = countResult.head.getLong(0)
    
    /**
     * SQL can be a powerfull tool from performing complex aggregations. For
     * example, the following query returns the top 10 usersnames by the
     * number of pages they created.
     */
    sqlContext.sql("SELECT username, COUNT(*) AS cnt FROM wikiData WHERE username <> '' GROUP BY username ORDER BY cnt DESC LIMIT 10").collect().foreach(println)
  }
}
