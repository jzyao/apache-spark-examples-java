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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * Example based on the Scala/Java/Python code from
 * https://spark.apache.org/docs/latest/sql-programming-guide.html
 * 
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
object S00GettingStarted {
  
  /**
   * Método principal.
   *
   * @param args Argumentos que le pasamos al programa.
   */
  def main(args: Array[String]) {
    
    val sc = new SparkContext(new SparkConf().setAppName("Getting Started"))
    
  }
}
