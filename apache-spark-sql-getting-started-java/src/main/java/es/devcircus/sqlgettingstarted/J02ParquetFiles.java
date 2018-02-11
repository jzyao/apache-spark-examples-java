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
package es.devcircus.sqlgettingstarted;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Example based on the Scala/Java/Python code from
 * https://spark.apache.org/docs/latest/sql-programming-guide.html
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public class J02ParquetFiles {

    private static final String BASE_DATA_PATH = "spark-examples/apache-spark-sql-getting-started-java/data/";
    private static final String PARQUET_FILE_NAME = "people.parquet";
    private static final String FULL_PARQUET_FILE_PATH = BASE_DATA_PATH + PARQUET_FILE_NAME;
    
    // Queries
    private static final String PEOPLE_QUERY = "SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19";

    /**
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // Arrancamos el contexto de ejecucion de Apache Spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Parquet Files")
                .getOrCreate();

        // Check if the file exists
        FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        if (!fs.exists(new Path(FULL_PARQUET_FILE_PATH))) {
            // $example on:basic_parquet_example$
            Dataset<Row> peopleDF = spark.read().json(args[0]);

            // DataFrames can be saved as Parquet files, maintaining the schema information
            peopleDF.write().parquet(FULL_PARQUET_FILE_PATH);
        }

        // Read in the Parquet file created above.
        // Parquet files are self-describing so the schema is preserved
        // The result of loading a parquet file is also a DataFrame
        Dataset<Row> parquetFileDF = spark.read().parquet(FULL_PARQUET_FILE_PATH);

        // Parquet files can also be used to create a temporary view and then used in SQL statements
        parquetFileDF.createOrReplaceTempView("parquetFile");
        Dataset<Row> namesDF = spark.sql(PEOPLE_QUERY);
        Dataset<String> namesDS = namesDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING());
        namesDS.show();
        // +------------+
        // |       value|
        // +------------+
        // |Name: Justin|
        // +------------+        

        // Paramos el contexto.
        spark.stop();
    }
}
