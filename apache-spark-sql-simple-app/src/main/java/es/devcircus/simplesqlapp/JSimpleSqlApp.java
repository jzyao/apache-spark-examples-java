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
package es.devcircus.simplesqlapp;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Example based on the Scala/Python code from
 * https://databricks-training.s3.amazonaws.com/data-exploration-using-spark-sql.html
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public class JSimpleSqlApp {

    private static final String BASE_DATA_PATH = "spark-examples/apache-spark-sql-simple-app/data/";
    private static final String PARQUET_FILE_NAME = "wiki_parquet";
    private static final String FULL_PARQUET_FILE_PATH = BASE_DATA_PATH + PARQUET_FILE_NAME;

    // Queries
    private static final String QUERY_COUNT = "SELECT COUNT(*) FROM wikiData";
    private static final String QUERY_COUNT_BY_USERNAME = "SELECT username, COUNT(*) AS cnt FROM wikiData WHERE username <> '' GROUP BY username ORDER BY cnt DESC LIMIT 10";

    /**
     * Método principal.
     *
     * @param args Argumentos que le pasamos al programa.
     */
    public static void main(String[] args) {

        // Arrancamos el contexto de ejecucion de Apache Spark
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSqlTestProject")
                .getOrCreate();

        // Read in the Parquet file created above.
        // Parquet files are self-describing so the schema is preserved
        // The result of loading a parquet file is also a DataFrame
        Dataset<Row> parquetFileDF = spark.read().parquet(FULL_PARQUET_FILE_PATH);

        /**
         * The result of loading in a parquet file is a SchemaRDD. A SchemaRDD
         * has all of the functions of a normal RDD. For example, lets figure
         * out how many records are in the data set.
         */
        Long countResult = parquetFileDF.count();
        // Mostramos el resultado por pantalla.
        System.out.println("Resultado del conteo del RDD...: " + countResult);
        // Resultado del conteo del RDD...: 39365

        // Parquet files can also be used to create a temporary view and then used in SQL statements
        parquetFileDF.createOrReplaceTempView("wikiData");
        Dataset<Row> rows = spark.sql(QUERY_COUNT);
        
        Dataset<Long> countValue = rows.map(
                (MapFunction<Row, Long>) row -> row.getLong(0),
                Encoders.LONG());
        countValue.show();
        // +-----+
        // |value|
        // +-----+
        // |39365|
        // +-----+

        /**
         * SQL can be a powerfull tool from performing complex aggregations. For
         * example, the following query returns the top 10 usersnames by the
         * number of pages they created.
         */
        rows = spark.sql(QUERY_COUNT_BY_USERNAME);

        Dataset<String> namesDS = rows.map(
                (MapFunction<Row, String>) row -> row.toString(),
                Encoders.STRING());
        namesDS.show();        
        // +--------------------+
        // |               value|
        // +--------------------+
        // |    [Waacstats,2003]|
        // |       [Cydebot,949]|
        // |      [BattyBot,939]|
        // |         [Yobot,890]|
        // |        [Addbot,853]|
        // |       [Monkbot,668]|
        // |[ChrisGualtieri,438]|
        // |   [RjwilmsiBot,387]|
        // |    [OccultZone,377]|
        // |    [ClueBot NG,353]|
        // +--------------------+

        // Paramos el contexto.
        spark.stop();
    }
}
