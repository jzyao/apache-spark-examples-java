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

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

/**
 * Example based on the Scala/Java/Python code from
 * https://spark.apache.org/docs/latest/sql-programming-guide.html
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public class J03JSONDatasets {

    /**
     * Método principal.
     *
     * @param args Argumentos que le pasamos al programa.
     */
    public static void main(String[] args) throws Exception {

        // Arrancamos el contexto de ejecucion de Apache Spark
        SparkConf sparkConf = new SparkConf().setAppName("JSON Datasets");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaSQLContext sqlCtx = new JavaSQLContext(ctx);

        System.out.println("=== Data source: JSON Dataset ===");
        
        // A JSON dataset is pointed by path.
        // The path can be either a single text file or a directory storing text files.
        String path = "data/people.json";
        // Create a JavaSchemaRDD from the file(s) pointed by path
        JavaSchemaRDD peopleFromJsonFile = sqlCtx.jsonFile(path);

        // Because the schema of a JSON dataset is automatically inferred, to write queries,
        // it is better to take a look at what is the schema.
        peopleFromJsonFile.printSchema();
        // The schema of people is ...
        // root
        //  |-- age: IntegerType
        //  |-- name: StringType

        // Register this JavaSchemaRDD as a table.
        peopleFromJsonFile.registerTempTable("people");

        // SQL statements can be run by using the sql methods provided by sqlCtx.
        JavaSchemaRDD teenagers3 = sqlCtx.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

        // The results of SQL queries are JavaSchemaRDDs and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        List<String> teenagerNames = teenagers3.map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "Name: " + row.getString(0);
            }
        }).collect();
        
        // Sacamos por pantalla los resultados de la query
        for (String name : teenagerNames) {
            System.out.println(name);
        }

        // Alternatively, a JavaSchemaRDD can be created for a JSON dataset represented by
        // a RDD[String] storing one JSON object per string.
        List<String> jsonData = Arrays.asList(
                "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
        JavaRDD<String> anotherPeopleRDD = ctx.parallelize(jsonData);
        JavaSchemaRDD peopleFromJsonRDD = sqlCtx.jsonRDD(anotherPeopleRDD);

        // Take a look at the schema of this new JavaSchemaRDD.
        peopleFromJsonRDD.printSchema();
        // The schema of anotherPeople is ...
        // root
        //  |-- address: StructType
        //  |    |-- city: StringType
        //  |    |-- state: StringType
        //  |-- name: StringType

        peopleFromJsonRDD.registerTempTable("people2");

        JavaSchemaRDD peopleWithCity = sqlCtx.sql("SELECT name, address.city FROM people2");
        List<String> nameAndCity = peopleWithCity.map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "Name: " + row.getString(0) + ", City: " + row.getString(1);
            }
        }).collect();
        
        // Sacamos por pantalla los resultados de la query
        for (String name : nameAndCity) {
            System.out.println(name);
        }

        // Paramos el contexto.
        ctx.stop();
    }
}
