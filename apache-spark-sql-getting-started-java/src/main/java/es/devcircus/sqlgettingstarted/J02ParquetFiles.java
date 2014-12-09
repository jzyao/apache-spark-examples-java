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
public class J02ParquetFiles {

    public static void main(String[] args) throws Exception {

        // Arrancamos el contexto de ejecucion de Apache Spark
        SparkConf sparkConf = new SparkConf().setAppName("Parquet Files");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaSQLContext sqlCtx = new JavaSQLContext(ctx);

        System.out.println("=== Data source: RDD ===");

        // Cargamos los datos a partir de un fichero de texto y los mapeamos a 
        // instancia de tipo Person.
        JavaRDD<Person> people = ctx.textFile("data/people.txt").map(
                new Function<String, Person>() {
                    @Override
                    public Person call(String line) {
                        String[] parts = line.split(",");

                        Person person = new Person();
                        person.setName(parts[0]);
                        person.setAge(Integer.parseInt(parts[1].trim()));

                        return person;
                    }
                });

        // Creamos el esquema a partir de los datos importados y lo registramos
        // como una tabla.
        JavaSchemaRDD schemaPeople = sqlCtx.applySchema(people, Person.class);

        System.out.println("=== Data source: Parquet File ===");

        // JavaSchemaRDDs can be saved as parquet files, maintaining the schema information.
        schemaPeople.saveAsParquetFile("people.parquet");

        // Read in the parquet file created above.
        // Parquet files are self-describing so the schema is preserved.
        // The result of loading a parquet file is also a JavaSchemaRDD.
        JavaSchemaRDD parquetFile = sqlCtx.parquetFile("people.parquet");

        //Parquet files can also be registered as tables and then used in SQL statements.
        parquetFile.registerTempTable("parquetFile");
        
        JavaSchemaRDD teenagers2 = sqlCtx.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19");
        
        List<String> teenagerNames = teenagers2.map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "Name: " + row.getString(0);
            }
        }).collect();
        
        // Sacamos por pantalla los resultados de la query
        for (String name : teenagerNames) {
            System.out.println(name);
        }

        // Paramos el contexto.
        ctx.stop();
    }
}
