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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.DataType;

import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;

/**
 * Example based on the Scala/Java/Python code from
 * https://spark.apache.org/docs/latest/sql-programming-guide.html
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public class J01ProgSpecifyingSch {

    public static void main(String[] args) throws Exception {

        // Arrancamos el contexto de ejecucion de Apache Spark
        SparkConf sparkConf = new SparkConf().setAppName("Programmatically Specifying the Schema");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaSQLContext sqlCtx = new JavaSQLContext(ctx);

        System.out.println("=== Data source: RDD ===");

        // Cargamos el contenido del fichero en una coleccion de string donde cada
        // uno se corresponde con una linea del fichero.
        JavaRDD<String> people = ctx.textFile("data/people.txt");

        // Nombre de los atributos del esquema
        String schemaString = "name age";

        // A continuacion generamos el schema basandonos en los atributos definidos
        // en la linea acnterior.
        List<StructField> fields = new ArrayList<>();
        // Recorremos la lista de atributos.
        for (String fieldName : schemaString.split(" ")) {
            // Para cada uno de los atributos definimos un campo de tipo String.
            fields.add(DataType.createStructField(fieldName, DataType.StringType, true));
        }
        // Cremos el tipo de dato a partir de los campos creados.
        StructType schema = DataType.createStructType(fields);

        // Convertimos las lineas que creamos como String a partir del fichero de
        // texto a instancias de filas. En este punto aun no podemos mapear al
        // esquema concreto.
        JavaRDD<Row> rowRDD = people.map(
                new Function<String, Row>() {
                    public Row call(String record) throws Exception {
                        String[] fields = record.split(",");
                        return Row.create(fields[0], fields[1].trim());
                    }
                });

        // Aplicamos el esquema que hemos creado a las lineas que hemos creado en
        // el paso anterior..
        JavaSchemaRDD peopleSchemaRDD = sqlCtx.applySchema(rowRDD, schema);

        // Registramos el esquema como tabla en el sistema, a partir de ahí podremos
        // lanzar querys.
        peopleSchemaRDD.registerTempTable("people");

        // Una vez hemos registrados los RDDs como tabla ya podemos lanzar consultas
        // SQL contra la tabla.
        JavaSchemaRDD results = sqlCtx.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

        // The results of SQL queries are SchemaRDDs and support all the normal 
        // RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        List<String> names = results.map(new Function<Row, String>() {
            public String call(Row row) {
                return "Name: " + row.getString(0);
            }
        }).collect();

        // Sacamos por pantalla los resultados de la query
        for (String name : names) {
            System.out.println(name);
        }

        // Paramos el contexto.
        ctx.stop();
    }
}
