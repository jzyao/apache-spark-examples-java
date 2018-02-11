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
package es.devcircus.sparkwordcount;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public class JavaWordCount {

    /**
     * Método principal.
     *
     * @param args Argumentos que le pasamos al programa.
     */
    public static void main(String[] args) {

        // Configure Spark
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count"));

        final int threshold;
        threshold = Integer.parseInt(args[1]);

        // split each document into words
        JavaRDD<String> lines = sc.textFile(args[0]);        
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // count the occurrence of each word
        JavaPairRDD<String, Integer> counts;
        counts = words.mapToPair(
                (String s) -> new Tuple2<String, Integer>(s, 1)).reduceByKey(
                        (Integer i1, Integer i2) -> i1 + i2);

        // filter out words with less than threshold occurrences
        JavaPairRDD<String, Integer> filtered = counts.filter(
                (Tuple2<String, Integer> tup) -> tup._2() >= threshold);

        // count characters
        JavaPairRDD<Character, Integer> charCounts;
        charCounts
                = filtered.flatMap((Tuple2<String, Integer> s) -> {
                    ArrayList<Character> chars = new ArrayList<>(s._1().length());
                    for (char c : s._1().toCharArray()) {
                        chars.add(c);
                    }
                    return chars.iterator();
        }).mapToPair((Character c) -> new Tuple2<Character, Integer>(c, 1)).reduceByKey((Integer i1, Integer i2) -> i1 + i2);
        
        // print results
        System.out.println(charCounts.collect());
    }
}
