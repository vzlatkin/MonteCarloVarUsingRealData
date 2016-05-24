package com.hortonworks.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.Collections;
import java.util.Map;

/**
 * TODO
 * calculate  average price change per stock
 * run the same algorithm in R
 * add functions
 * add a start and stop timer for benchmarking
 * add kryo serializer
 * add broadcast code
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Main m = new Main();
        m.run(args);
    }

    Object run(String[] args) {
        /*
        Initializations
        */
        final int NUM_TRIALS = 1000000;
        String listOfCompanies = new File("companies_list.txt").toURI().toString();
        String stockDataDir = "hdfs://sandbox.hortonworks.com/tmp/stockData/*.csv";

        if (args.length > 0) {
            listOfCompanies = args[0];
        }
        if (args.length > 1) {
            stockDataDir = args[1];
        }

        SparkConf conf = new SparkConf().setAppName("monte-carlo-var-calculator");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*
        read a list of stock symbols and their weights in the portfolio, then transform into a Map<Symbol,Weight>
        */
        JavaRDD<String> filteredFileRDD = sc.textFile(listOfCompanies).filter(s -> !s.startsWith("#") && !s.trim().isEmpty());
        JavaPairRDD<String, Float> symbolsAndWeightsRDD = filteredFileRDD.filter(s->!s.startsWith("Symbol")).mapToPair(s ->
        {
            String[] splits = s.split(",", -2);
            return new Tuple2<>(splits[0], new Float(splits[1]));
        });
        Map<String, Float> symbolsAndWeights = symbolsAndWeightsRDD.collectAsMap();

        //debug
        System.out.println("companies and their weights in the overall portfolio");
        symbolsAndWeightsRDD.foreach(t -> System.out.println("s:" + t._1() + " w: " + t._2()));

        /*
        read all stock trading data, and transform
        1. get a PairRDD of date -> (symbol, changeInPrice)
        2. reduce by key to get all dates together
        3. filter every date that doesn't have the max number of symbols
\        */

        // 1. get a PairRDD of date -> Tuple2(symbol, changeInPrice)
        JavaPairRDD<String, Tuple2> datesToSymbolsAndChangeRDD = sc.textFile(stockDataDir).flatMapToPair(x -> {
            //skip header
            if (x.contains("Change_Pct")) {
                return Collections.EMPTY_LIST;
            }
            String[] splits = x.split(",", -2);
            Float changeInPrice = new Float(splits[8]);
            String symbol = splits[7];
            String date = splits[0];
            return Collections.singletonList(new Tuple2<>(date, new Tuple2<>(symbol, changeInPrice)));
        });
        //debug
        datesToSymbolsAndChangeRDD.take(10).forEach(x -> System.out.println(x._1() + "->" + x._2()));

        //2. reduce by key to get all dates together
        JavaPairRDD<String, Iterable<Tuple2>> groupedDatesToSymbolsAndChangeRDD = datesToSymbolsAndChangeRDD.groupByKey();
        //debug
        groupedDatesToSymbolsAndChangeRDD.take(10).forEach(x -> System.out.println(x._1() + "->" + x._2()));

        //3. filter every date that doesn't have the max number of symbols
        long numSymbols = symbolsAndWeightsRDD.count();
        Map<String, Object> countsByDate = datesToSymbolsAndChangeRDD.countByKey();
        JavaPairRDD<String, Iterable<Tuple2>> filterdDatesToSymbolsAndChangeRDD = groupedDatesToSymbolsAndChangeRDD.filter(x -> (Long) countsByDate.get(x._1()) >= numSymbols);
        long numEvents = filterdDatesToSymbolsAndChangeRDD.count();
        //debug
        System.out.println("num symbols: " + numSymbols);
        filterdDatesToSymbolsAndChangeRDD.take(10).forEach(x -> System.out.println(x._1() + "->" + x._2()));

        if (numEvents < 1) {
            System.out.println("No trade data");
            return "No trade data";
        }

        /*
        execute NUM_TRIALS
        1. pick a random date from the list of historical trade dates
        2. sum(stock weight in overall portfolio * change in price on that date)
         */
        double fraction = 1.0 * NUM_TRIALS / numEvents;
        System.out.println("fraction: " + fraction);
        JavaRDD<Float> resultOfTrials = filterdDatesToSymbolsAndChangeRDD.sample(true, fraction).map(i -> {
            Float total = 0f;
            for (Tuple2 t : i._2()) {
                String symbol = t._1().toString();
                Float changeInPrice = new Float(t._2().toString());
                Float weight = symbolsAndWeights.get(symbol);

                total += changeInPrice * weight;
                //debug
//                System.out.println(symbol + " with weight " + weight + " changed by " + changeInPrice
//                        + " for a total of " + total);
            }
            return total;
        });
        //debug
        System.out.println("total runs: " + resultOfTrials.count());
        resultOfTrials.take(10).forEach(System.out::println);

        Float result = resultOfTrials.takeOrdered(Math.max(NUM_TRIALS / 20, 1)).get(0);
        System.out.println("With a 95% certainty, you won't lose more than " + result
                + "% in a single day");

        sc.stop();

        return result;
    }
}