/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.test_flink_java;

/**
 *
 * @author Prasad Golla Durga
 */
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class PrimeNumber {

    public static void main(String[] args) throws Exception {
        // Create the execution environment of Flink.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("localhost", 9000);

        DataStream<Integer> parsed = text.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) {
                int num = Integer.parseInt(value);

                int i = 2;
                boolean flag = false;
                while (i <= num / 2) {
                    if (num % i == 0) {
                        flag = true;
                        break;
                    }

                    ++i;
                }

                if (!flag) {
                    System.out.println("A Fraud is Detected (It is a prime number).");
                } else {

                }
                if (flag == true) {
                    return num;
                } else {
                    return num;

                }
            }
        }
        );

        parsed.print().setParallelism(1);
        env.execute("Socket Window WordCount");
    }
}
