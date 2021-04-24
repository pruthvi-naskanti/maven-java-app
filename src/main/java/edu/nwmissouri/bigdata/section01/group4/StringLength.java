/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.nwmissouri.bigdata.section01.group4;

/**
 *
 * @author S538094
 */
        
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
public class StringLength {
    
    public void FraudDetectStringLength() throws Exception{
         // Create the execution environment.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Get the input data by connecting the socket. Here it is connected to the local port 9000. If the port 9000 has been already occupied, change to another port.
        DataStream<String> text = env.socketTextStream("localhost", 9000);
        // Taking the input data and checking its even or not,if even return number else its a fraud return 0.
        DataStream<Integer> parsed = text.map(new MapFunction<String, Integer>() {
                                        @Override
                                        public Integer map(String input) {
                                            if(input.length()<=8){
                                                return input.length();
                                            }
                                            else
                                                return -1;
                                        }
                                    });
       
        parsed.print().setParallelism(1);
        env.execute("Socket Window WordCount");
    }
    
}
    

