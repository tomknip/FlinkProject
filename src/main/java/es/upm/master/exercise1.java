package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;


public class exercise1 {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Input data
        DataStream<String> text;
        text = env.readTextFile(params.get("input"));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Process the data for right types
        SingleOutputStreamOperator<Tuple4<Long, Long, Long, Integer>> mapStream = text.
                map(new MapFunction<String, Tuple4<Long, Long, Long, Integer>>() {
                    public Tuple4<Long,Long, Long, Integer> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        Tuple4<Long,Long,Long, Integer> out = new Tuple4(Long.parseLong(fieldArray[0]),
                                Long.parseLong(fieldArray[3]), Long.parseLong(fieldArray[4]),
                                1);
                        return out;
                    }
                })
                .filter(new FilterFunction<Tuple4<Long, Long, Long, Integer>>(){
                    public boolean filter(Tuple4<Long, Long, Long, Integer> in) throws Exception {
                        return in.f2 == 4;
                    }
                });

        KeyedStream<Tuple4<Long, Long, Long, Integer>, Tuple> keyedStream = mapStream.assignTimestampsAndWatermarks(new
                AscendingTimestampExtractor<Tuple4<Long, Long, Long, Integer>>(){
                    public long extractAscendingTimestamp(Tuple4<Long, Long, Long, Integer> element){
                        return element.f0 * 10000;
                    }}).keyBy(1, 2);

        SingleOutputStreamOperator<Tuple4<Long, Long, Long, Integer>> numVehiclesPerTime =
                keyedStream.window(TumblingEventTimeWindows.of(Time.hours(1))).apply(new NumberVehicles());

        // Next one is same as wordCount
//        SingleOutputStreamOperator<Tuple4<Long, Long, Long, Integer>> result = keyedStream.reduce(new ReduceFunction<Tuple4<Long, Long, Long, Integer>>() {
//            public Tuple4<Long, Long, Long, Integer> reduce(Tuple4<Long, Long, Long, Integer> accumulator, Tuple4<Long, Long, Long, Integer> input) throws Exception {
//                return new Tuple4<Long, Long, Long, Integer>(accumulator.f0, accumulator.f1, accumulator.f2, accumulator.f3 + input.f3);
//            }
//        });

        // Output result to a file
        if (params.has("output")){
            numVehiclesPerTime.writeAsCsv(params.get("output"));
        }
        env.execute("exercise1");

    }

    public static class NumberVehicles implements WindowFunction<Tuple4<Long, Long, Long, Integer>,
            Tuple4<Long, Long, Long, Integer>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<Long, Long, Long, Integer>> input,
                          Collector<Tuple4<Long, Long, Long, Integer>> collector) throws Exception {
            Iterator<Tuple4<Long, Long, Long, Integer>> iterator = input.iterator();
            Tuple4<Long, Long, Long, Integer> first = iterator.next();
            Long ts = 0L;
            Long xway = 0L;
            Long lane = 0L;
            Integer tot = 0;
            if(first!=null){
                ts = first.f0;
                xway = first.f1;
                lane = first.f2;
                tot = first.f3;
            }
            while(iterator.hasNext()){
                Tuple4<Long, Long, Long, Integer> next = iterator.next();
                tot += next.f3;
            }
            collector.collect(new Tuple4<Long, Long, Long, Integer>(ts, xway, lane, tot));
        }
    }
}