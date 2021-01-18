package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.util.Collector;
import java.util.ArrayList;

// Attempt
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction.OnTimerContext;
import java.util.PriorityQueue;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
// End attempt

import java.util.Iterator;

public class exercise2 {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Input data
        DataStreamSource<String> text = env.readTextFile(params.get("input"));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Process the data for right types
        SingleOutputStreamOperator<Tuple6<Long, Long, Long, Long, Long, Long>> mapStream = text.
                map(new MapFunction<String, Tuple6<Long, Long, Long, Long, Long, Long>>() {
                    public Tuple6<Long, Long, Long, Long, Long, Long> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        Tuple6<Long,Long,Long,Long,Long,Long> out = new Tuple6(
                                Long.parseLong(fieldArray[0]), // Time
                                Long.parseLong(fieldArray[1]), // VID
                                Long.parseLong(fieldArray[2]), // Spd
                                Long.parseLong(fieldArray[3]), // XWay
                                Long.parseLong(fieldArray[5]), // Dir
                                Long.parseLong(fieldArray[6])); // Seg
                        return out;
                    }
                })
                .filter(new FilterFunction<Tuple6<Long, Long, Long, Long, Long, Long>>(){
                    public boolean filter(Tuple6<Long, Long, Long, Long, Long, Long> in) throws Exception {
                        return (in.f5 >= Long.parseLong(params.get("startSegment")) && // select relevant segments
                                in.f5 <= Long.parseLong(params.get("endSegment")) &&
                                in.f4 == 0); // only eastbound vehicle
                    }
                });

        KeyedStream<Tuple6<Long, Long, Long, Long, Long, Long>, Tuple> keyedStream = mapStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple6<Long, Long, Long, Long, Long, Long>>(){
                    public long extractAscendingTimestamp(Tuple6<Long, Long, Long, Long, Long, Long> element){
                        return element.f0 * 1000;
                    }})
                .keyBy(1,4);

        SingleOutputStreamOperator<Tuple6<Long, Long, Double, Long, Long, Long>> avgSpeed = keyedStream.
                window(TumblingEventTimeWindows.of(Time.seconds(Long.parseLong(params.get("time"))))).
                apply(new exercise2.calculateAvgSpeed()).
                filter(new FilterFunction<Tuple6<Long, Long, Double, Long, Long, Long>>(){
                    public boolean filter(Tuple6<Long, Long, Double, Long, Long, Long> in) throws Exception {
                        return (in.f2 > Integer.parseInt(params.get("speed")));
                    }
                });

        SingleOutputStreamOperator<Tuple4<Long, Long, Integer, String>> speedersOnXway = avgSpeed.
                windowAll(TumblingEventTimeWindows.of(Time.seconds(Long.parseLong(params.get("time"))))).
                apply(new exercise2.SpeedersOnXway());

        // Output result to a file
        if (params.has("output")){
            // speedersOnXway.print();
            speedersOnXway.writeAsCsv(params.get("output")).setParallelism(1);
        }
        env.execute("exercise2");
    }

    public static class calculateAvgSpeed implements WindowFunction<Tuple6<Long, Long, Long, Long, Long, Long>,
            Tuple6<Long, Long, Double, Long, Long, Long>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple6<Long, Long, Long, Long, Long, Long>> iterable,
                          Collector<Tuple6<Long, Long, Double, Long, Long, Long>> collector) throws Exception {
            Iterator<Tuple6<Long, Long, Long, Long, Long, Long>> iterator = iterable.iterator();
            Tuple6<Long, Long, Long, Long, Long, Long> first = iterator.next();
            Long ts = 0L;
            Long xway = 0L;
            Long VID = 0L;
            Long spd = 0L;
            Long Seg = 0L;
            Integer count = 1;
            if(first!=null){
                ts = first.f0;
                xway = first.f3;
                VID = first.f1;
                spd = first.f2;
                Seg = first.f5;
            }
            while(iterator.hasNext()){
                Tuple6<Long, Long, Long, Long, Long, Long> next = iterator.next();
                spd += next.f2;
                count++;
            }
            Double avg = spd.doubleValue()/count;
            collector.collect(new Tuple6<Long, Long, Double, Long, Long, Long>(ts, VID, avg, xway, count.longValue(), Seg));
        }
    }

    public static class SpeedersOnXway implements AllWindowFunction<Tuple6<Long, Long, Double, Long, Long, Long>,
            Tuple4<Long, Long, Integer, String>, TimeWindow> {
        public void apply(TimeWindow timeWindow, Iterable<Tuple6<Long, Long, Double, Long, Long, Long>> iterable,
                          Collector<Tuple4<Long, Long, Integer, String>> collector) throws Exception {
            Iterator<Tuple6<Long, Long, Double, Long, Long, Long>> iterator = iterable.iterator();
            Tuple6<Long, Long, Double, Long, Long, Long> first = iterator.next();
            Long ts = 0L;
            Long xway = 0L;
            Integer count = 1;
            String vids = "[ ";
            if(first!=null){
                ts = first.f0;
                xway = first.f3;
                vids += first.f1 + " ";
            }
            while(iterator.hasNext()){
                Tuple6<Long, Long, Double, Long, Long, Long> next = iterator.next();
                if(ts > next.f0){
                    ts = next.f0;
                }
                vids += "- " + next.f1 + " ";
                count += 1;
            }
            vids += "]";
            collector.collect(new Tuple4<Long, Long, Integer, String>(ts, xway, count, vids));
        }
    }

}