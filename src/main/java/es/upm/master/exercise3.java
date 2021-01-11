package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;


public class exercise3 {
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
                    public Tuple6<Long, Long, Long, Long, Long, Long> map(String in) throws Exception {
                        String[] fieldArray = in.split(",");
                        Tuple6<Long, Long, Long, Long, Long, Long> out = new Tuple6(Long.parseLong(fieldArray[0]),
                                Long.parseLong(fieldArray[1]), Long.parseLong(fieldArray[2]), Long.parseLong(fieldArray[3]),
                                Long.parseLong(fieldArray[6]), Long.parseLong("1"));
                        return out;
                    }
                })
                .filter(new FilterFunction<Tuple6<Long, Long, Long, Long, Long, Long>>() {
                    public boolean filter(Tuple6<Long, Long, Long, Long, Long, Long> in) throws Exception {
                        return in.f4 == Long.parseLong(params.get("segment"));
                    }
                });


        KeyedStream<Tuple6<Long, Long, Long, Long, Long, Long>, Tuple> keyedStream = mapStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple6<Long, Long, Long, Long, Long, Long>>(){
                                     public long extractAscendingTimestamp(Tuple6<Long, Long, Long, Long, Long, Long> element){
                                         return element.f0 * 10000;
                                     }})
                .keyBy(1);


        SingleOutputStreamOperator<Tuple6<Long, Long, Long, Long, Long, Long>> sumSpeed = keyedStream.
                window(TumblingEventTimeWindows.of(Time.seconds(36000))).apply(new calculateAvgSpeed());


        SingleOutputStreamOperator<Tuple3<Long, Long, Long>> result = sumSpeed.
                map(new MapFunction<Tuple6<Long, Long, Long, Long, Long, Long>, Tuple3<Long, Long, Long>>() {
                    public Tuple3<Long, Long, Long> map(Tuple6<Long, Long, Long, Long, Long, Long> in) throws Exception {
                        Tuple3<Long, Long, Long> out = new Tuple3<Long, Long, Long>(in.f1, in.f2, in.f3);
                        return out;
                    }
                });

        SingleOutputStreamOperator<Tuple6<Long, Long, Long, Long, Long, Long>> maxAvgSpeed = sumSpeed.
                windowAll(TumblingEventTimeWindows.of(Time.seconds(36000))).apply(new calculateMaxAvgSpeed());

        SingleOutputStreamOperator<Tuple3<Long, Long, Long>> result2 = maxAvgSpeed.
                map(new MapFunction<Tuple6<Long, Long, Long, Long, Long, Long>, Tuple3<Long, Long, Long>>() {
                    public Tuple3<Long, Long, Long> map(Tuple6<Long, Long, Long, Long, Long, Long> in) throws Exception {
                        Tuple3<Long, Long, Long> out = new Tuple3<Long, Long, Long>(in.f1, in.f2, in.f3);
                        return out;
                    }
                });

        // Output result to a file
        if (params.has("output1")) {
            result.writeAsCsv(params.get("output1"));
        }

        if (params.has("output2")) {
            result2.writeAsCsv(params.get("output2"));
        }
        env.execute("exercise3");
    }

    public static class calculateAvgSpeed implements WindowFunction<Tuple6<Long, Long, Long, Long, Long, Long>,
            Tuple6<Long, Long, Long, Long, Long, Long>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple6<Long, Long, Long, Long, Long, Long>> iterable,
                          Collector<Tuple6<Long, Long, Long, Long, Long, Long>> collector) throws Exception {
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
                Seg = first.f4;
            }
            while(iterator.hasNext()){
                Tuple6<Long, Long, Long, Long, Long, Long> next = iterator.next();
                spd += next.f2;
                count++;
            }
            Long avg = spd/count;
            collector.collect(new Tuple6<Long, Long, Long, Long, Long, Long>(ts, VID, xway, avg, Seg, count.longValue()));
        }
    }

    public static class calculateMaxAvgSpeed implements AllWindowFunction<Tuple6<Long, Long, Long, Long, Long, Long>,
            Tuple6<Long, Long, Long, Long, Long, Long>, TimeWindow> {
        public void apply(TimeWindow timeWindow, Iterable<Tuple6<Long, Long, Long, Long, Long, Long>> iterable,
                          Collector<Tuple6<Long, Long, Long, Long, Long, Long>> collector) throws Exception {
            Iterator<Tuple6<Long, Long, Long, Long, Long, Long>> iterator = iterable.iterator();
            Tuple6<Long, Long, Long, Long, Long, Long> first = iterator.next();
            Long ts = 0L;
            Long xway = 0L;
            Long VID = 0L;
            Long spd = 0L;
            Long Seg = 0L;
            Long count = 1L;
            if(first!=null){
                ts = first.f0;
                xway = first.f2;
                VID = first.f1;
                spd = first.f3;
                Seg = first.f4;
                count = first.f5;
            }
            while(iterator.hasNext()){
                Tuple6<Long, Long, Long, Long, Long, Long> next = iterator.next();
                if (next.f3 > spd) {
                    spd = next.f3;
                    VID = next.f1;
                }
            }
            collector.collect(new Tuple6<Long, Long, Long, Long, Long, Long>(ts, VID, xway, spd, Seg, count));
        }
    }
}


