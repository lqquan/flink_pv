import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zetyun.watermarks.MyTimestampExtractor;
import com.zetyun.windows.MyProcessWindowFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 *
 *
 */

public class KafkaWindowCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment  streamEnv=StreamExecutionEnvironment.getExecutionEnvironment();

        //set 事件时间
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //checkpoint

        //kafka info
        Properties props=new Properties();
        props.setProperty("bootstrap.servers","172.20.8.154:9092");
        props.setProperty("group.id","flink_pv");
        props.setProperty("auto.offset.reset","earliest");//latest,earliest

        //获取kafka数据
        DataStreamSource<String> readData = streamEnv.addSource(new FlinkKafkaConsumer<String>("pv",
                new SimpleStringSchema(), props));

       //设置Watermarks
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator =
                readData.assignTimestampsAndWatermarks(new MyTimestampExtractor());


        //将不需要的数据过滤，减少网络开销，加大传输速度
        SingleOutputStreamOperator<Tuple2<String, String>> flatMapFiled = stringSingleOutputStreamOperator.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String str, Collector<Tuple2<String, String>> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(str);
                System.out.println("jsonObject:"+jsonObject);
                out.collect(new Tuple2<String, String>(jsonObject.getString("event_time"), jsonObject.getString("session_id")));
            }
        });
       // flatMapFiled.print();
        //先做keyby，这样window可以加大并发
        KeyedStream<Tuple2<String, String>, Tuple> tuple2TupleKeyedStream = flatMapFiled.keyBy(0);

        //Time.days(1),Time.hours(-8): 窗口大小是1天，由于flink默认的窗口时区是UTC-0,其地区需要指定时间偏移量调整时区，
        //TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8))
       // tuple2TupleKeyedStream.print();
        SingleOutputStreamOperator<Tuple2<String, Long>> process = tuple2TupleKeyedStream.window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                //ContinuousEventTimeTrigger 表示连续事件时间触发器，用在EventTime属性的任务流中，以事件时间的进度来推动定期触发
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(1)))
                //CountTrigger 指定条数触发
                .trigger(CountTrigger.of(1))
                //CountEvictor :数量剔除器。在window中保留指定数量的元素，并从窗口头部开始丢弃其余元素
                //deltaEvictor: 阈值剔除器。计算window中最后一个元素与其余每个元素之间的增量，丢弃增量大于或等于阈值的元素
                //TimeEvictor：时间剔除器。保留window最近一段内的元素，并丢弃其余元素
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                //.sum(1).print();
                //Flink DataStream Window 窗口函数 ReduceFuntion,AggregateFunction,processWindowFuntion
                //增量聚合：窗口不维护原始数据，只维护中间结果，每次基本中间结果和增量数据进行聚合， ReduceFuntion,AggregateFunction
                //全量聚合：窗口需要维护全部的数据，窗口触发会全量聚合，如processWindowFuntion
                .process(new MyProcessWindowFunction());

        process.print();

        //redis conf
//        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder()
//                .setHost("172.20.3.195")
//                .setPort(6382)
//                .build();

        //sink kafka


     //   process.addSink(new RedisSink<Tuple2<String, Long>>(redisConf,new MyRedisMapper()));


        streamEnv.execute("KafkaWindowCount");
    }


}
