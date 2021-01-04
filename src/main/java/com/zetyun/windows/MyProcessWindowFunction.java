package com.zetyun.windows;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Iterator;

import static com.zetyun.util.DateUtil.longToString;

//IN, OUT, KEY, W extends Window
public class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, String>, Tuple2<String, Long>, Tuple, TimeWindow> {

    //自定义状态
    private transient MapState<String, Long> pvCountState;
    private SimpleDateFormat formatter =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //初始化状态
        MapStateDescriptor pvDescriptor = new MapStateDescriptor<String, Long>("pv_count", String.class, Long.class);
        pvCountState = getRuntimeContext().getMapState(pvDescriptor);
    }

    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, String>> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
        //获取当前窗口时间，用当前窗口时间作为key，插入到state
        TimeWindow window = context.window();
        long startWindow = window.getStart();
        System.out.println("startWindow: "+longToString(startWindow)+ "endWindow: "+longToString(window.getEnd()));
        String formatStartWindow = formatter.format(startWindow).toString();
        //计算当前窗口的count
        long pvCount = 0;
        Iterator<Tuple2<String, String>> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            pvCount += 1;
        }
        //获取全局的pvCountState
        Long pvState = pvCountState.get(formatStartWindow);
        if (pvState == null) {
            pvCountState.put(formatStartWindow, pvCount);
        } else {
            pvCountState.put(formatStartWindow, pvCount + pvState);
        }
        collector.collect(new Tuple2<String, Long>(formatStartWindow, pvCountState.get(formatStartWindow)));
    }


}
