package com.zetyun.watermarks;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

import static com.zetyun.util.DateUtil.dataToStamp;
import static com.zetyun.util.DateUtil.longToString;
import static com.zetyun.util.DateUtil.stampToString;


public class MyTimestampExtractor implements AssignerWithPeriodicWatermarks<String>  {

    //设置最大延迟时间
    //当前watermake时间减去 设置的maxTiemlog ,就会触发当前窗口
    private final long maxTiemLag=0L; //30 seconds
    private Long currentMaxTimestamp=0L;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        //System.out.println("currentMaxTimestamp:"+currentMaxTimestamp+"      maxTiemLag:"+maxTiemLag);
        Watermark a = new Watermark(currentMaxTimestamp-maxTiemLag);
        return a;
    }

    @Override
    public long extractTimestamp(String str, long l) {
        Long time=0L;
        JSONObject jsonObject = JSON.parseObject(str);
        try {
             time = dataToStamp(jsonObject.get("event_time").toString());
            currentMaxTimestamp=Math.max(time,currentMaxTimestamp);
            //System.out.println("time;"+time +"---------"+"time:"+stampToString(time.toString()));
            //System.out.println("currentMaxTimestamp;"+currentMaxTimestamp +"---------"+"currentMaxTimestamp:"+stampToString(currentMaxTimestamp.toString()));
            //System.out.println("watermake: "+getCurrentWatermark().getTimestamp()+"getCurrentWatermark().getTimestamp()"+longToString(getCurrentWatermark().getTimestamp()));
            return time;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return time;

    }
}
