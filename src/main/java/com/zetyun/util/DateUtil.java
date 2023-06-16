package com.zetyun.util;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
//    public static void main(String[] args) throws Exception{
//
//      //  System.out.println(dataToStamp("2020-12-24 09:37:53:371"));
//        Long ll=1608773873371L;
//       System.out.println(stampToString("1608773843371"));
//    }
    //字符串转时间戳
    public static Long dataToStamp(String str) throws Exception {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date date = simpleDateFormat.parse(str);
        long ts = date.getTime();
        return ts;
    }

    //时间戳转字符串
    public static String stampToString(String timestamp) throws Exception {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        Long date=Long.valueOf(timestamp);
        return simpleDateFormat.format(date);
    }

    //时间戳转字符串
    public static String longToString(Long timestamp) throws Exception {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        Long date=Long.valueOf(timestamp);
        return simpleDateFormat.format(date);
    }
}
