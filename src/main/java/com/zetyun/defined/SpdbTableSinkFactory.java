package com.zetyun.defined;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * 1.  定义 SPI 的工厂类 `RedisDynamicTableFactory implements DynamicTableSinkFactory`，并且在 resource\\META-INF 下创建 SPI 的插件文件
 *
 * 2.  实现 factoryIdentifier 标识 `redis`
 *
 * 3.  实现 `RedisDynamicTableFactory#createDynamicTableSink` 来创建对应的 source `RedisDynamicTableSink`
 *
 * 4.  定义 `RedisDynamicTableSink implements DynamicTableSink`
 *
 * 5.  实现 `RedisDynamicTableFactory#getSinkRuntimeProvider` 方法，创建具体的维表 UDF  `RichSinkFunction<T>`，这里直接服用了 bahir redis 中的 `RedisSink<IN>`
 *
 * ————————————————
 * 版权声明：本文为CSDN博主「大数据羊说」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
 * 原文链接：https://blog.csdn.net/qq_34608620/article/details/119745550
 */
public class SpdbTableSinkFactory implements DynamicTableSinkFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SpdbTableSinkFactory.class);
    public static final String FACTORY_ID = "spdb";
    public static final ConfigOption<String> PATH= ConfigOptions.key("path").stringType().noDefaultValue().withDescription("put path");



    // 标识 redis
    @Override
    public String factoryIdentifier() {
        return FACTORY_ID;
    }


    //必填参数
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
         return new HashSet<>();
    }

    @Override
    //非必填项目
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options=new HashSet<>();
        options.add(PATH);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper=FactoryUtil.createTableFactoryHelper(this, context);
        //验证参数
        helper.validate();
        ReadableConfig options = helper.getOptions();

        TableSchema schema = context.getCatalogTable().getSchema();
        return new SpdbDynamicTableSink(schema,options);
    }


}
