package com.zetyun.defined;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

import java.util.Set;

public class SpdbDynamicTableSink implements DynamicTableSink {



    private final TableSchema schema;
    private final ReadableConfig options ;

    public SpdbDynamicTableSink(TableSchema schema,  ReadableConfig options) {
        this.schema = schema;
        this.options = options;
    }

    /**
     * @param changelogMode
     * @return getChangelogMode()方法需要返回该Sink可以接受的change log行的类别。
     * 如果是Redis写入的数据可以是只追加的 ,
     * 也可以是带有回撤语义的（如各种聚合数据），
     * 因此支持INSERT、UPDATE_BEFORE和UPDATE_AFTER类别。
     */
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT).build();

    }

    //实现SinkRuntimeProvider，即编写SinkFunction供底层运行时调用
    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

       // String host = options.getOptional(HOST).get();

        SpdbSinkFunction spdbSinkFunction=new SpdbSinkFunction(options);

        return SinkFunctionProvider.of(spdbSinkFunction);


    }


    @Override
    public DynamicTableSink copy() {
        return  new SpdbDynamicTableSink(schema, options);
    }


    /**
     * sink   接口功能总结 日志标识
     * @return
     */
    @Override
    public String asSummaryString() {
        return "spdb table sink";
    }
}
