package com.zetyun.defined;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

public class SpdbSinkFunction extends RichSinkFunction<RowData>implements CheckpointedFunction {
    private ReadableConfig options ;

    public SpdbSinkFunction( ReadableConfig options ) {
        this.options=options;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        System.out.printf("2222222");
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }

    @Override
    public void invoke(RowData value) throws Exception {
        super.invoke(value);
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        super.invoke(value, context);
        System.out.printf("11111111");
    }
}
