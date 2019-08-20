package com.littlely.mr.flowsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void set(long sum_upFlow, long sum_downFlow){
        upFlow = sum_upFlow;
        downFlow = sum_downFlow;
        sumFlow = sum_upFlow + sum_downFlow;
    }

    public FlowBean(){
        super();
    }

    public FlowBean(long upFlow, long downFlow){
        super();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        sumFlow = upFlow + downFlow;
    }

    //序列化方法
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    //反序列化方法
    public void readFields(DataInput in) throws IOException {
        //必须和序列化方法顺序一致
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }
}
