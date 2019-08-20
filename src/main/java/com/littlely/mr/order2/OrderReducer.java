package com.littlely.mr.order2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

    DoubleWritable vv = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double val;
        double result = -1000;

        for (DoubleWritable v : values){
            val = v.get();
            if (result < val){
                result = val;
            }
        }
        vv.set(result);

        context.write(key, vv);
    }
}
