package com.littlely.mr.table;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        // 1 准备存储订单的集合
        ArrayList<TableBean> orderBeans = new ArrayList<TableBean>();
        // 2 准备bean对象
        TableBean pdBean = new TableBean();
        for (TableBean bean : values){
            if ("order".equals(bean.getFlag())){
                // 拷贝传递过来的每条订单数据到集合
                TableBean orderBean = new TableBean();
                try{
                    BeanUtils.copyProperties(orderBean, bean);
                    orderBeans.add(orderBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }else{
                try {
                    BeanUtils.copyProperties(pdBean, bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        // 3 表的拼接
        for (TableBean bean : orderBeans){
            bean.setpName(pdBean.getpName());
            // 4 写出
            context.write(bean, NullWritable.get());
        }
    }
}
