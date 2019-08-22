package com.littlely.mr.cachetable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Map<String, String> pdMap = new HashMap<String, String>(); //also can be treeMap
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        String line;

        // 1 获取缓存文件
        URI[] cacheFiles = context.getCacheFiles();

        //-------------------------hadoop集群上跑----------------------------
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path path = new Path(cacheFiles[0].toString());
        FSDataInputStream fsin = fs.open(path);
        DataInputStream in = new DataInputStream(fsin);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"));
        //--------------------------------------------------------------------

        //-----------------------本地跑---------------------------------------
//        String path = cacheFiles[0].getPath();
//        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "utf-8"));
        //--------------------------------------------------------------------

        while(StringUtils.isNotEmpty(line = reader.readLine())){
            // 2 切割
            String[] fields = line.split("\t");
            // 3 缓存数据到集合
            pdMap.put(fields[0], fields[1]);
        }
        // 4 关闭流
        reader.close();
        in.close();
        fsin.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 获取一行
        String line = value.toString();
        // 2 截取
        String[] fields = line.split("\t");
        // 3 获取产品id
        String pId = fields[1];
        // 4 获取商品名称
        String pdName = pdMap.get(pId);
        // 5 拼接
        k.set(fields[0] + "\t" + fields[2] + "\t" + pdName);
        // 6 写出
        context.write(k, NullWritable.get());
    }
}
