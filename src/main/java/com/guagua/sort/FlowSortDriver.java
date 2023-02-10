package com.guagua.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/8 13:44
 * @describe
 * 集群运行：hadoop jar hadoop-1.0-SNAPSHOT.jar com.guagua.wc.WordCountDriver /user/guagua/input /user/guagua/output
 */
public class FlowSortDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        args = new String[]{"data/flow", "data/flow-output"};
        args = new String[]{"output/flow-output", "output/flow-sort-output"};
//        1、获取job对象
        Configuration conf = new Configuration();
        conf.set(TextOutputFormat.SEPERATOR, ",");
        Job job = Job.getInstance(conf);

//        2、设置jar存储位置
        job.setJarByClass(FlowSortDriver.class);

        job.setPartitionerClass(FlowSortPartitioner.class);
        job.setNumReduceTasks(5);

//        3、关联mapper 和 reducer
        job.setMapperClass(FlowBeanSortMapper.class);
        job.setReducerClass(FlowBeanSortReducer.class);

//        4、设置mapper 阶段输出数据的key, value类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

//        5、设置最终数据输出的key 和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

//        6、设置程序输入，输出的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        7、提交job
//        job.submit();
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0 : 1);

    }








}
