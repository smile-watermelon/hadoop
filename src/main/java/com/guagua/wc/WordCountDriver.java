package com.guagua.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/8 13:44
 * @describe
 * 集群运行：hadoop jar hadoop-1.0-SNAPSHOT.jar com.guagua.wc.WordCountDriver /user/guagua/input /user/guagua/output
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        args = new String[]{"input/inputcombinetextinputformat", "output/combine-output"} ;
//        1、获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

//        2、设置jar存储位置
        job.setJarByClass(WordCountDriver.class);

//        3、关联mapper 和 reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

//        4、设置mapper 阶段输出数据的key, value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

//        5、设置最终数据输出的key 和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        6、设置程序输入，输出的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 不适用于求平均值的场景
//        方式一
//        job.setCombinerClass(WordCountCombiner.class);
//        方式二
        job.setCombinerClass(WordCountReducer.class);

//        job.setInputFormatClass(CombineTextInputFormat.class);
        // 设置切片的大小为4m，第一文件小于4m，会和第二个文件进行合并，组成一个切片
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        // 切片大小为20M，多个小文件合并成一个切片
        // 2023-02-08 21:24:45,964 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
//        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

//        7、提交job
//        job.submit();
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0 : 1);

    }








}
