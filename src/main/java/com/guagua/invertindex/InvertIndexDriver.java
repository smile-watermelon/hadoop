package com.guagua.invertindex;

import com.guagua.group.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/11 14:56
 * @describe
 */
public class InvertIndexDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        args = new String[]{"input/inputoneindex", "output/invert-index-output"};
//        1、获取job对象
        Configuration conf = new Configuration();
        // 设置输出文件中，k和v的分隔符
//        conf.set(TextOutputFormat.SEPERATOR, ",");

        Job job = Job.getInstance(conf);

//        2、设置jar存储位置
        job.setJarByClass(InvertIndexDriver.class);

//        3、关联mapper 和 reducer
        job.setMapperClass(InvertIndexMapper.class);
        job.setReducerClass(InvertIndexReducer.class);

//        4、设置mapper 阶段输出数据的key, value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

//        5、设置最终数据输出的key 和value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        6、设置程序输入，输出的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        7、提交job
//        job.submit();
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0 : 1);
    }
}
