package com.guagua.outdb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/9 22:28
 * @describe
 */
public class LogDBDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        args = new String[]{"/Users/guagua/improve/hadoop/ziliao/11_input/inputoutputformat", "data/filter-out1"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(LogDBDriver.class);

        job.setMapperClass(LogDBMapper.class);
        job.setReducerClass(LogDBReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(LogBeanDBWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputFormatClass(DBOutputFormat.class);
//        DBInputFormat.setInput(job, null, "test", null, null, "url");
        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver",
                "jdbc:mysql://10.211.55.50/test","guagua","123456");
        String[] fields = {"id", "url"};
        DBOutputFormat.setOutput(job, "test", fields);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
