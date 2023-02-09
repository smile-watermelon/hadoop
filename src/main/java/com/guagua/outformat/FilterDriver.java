package com.guagua.outformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/8 21:38
 * @describe
 */
public class FilterDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        args = new String[]{"/Users/guagua/improve/hadoop/ziliao/11_input/inputoutputformat", "data/filter-out"};

        Configuration conf = new Configuration();
//        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");

        Job job = Job.getInstance(conf);

        job.setJarByClass(FilterDriver.class);

        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputFormatClass(FilterOutputFormat.class);

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
