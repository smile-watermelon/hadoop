package com.guagua.reducejoin;

import com.guagua.outformat.FilterDriver;
import com.guagua.outformat.FilterMapper;
import com.guagua.outformat.FilterOutputFormat;
import com.guagua.outformat.FilterReducer;
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
 * @date 2023/2/10 11:08
 * @describe
 */
public class JoinDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        args = new String[]{"/Users/guagua/improve/hadoop/ziliao/11_input/inputtable", "data/table-out"};

        Configuration conf = new Configuration();
//        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");

        Job job = Job.getInstance(conf);

        job.setJarByClass(JoinDriver.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        job.setGroupingComparatorClass(TableGroupingComparator.class);

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
