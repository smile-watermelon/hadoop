package com.guagua.mapjoin;

import com.guagua.reducejoin.JoinMapper;
import com.guagua.reducejoin.JoinReducer;
import com.guagua.reducejoin.TableBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author guagua
 * @date 2023/2/10 12:04
 * @describe
 */
public class MapJoinDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        args = new String[]{"input/inputtable2", "out/mapjoin-out"};

        Configuration conf = new Configuration();
//        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");

        Job job = Job.getInstance(conf);

        job.setJarByClass(MapJoinDriver.class);

        job.setMapperClass(MapJoinMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new URI("/Users/guagua/improve/hadoop/ziliao/11_input/inputtable/pd.txt"));

        // 只需要map阶段，不需要reduce阶段
        job.setNumReduceTasks(0);

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
