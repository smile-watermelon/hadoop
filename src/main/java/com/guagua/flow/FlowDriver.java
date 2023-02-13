package com.guagua.flow;

import com.guagua.wc.WordCountDriver;
import com.guagua.wc.WordCountMapper;
import com.guagua.wc.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
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
public class FlowDriver {

    /**
     * 默认使用HashPartition 根据key 和整型最大值进行 与 运算，保证分区数不超出整型最大值，再根据 reduceTask 进行取余得出数据在那个分区
     *  (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
     *
     *  切片大小计算，默认 minSize = 1,
     *  mapreduce.input.fileinputformat.split.maxsize， 根据配置的 最大切片 参数和 块大小，取最小值，然后取最大值
     *      Math.max(minSize, Math.min(maxSize, blockSize));
     *
     * CombineTextInputFormat 切片的方式：
     *  1、先按照设置的  CombineTextInputFormat.setMaxInputSplitSize(job, 20971520); 切片大小
     *      对文件按照大小进行切片，小于设置的值，切成一片，大于设置的值，按照大小平均切成2片
     *  2、将所有的切片列表挨个和 设置的切面大小进行比较，小于会合并下一个切片共同形成一个切片。
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        args = new String[]{"input/inputflow", "output/flow-output"};
//        1、获取job对象
        Configuration conf = new Configuration();
        // 设置输出文件中，k和v的分隔符
//        conf.set(TextOutputFormat.SEPERATOR, ",");

        Job job = Job.getInstance(conf);
        job.setJobName("流量统计");

//        2、设置jar存储位置
        job.setJarByClass(FlowDriver.class);

//        job.setPartitionerClass(FlowPartitioner.class);
        // 设置一个reduceTask 最终只会输出一个文件
//        job.setNumReduceTasks(5);

//        3、关联mapper 和 reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

//        4、设置mapper 阶段输出数据的key, value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

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
