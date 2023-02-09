package com.guagua.outformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/9 21:36
 * @describe
 */
public class MyFilterRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fos;
    FSDataOutputStream fosOther;


    public MyFilterRecordWriter(TaskAttemptContext job, String output1, String output2) {
        // 获取文件系统
        FileSystem fs = null;
        try {
            fs = FileSystem.get(job.getConfiguration());
            fos = fs.create(new Path(output1));
            fosOther = fs.create(new Path(output2));

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String str = key.toString();
        if (str.contains("atguigu")) {
            fos.write(str.getBytes());
        } else {
            fosOther.write(str.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fosOther);
    }
}
