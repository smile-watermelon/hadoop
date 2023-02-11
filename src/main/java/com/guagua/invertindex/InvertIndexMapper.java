package com.guagua.invertindex;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/11 14:48
 * @describe
 */
public class InvertIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    String fileName;
    private Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        Path path = inputSplit.getPath();
        fileName = path.getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split(" ");
        for (String word : fields) {
            k.set(word + "--" + fileName);
            context.write(k, v);
        }
    }
}
