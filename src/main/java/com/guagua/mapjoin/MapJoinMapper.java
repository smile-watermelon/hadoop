package com.guagua.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

/**
 * @author guagua
 * @date 2023/2/10 12:08
 * @describe
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    HashMap<String, String> pdMap = new HashMap<>();
    Text k = new Text();



    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();
        // 缓存小表
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(path)), StandardCharsets.UTF_8));
        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())) {
            String[] fields = line.split("\t");
            pdMap.put(fields[0], fields[1]);
        }

        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");
        String pid = fields[1];
        String productName = pdMap.get(pid);

        StringBuilder builder = new StringBuilder();
        builder.append(fields[0]);
        builder.append(",");
        builder.append(productName);
        builder.append(",");
        builder.append(fields[2]);

        k.set(builder.toString());

        context.write(k, NullWritable.get());
    }
}
