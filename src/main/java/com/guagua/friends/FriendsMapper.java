package com.guagua.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/11 17:16
 * @describe
 */
public class FriendsMapper extends Mapper<LongWritable, Text, RelationBean, RelationBean> {

    RelationBean k = new RelationBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] relation = line.split(":");


        k.setId(relation[0]);
        k.setFriends(relation[1]);

        context.write(k, k);
    }
}
