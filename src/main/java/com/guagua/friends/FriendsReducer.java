package com.guagua.friends;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/11 18:05
 * @describe
 */
public class FriendsReducer extends Reducer<RelationBean, RelationBean, Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void reduce(RelationBean key, Iterable<RelationBean> values, Context context) throws IOException, InterruptedException {

        for (RelationBean value : values) {
            k.set(value.toString());
            context.write(k, NullWritable.get());
        }
    }
}
