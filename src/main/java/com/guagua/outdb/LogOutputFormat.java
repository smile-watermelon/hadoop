package com.guagua.outdb;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @author guagua
 * @date 2023/2/9 23:05
 * @describe
 */
public class LogOutputFormat extends DBOutputFormat {

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException {
//        LogRecordWriter logRecordWriter = null;
//        try {
//            logRecordWriter = new LogRecordWriter(context);
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return  logRecordWriter;
        return null;
    }
}
