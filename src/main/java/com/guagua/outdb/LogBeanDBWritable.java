package com.guagua.outdb;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author guagua
 * @date 2023/2/9 22:09
 * @describe
 */
public class LogBeanDBWritable implements Writable, DBWritable {

    private int id;
    private String url;

    public LogBeanDBWritable(int id, String url) {
        this.id = id;
        this.url = url;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.write(url.getBytes());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.url = Text.readString(in);
    }



    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1, this.id);
        statement.setString(2, this.url);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getInt(1);
        this.url = resultSet.getString(2);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "LogBeanDBWritable{" +
                "id=" + id +
                ", url='" + url + '\'' +
                '}';
    }
}
