package com.guagua.friends;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/11 17:25
 * @describe
 */
public class RelationBean implements Writable {

    private String id;

    private String friends;

    public RelationBean() {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(friends);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        friends = in.readUTF();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getFriends() {
        return friends;
    }

    public void setFriends(String friends) {
        this.friends = friends;
    }

    @Override
    public String toString() {
        return id + "\t" + friends;
    }
}
