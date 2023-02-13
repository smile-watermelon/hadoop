package com.guagua.friends;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.util.Arrays;

/**
 * @author guagua
 * @date 2023/2/11 17:22
 * @describe
 */
public class FriendsGroupingComparator extends WritableComparator {


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        RelationBean bean = (RelationBean) a;
        RelationBean bean1 = (RelationBean) b;

        String[] friends1 = bean.getFriends().split(",");
        String[] friends = bean1.getFriends().split(",");

        String commonFriends = getCommonFriends(friends1, friends);

        if (StringUtils.isNotEmpty(commonFriends)) {
            return 0;
        } else {
            return -1;
        }
    }

    private String getCommonFriends(String[] friends1, String[] friends) {
        if (null == friends1) {
            return "";
        }
        if (null == friends) {
            return "";
        }
        Arrays.sort(friends1);
        Arrays.sort(friends);

        int f1Len = friends1.length;
        int flen = friends.length;
        int len = Integer.min(f1Len, flen);

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < len; i++) {
            if (friends[i].equals(friends1[i])) {
                builder.append(friends[i]).append(",");
            }
        }
        String result = builder.toString();
        if (!StringUtils.isNotEmpty(result)) {
            return "";
        }
        return result.substring(0, result.length() - 1);
    }


}
