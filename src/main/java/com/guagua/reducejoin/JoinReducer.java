package com.guagua.reducejoin;

import com.guagua.group.OrderBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author guagua
 * @date 2023/2/10 10:33
 * @describe
 */
public class JoinReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        List<TableBean> orders = new ArrayList<>();
        TableBean pdBean = new TableBean();

        for (TableBean value : values) {
            if(value.getFlag().equals("order")) {

                TableBean tmp = new TableBean();
                try {
                    // value 保存的是tableBean对象的引用，不进行拷贝，添加的只是最后
                    BeanUtils.copyProperties(tmp, value);
                    orders.add(tmp);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        for (TableBean order : orders) {
            order.setProductName(pdBean.getProductName());
            context.write(order, NullWritable.get());
        }
    }
}
