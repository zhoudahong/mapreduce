package cn.itstudy.demo2.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * author: DahongZhou
 * Date:
 *
 */
public class MyCombiner extends Reducer<PairSort, Text, PairSort, Text> {
    //输入类型是key2，value2，输出也是key2，value2
    //这样可以减少输出到reduce的key2的数量
    @Override
    protected void reduce(PairSort key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //写规约的逻辑
        for (Text value : values) {
            context.write(key, value);
        }

    }
}
