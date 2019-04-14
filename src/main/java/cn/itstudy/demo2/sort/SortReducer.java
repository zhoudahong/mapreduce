package cn.itstudy.demo2.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * author: DahongZhou
 * Date:
 */
public class SortReducer extends Reducer<PairSort, Text, PairSort, Text> {
    @Override
    protected void reduce(PairSort key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, value);
        }
    }
}
