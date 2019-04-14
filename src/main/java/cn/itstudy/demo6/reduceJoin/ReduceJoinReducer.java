package cn.itstudy.demo6.reduceJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * author: DahongZhou
 * Date:
 * <p>
 * 输出的数据：
 * p0001,小米5,1000,2000          1001,20150710,p0001,2
 * p0002,锤子T1,1000,3000         1002,20150710,p0002,3
 * 1002,20150710,p0003,3
 */
public class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String firstPart = "";
        String secondPart = "";

        for (Text value : values) {
            if (value.toString().startsWith("p")) {
                firstPart = value.toString();
            } else {
                secondPart = value.toString();
            }
        }
        context.write(key, new Text(firstPart + "\t" + secondPart));
    }
}
