package cn.itstudy.demo2.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * author: DahongZhou
 * Date:
 */
public class SortMapper extends Mapper<LongWritable, Text, PairSort, Text> {
    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        PairSort pairSort = new PairSort();
        String line = value.toString();

        String[] split = line.split("\t");
        pairSort.setFirst(split[0]);
        pairSort.setSecond(Integer.parseInt(split[1]));

        context.write(pairSort, value);
    }
}
