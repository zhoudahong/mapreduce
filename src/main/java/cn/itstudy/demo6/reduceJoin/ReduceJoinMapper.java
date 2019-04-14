package cn.itstudy.demo6.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * author: DahongZhou
 * Date:
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //判断数据从哪个文件来

        //获取文件的切片
        FileSplit inputSplit = (FileSplit) context.getInputSplit();

        //获取文件名
        String name = inputSplit.getPath().getName();
        String line = value.toString();
        //拿到的是商品表的数据
        //if ("orders.txt".equals(name)) {
        if (line.startsWith("p")) {
            String[] split = line.split(",");
            if (split != null & split.length > 0) {
                text.set(split[0]);
                context.write(text, value);
            }
        } else {//拿到的是订单表的数据
            String[] split = line.split(",");
            if (split != null & split.length > 0) {
                text.set(split[2]);
                context.write(text, value);
            }
        }
    }
}
