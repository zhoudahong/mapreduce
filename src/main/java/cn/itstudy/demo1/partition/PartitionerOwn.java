package cn.itstudy.demo1.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * author: DahongZhou
 * Date:
 */
public class PartitionerOwn extends Partitioner<Text, NullWritable> {
    /**
     * 自定义如何分区
     *
     * @param text         key2那一行数据
     * @param nullWritable value2
     * @param i            reduceTask的数量
     * @return
     */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        String[] split = text.toString().split("\t");
        String gameResult = split[5];

        if (null != gameResult & "" != gameResult) {
            if (Integer.parseInt(gameResult) > 15) {
                //判断如果开奖结果大于15，那么去0分区
                return 0;
            } else {
                //开奖结果小于15，去1分区
                return 1;
            }
        }
        return 0;

    }


}
