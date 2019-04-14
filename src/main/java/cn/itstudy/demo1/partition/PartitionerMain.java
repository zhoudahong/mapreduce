package cn.itstudy.demo1.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * author: DahongZhou
 * Date:
 */
public class PartitionerMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //获取job类，组装MR任务
        Job job = Job.getInstance(super.getConf(), PartitionerMain.class.getSimpleName());

        job.setJarByClass(PartitionerMain.class);//打包运行必须的

        //第一步：设置
        job.setInputFormatClass(TextInputFormat.class);
        //分区案例，不能本地模式运行，使用file：///也会报错，只能打包
        TextInputFormat.addInputPath(job, new Path(args[0]));

        //第二步：自定义Map逻辑
        job.setMapperClass(PartitionerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setPartitionerClass(PartitionerOwn.class);//第三步：用自定义分区类来完成分区

        //省略第四、五、六步

        //第七步：自定义Reduce逻辑
        job.setReducerClass(PartitionerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置reduceTask个数
        job.setNumReduceTasks(2);

        //第八步：输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交任务
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new PartitionerMain(), args);
        System.exit(run);
    }
}
