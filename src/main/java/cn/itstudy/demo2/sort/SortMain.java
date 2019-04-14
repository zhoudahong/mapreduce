package cn.itstudy.demo2.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * author: DahongZhou
 * Date:
 */
public class SortMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "sort");

        job.setJarByClass(SortMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("E:\\黑马程序员\\05_大数据\\深圳大数据6期hadoop\\4、第四天\\资料\\排序\\input"));


        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(PairSort.class);
        job.setMapOutputValueClass(Text.class);


        //设置第五步：规约
        job.setCombinerClass(MyCombiner.class);

        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(PairSort.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("E:\\黑马程序员\\05_大数据\\深圳大数据6期hadoop\\4、第四天\\资料\\排序\\outSort2"));

        //true表示将运行进度等信息及时输出给用户，false的话只是等待作业结束
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;

    }


    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new SortMain(), args);
        System.exit(run);
    }
}
