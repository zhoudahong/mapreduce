package cn.itstudy.demo7.mapJoin;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * author: DahongZhou
 * Date:
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Map<String, String> map = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        //获取缓存文件的列表
        URI[] cacheFiles = DistributedCache.getCacheFiles(configuration);

        //获取分布式文件系统
        FileSystem fileSystem = FileSystem.get(cacheFiles[0], configuration);

        //把分布式缓存文件读成一个流
        FSDataInputStream inputStream = fileSystem.open(new Path(cacheFiles[0]));

        //通过bufferedReader来读取输入流
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            //往下读一行
            String[] split = line.split(",");
            //以商品pid为key，以整行内容作为value存到map中
            map.put(split[0], line);
        }

        fileSystem.close();
        IOUtils.closeQuietly(inputStream);
    }


    //读取订单表时，对应map集合中存在的key，添加数据
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] split = line.split(",");
        //从map中获取商品表数据
        String product_line = map.get(split[2]);

        context.write(new Text(product_line + "\t" + value.toString()), NullWritable.get());
    }
}
