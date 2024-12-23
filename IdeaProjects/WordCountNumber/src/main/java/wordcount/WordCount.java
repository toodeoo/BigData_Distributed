package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        long totalSize = (long) (2173565481L);
        long splitSize = totalSize / Integer.parseInt(args[2]);             // 每个 Map 的分片大小
        configuration.setLong("mapreduce.input.fileinputformat.split.minsize", splitSize);
        configuration.setLong("mapreduce.input.fileinputformat.split.maxsize", splitSize);
        // 设置 Map 任务数量
        configuration.setInt("mapreduce.job.maps", Integer.parseInt(args[2]));

        // 设置 Reduce 任务数量
        configuration.setInt("mapreduce.job.reduces", Integer.parseInt(args[3]));

        Job job = Job.getInstance(configuration, getClass().getSimpleName());
        // 设置程序的类名
        job.setJarByClass(getClass());

        // 设置数据的输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置map和reduce方法
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置map方法的输出键值对数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置reduce方法的输出键值对数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        long startTime = System.currentTimeMillis();

        // 启动作业并等待完成
        boolean success = job.waitForCompletion(false);
        long endTime = System.currentTimeMillis();

        if (success) {
            Counters counters = job.getCounters();

            // 获取 Map 阶段执行时间
            Counter mapTimeCounter = counters.findCounter("MAP_PHASE", "Time");
            long mapTime = mapTimeCounter.getValue();

            // 获取 Reduce 阶段执行时间
            Counter reduceTimeCounter = counters.findCounter("REDUCE_PHASE", "Time");
            long reduceTime = reduceTimeCounter.getValue();
            System.out.println("Map Phase Time: " + mapTime + " milliseconds." + "Reduce Phase Time: " + reduceTime + " milliseconds.");
            System.out.println("Total execution time: " + (endTime-startTime) + " milliseconds.");
        }

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        /* 步骤2：运行作业 */
        int exitCode = ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);
    }
}