package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/* 步骤1：确定输入键值对[K1,V1]的数据类型为[LongWritable,Text]，输出键值对 [K2,V2]的数据类型为[Text,IntWritable] */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private long startTime;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 在 map 阶段开始时，启动计时器
        startTime = System.currentTimeMillis();
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 在 map 阶段结束时，计算执行时间
        long endTime = System.currentTimeMillis();
        long mapTime = endTime - startTime;

        // 将执行时间记录到计数器中
        Counter mapPhaseTimeCounter = context.getCounter("MAP_PHASE", "Time");
        mapPhaseTimeCounter.increment(mapTime); // 增加 Map 阶段的执行时间
    }
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        /* 步骤2：编写处理逻辑将[K1,V1]转换为[K2,V2]并输出 */
        // 以空格作为分隔符拆分成单词
        String[] datas = value.toString().split(" ");
        for (String data : datas) {
            // 输出分词结果
            context.write(new Text(data), new IntWritable(1));
        }
    }
}