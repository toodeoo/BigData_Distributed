package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/* 步骤1：确定输入键值对[K2,List(V2)]的数据类型为[Text, IntWritable]，输出键值对[K3,V3]的数据类型为[Text,IntWritable] */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private long startTime;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 在 reduce 阶段开始时，启动计时器
        startTime = System.currentTimeMillis();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 在 reduce 阶段结束时，计算执行时间
        long endTime = System.currentTimeMillis();
        long reduceTime = endTime - startTime;

        // 将执行时间记录到计数器中
        Counter reducePhaseTimeCounter = context.getCounter("REDUCE_PHASE", "Time");
        reducePhaseTimeCounter.increment(reduceTime); // 增加 Reduce 阶段的执行时间
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        /* 步骤2：编写处理逻辑将[K2,List(V2)]转换为[K3,V3]并输出 */
        int sum = 0;
        // 遍历累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }
        // 输出计数结果
        context.write(key, new IntWritable(sum));
    }
}