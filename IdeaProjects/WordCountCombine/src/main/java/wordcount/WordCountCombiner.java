package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, IntWritable,Text, IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
        int sum = 0;//初始一个计数器

        for(IntWritable value:values){
            sum += value.get();
        }
        //System.out.println("key:"+key+"   value:"+sum);
        context.write(key,new IntWritable(sum));
    }
}
