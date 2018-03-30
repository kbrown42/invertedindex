package edu.utk.mapreduce.reducers;

import edu.utk.mapreduce.counters.WordCounters;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.log4j.Logger;

public class IntSumReducer
        extends Reducer <Text, IntWritable, Text, IntWritable> {

    private Logger logger = Logger.getLogger(IntSumReducer.class);

    private IntWritable result = new IntWritable();


    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable val: values){
            sum += val.get();
        }

        result.set(sum);
        context.write(key, result);

        context.getCounter(WordCounters.Counter.UNIQUE).increment(1);
        context.getCounter(WordCounters.Counter.TOTAL).increment((long)sum);
    }
}
