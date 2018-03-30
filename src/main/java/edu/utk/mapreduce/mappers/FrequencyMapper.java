package edu.utk.mapreduce.mappers;

import edu.utk.mapreduce.exceptions.ZeroTotalWordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FrequencyMapper
        extends Mapper<Text, Text, Text, FloatWritable> {

    private FloatWritable result = new FloatWritable();

    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        long total = conf.getInt("totalCount", 0);

        if (total == 0) {
            StringBuilder sb = new StringBuilder();
            sb.append("Could not calculate frequency.");
            sb.append("FrequencyMapper requires 'totalCount in job configuration");

            throw new ZeroTotalWordCount(sb.toString());
        }

        float freq = Integer.parseInt(value.toString()) / (float)total;
        result.set(freq);
        context.write(key, result);
    }
}
