package edu.utk.mapreduce.mappers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ThresholdMapper
        extends Mapper<Text, FloatWritable, Text, NullWritable> {
    private Logger logger = Logger.getLogger(ThresholdMapper.class);

    @Override
    public void map(Text key, FloatWritable value, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        double threshold = conf.getDouble("frequencyThreshold", 0.0);

        if (threshold == 0.0) {
            logger.warn("No frequency threshold found in configuration.");
            context.write(key, NullWritable.get());
        } else {
            if (value.get() >= threshold){
                context.write(key, NullWritable.get());
            }
        }



    }
}
