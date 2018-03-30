package edu.utk.mapreduce;

import edu.utk.mapreduce.mappers.FrequencyMapper;
import edu.utk.mapreduce.mappers.ThresholdMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class StopWordsDriver extends Configured implements Tool{

    public int wordCount;

    public StopWordsDriver(int wc){
        this.wordCount = wc;
    }

    @Override
    public int run(String[] args) throws Exception {
        // Create job configuration
        Configuration conf = new Configuration();

        conf.setInt("totalCount", this.wordCount);
        conf.setDouble("frequencyThreshold", 0.0017);

        Job job = Job.getInstance(conf, "Word Count");

        job.setJarByClass(getClass());

        // Set input and output paths
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Create configurations for each mapper
        Configuration freqConf = new Configuration(false);
        Configuration thresholdConf = new Configuration(false);

        ChainMapper.addMapper(job, FrequencyMapper.class, Text.class,
                IntWritable.class, Text.class, FloatWritable.class, freqConf);

        ChainMapper.addMapper(job, ThresholdMapper.class, Text.class,
                FloatWritable.class, Text.class, NullWritable.class, thresholdConf);

        job.setReducerClass(Reducer.class);

        int exitCode = job.waitForCompletion(true) ? 0 : 1;
        return exitCode;
    }

}
