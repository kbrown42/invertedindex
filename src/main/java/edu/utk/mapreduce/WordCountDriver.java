package edu.utk.mapreduce;

import edu.utk.mapreduce.counters.WordCounters;
import edu.utk.mapreduce.customtypes.DocPosWritable;
import edu.utk.mapreduce.mappers.LowerCaseMapper;
import edu.utk.mapreduce.mappers.RegExMapper;
import edu.utk.mapreduce.mappers.TokenizerMapper;
import edu.utk.mapreduce.mappers.CountMapper;
import edu.utk.mapreduce.reducers.IntSumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


public class WordCountDriver extends Configured implements Tool {

    public int wordCount;

    @Override
    public int run(String[] args) throws Exception {
        // Create job configuration
        Configuration conf = new Configuration();

        // Create configurations for each map
        Configuration tokenizerConf = new Configuration(false);
        Configuration counterConf = new Configuration(false);
        Configuration lowerCaseConf = new Configuration(false);
        Configuration regexConf = new Configuration(false);

        Job job = Job.getInstance(conf, "Word Count");

        job.setJarByClass(getClass());

        // Add mappers to chain mapper
        ChainMapper.addMapper(job, TokenizerMapper.class, Object.class,
                Text.class, Text.class, DocPosWritable.class, tokenizerConf);

        ChainMapper.addMapper(job, LowerCaseMapper.class, Text.class,
                IntWritable.class, Text.class, IntWritable.class, lowerCaseConf);

        ChainMapper.addMapper(job, RegExMapper.class, Text.class,
                IntWritable.class, Text.class, IntWritable.class, regexConf);

        ChainMapper.addMapper(job, CountMapper.class, Text.class,
                DocPosWritable.class, Text.class, IntWritable.class, counterConf);


        // Set combiner and reducer classes
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        // Set output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int exitCode = job.waitForCompletion(true) ? 0 : 1;

        if (exitCode == 0){
            Counter c = job.getCounters().findCounter(WordCounters.Counter.TOTAL);
            this.wordCount = (int)c.getValue();
        }

        return exitCode;

    }
}
