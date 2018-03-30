package edu.utk.mapreduce;

import edu.utk.mapreduce.customtypes.DocPosWritable;
import edu.utk.mapreduce.mappers.LowerCaseMapper;
import edu.utk.mapreduce.mappers.RegExMapper;
import edu.utk.mapreduce.mappers.StopWordFilterMapper;
import edu.utk.mapreduce.mappers.TokenizerMapper;
import edu.utk.mapreduce.reducers.IndexReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class IndexDriver extends Configured implements Tool {

    // Needs location of stopwords file
    // Tokenizer -> StopWordsFilter -> Output
    @Override
    public int run(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Configuration tokenConf = new Configuration(false);
        Configuration stopFilterConf = new Configuration(false);
        Configuration lowerCaseConf = new Configuration(false);
        Configuration regexConf = new Configuration(false);


        ChainMapper.addMapper(job, TokenizerMapper.class,
                LongWritable.class, Text.class, Text.class,
                DocPosWritable.class, tokenConf);

        ChainMapper.addMapper(job, LowerCaseMapper.class, Text.class,
                DocPosWritable.class, Text.class, DocPosWritable.class, lowerCaseConf);

        ChainMapper.addMapper(job, RegExMapper.class, Text.class,
                DocPosWritable.class, Text.class, DocPosWritable.class, regexConf);


        ChainMapper.addMapper(job, StopWordFilterMapper.class,
                Text.class, DocPosWritable.class, Text.class,
                DocPosWritable.class, stopFilterConf);

        job.setReducerClass(IndexReducer.class);

        int exitCode = job.waitForCompletion(true) ? 0 : 1;

        return exitCode;
    }

}
