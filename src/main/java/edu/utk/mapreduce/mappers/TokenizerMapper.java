package edu.utk.mapreduce.mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import edu.utk.mapreduce.customtypes.DocPosWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import edu.utk.mapreduce.counters.WordCounters;

public class TokenizerMapper extends
        Mapper<LongWritable, Text, Text, DocPosWritable> {



    private Logger logger = Logger.getLogger(TokenizerMapper.class);
    private Text word = new Text();
//    private DocPosWritable pos = new DocPosWritable();


    @Override
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
        FileSplit fs =  (FileSplit)context.getInputSplit();

        String fileName = fs.getPath().getName();


        StringTokenizer itr = new StringTokenizer(value.toString());

        while(itr.hasMoreTokens()) {
            String token = itr.nextToken();
            token = token.trim();
            word.set(token);

            DocPosWritable pos = new DocPosWritable();
            pos.setFileName(fileName);
            pos.setOffset(key.get());


            context.write(word, pos);
        }

    }
}
