package edu.utk.mapreduce.mappers;

import edu.utk.mapreduce.counters.WordCounters;
import edu.utk.mapreduce.customtypes.DocPosWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.IOException;

public class RegExMapper extends
        Mapper <Text, DocPosWritable, Text, DocPosWritable> {

    private String regex = "[^a-zA-Z]";
    private Text word = new Text();

    private Logger logger = Logger.getLogger(RegExMapper.class);

    @Override
    public void map(Text key, DocPosWritable value, Context context)
            throws IOException, InterruptedException {
        // Remove unwanted punctuation from text key
        // and write to context
        String token = key.toString().replaceAll(regex, "");

        if (token.isEmpty()){
            context.getCounter(WordCounters.Counter.EMPTY).increment(1);
        } else {
            word.set(token);
            context.write(word, value);
        }
    }
}
