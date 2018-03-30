package edu.utk.mapreduce.mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import edu.utk.mapreduce.customtypes.DocPosWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class CountMapper extends
        Mapper<Text, DocPosWritable, Text, IntWritable> {

    private Text word = new Text();
    private Logger logger = Logger.getLogger(CountMapper.class);
    private final static IntWritable ONE = new IntWritable(1);



    @Override
    public void map(Text key, DocPosWritable value, Context context
    ) throws IOException, InterruptedException {

        word.set(key);
        context.write(word, ONE);
    }

}

