package edu.utk.mapreduce.mappers;

import edu.utk.mapreduce.customtypes.DocPosWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LowerCaseMapper extends
        Mapper<Text, DocPosWritable, Text, DocPosWritable> {

    private Text word = new Text();

    @Override
    public void map(Text key, DocPosWritable value, Context context)
            throws IOException, InterruptedException {
        // Convert text to lowercase and write to context
        String token = key.toString().toLowerCase();
        word.set(token);

        context.write(word, value);

    }
}
