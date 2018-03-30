package edu.utk.mapreduce.mappers;

import edu.utk.mapreduce.counters.WordCounters;
import edu.utk.mapreduce.customtypes.DocPosWritable;
import edu.utk.mapreduce.utils.StopWordListReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class StopWordFilterMapper
        extends Mapper<Text, DocPosWritable, Text, DocPosWritable> {

    private Set<String> hs;


    @Override
    public void map(Text key, DocPosWritable value, Context context)
            throws IOException, InterruptedException {

        this.hs = new StopWordListReader().getSet();

        if (!this.hs.contains(key.toString())){
            context.write(key, value);
        } else {
            Counter c = context.getCounter(
                                WordCounters.Counter.STOPWORDSFILTERED);
            c.increment(1);
        }



    }
}
