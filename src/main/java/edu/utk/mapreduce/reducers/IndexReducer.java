package edu.utk.mapreduce.reducers;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import edu.utk.mapreduce.customtypes.DocPosWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IndexReducer
        extends Reducer<Text, DocPosWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<DocPosWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        List<DocPosWritable> lst = new ArrayList<>();

        for(DocPosWritable val: values){
            lst.add(new DocPosWritable(val));
        }
        Collections.sort(lst);

        StringBuilder sb = new StringBuilder();

        for (DocPosWritable val : lst) {
            sb.append( val.toString());
            sb.append("\t");
        }

        context.write(key, new Text(sb.toString()));

    }
}
