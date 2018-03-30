package edu.utk.mapreduce.customtypes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DocPosWritable implements WritableComparable<DocPosWritable> {
    private Text fileName;
    private LongWritable offset;

    public DocPosWritable(){
        this.fileName = new Text();
        this.offset = new LongWritable();
    }

    public DocPosWritable(DocPosWritable o) {
        this();
        this.setFileName(o.getFileName().toString());
        this.setOffset(o.getOffset().get());
    }

//    public DocPosWritable(String filename, long offset){
//
//        this.fileName.set(filename);
//        this.offset.set(offset);
//    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.fileName.write(dataOutput);
        this.offset.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.fileName.readFields(dataInput);
        this.offset.readFields(dataInput);
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("<");
        sb.append(this.fileName.toString());
        sb.append(",");
        sb.append(this.offset.toString());
        sb.append(">");
        return sb.toString();
    }

    public Text getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName.set(fileName);
    }

    public LongWritable getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset.set(offset);
    }

    @Override
    public int compareTo(DocPosWritable o) {
        int result;
        int fileComp;
        int intComp;

        String myFile = this.fileName.toString();
        String otherFile = o.getFileName().toString();

        Long myOffset = this.offset.get();
        Long otherOffset = o.offset.get();

        // compare files
        fileComp = myFile.compareTo(otherFile);

        // It's in the same file so compare offset
        if (fileComp == 0) {
            intComp = Long.compare(myOffset, otherOffset);
            result = intComp;
        } else {
            result = fileComp;
        }

        return result;
    }
}
