package edu.utk.mapreduce.exceptions;

import java.io.IOException;

public class ZeroTotalWordCount extends IOException  {

    public ZeroTotalWordCount() {}

    public ZeroTotalWordCount(String msg) {
        super(msg);
    }

}
