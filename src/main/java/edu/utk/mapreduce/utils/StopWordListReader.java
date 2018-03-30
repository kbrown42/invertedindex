package edu.utk.mapreduce.utils;

import com.google.common.base.Charsets;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class StopWordListReader {

    public Set<String> set;

    public StopWordListReader() throws IOException {
        set = new HashSet<>();
        InputStream stream = read();
        InputStreamReader reader = new InputStreamReader(stream, Charsets.UTF_8);
        new BufferedReader(reader).lines()
                                    .map(String::trim)
                                    .forEach(set::add);
    }


    private InputStream read(){
        ClassLoader cl = getClass().getClassLoader();
        return cl.getResourceAsStream("stopwords.txt");
    }

    public Set<String> getSet(){
        return this.set;
    }
}
