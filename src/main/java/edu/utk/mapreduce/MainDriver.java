package edu.utk.mapreduce;

import edu.utk.mapreduce.utils.StopWordListReader;
import org.apache.hadoop.util.ToolRunner;

public class MainDriver {

    public static void main(String[] args) throws Exception {
        // Compute word counts
//        WordCountDriver wcDriver = new WordCountDriver();
//        int countExitCode = ToolRunner.run(wcDriver, args);
//
//        String[] stopWordsArgs = new String[2];
//
//        stopWordsArgs[0] = args[1];
//        stopWordsArgs[1] = args[1] + "/stopWords";
//
//        int stopWordsExitCode = 1;
//        if (countExitCode == 0) {
//            StopWordsDriver swDriver = new StopWordsDriver(wcDriver.wordCount);
//            stopWordsExitCode = ToolRunner.run(swDriver,
//                                                stopWordsArgs);
//        }
//
//        System.exit(stopWordsExitCode);

        IndexDriver idxDriver = new IndexDriver();
        int exitCode = ToolRunner.run(idxDriver, args);


        System.exit(exitCode);
    }
}
