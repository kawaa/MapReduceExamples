package com.hakunamapdata.examples;

import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndex extends Configured implements Tool {

    public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {

            /*
             * FileSplit for the input file provides access to the file's path.
             */
            Path path = ((FileSplit) context.getInputSplit()).getPath();
            String tokenPlace = path.getName() + "@" + key.get();
            
            String lowerCasedLine = value.toString().toLowerCase();

            /* 
             * Split the line into words. For each word on the line,
             * emit an output record that has the word as the key and
             * the location of the word as the value. 
             */
            for (String token : lowerCasedLine.split("\\s")) {
                if (token.length() > 0) {
                    context.write(new Text(token), new Text(tokenPlace));
                }
            }
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        
        private static final String SEP = ",";

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String valueList = "";
            boolean firstValue = true;
            for (Text value : values) {
                if (!firstValue) {
                    valueList += SEP;
                } else {
                    firstValue = false;
                }
                valueList += value.toString();
            }
            
            context.write(key, new Text(valueList.toString()));
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf("Usage: InvertedIndex <input dir> <output dir>\n");
            return -1;
        }

        Job job = new Job(getConf());
        job.setJarByClass(InvertedIndex.class);
        job.setJobName("Inverted Index");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new InvertedIndex(), args);
        System.exit(exitCode);
    }
}