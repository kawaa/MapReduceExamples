package com.hakunamapdata.examples;

import java.io.IOException;
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
import org.apache.commons.lang3.StringUtils;

public class InvertedIndexStringUtils extends Configured implements Tool {

    public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String fileName = null;
        private Text location = new Text();
        private Text word = new Text();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Path path = ((FileSplit) context.getInputSplit()).getPath();
            fileName = path.getName();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {

            String tokenPlace = fileName + "@" + key.get();

            String lowerCasedLine = StringUtils.lowerCase(value.toString());
            for (String token : StringUtils.split(lowerCasedLine, "\\s")) {
                if (token.length() > 0) {
                    word.set(token);
                    location.set(tokenPlace);
                    context.write(word, location);
                }
            }
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {

        private Text indexValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String valueList = StringUtils.join(values, Utils.COMMA);
            indexValue.set(valueList);
            context.write(key, indexValue);
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.out.printf("Usage: InvertedIndexStringUtils "
                    + "<input dir> <output dir> [<profiler enabled>]\n");
            return -1;
        }

        Configuration conf = getConf();

        if ((args.length == 3) && (Boolean.parseBoolean(args[2]))) {
            Utils.enableProfiling(conf, "0", "0");
        }

        Job job = new Job(conf);
        job.setJarByClass(InvertedIndexStringUtils.class);
        job.setJobName("InvertedIndexStringUtils");

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
        int exitCode = ToolRunner.run(new Configuration(), new InvertedIndexStringUtils(), args);
        System.exit(exitCode);
    }
}