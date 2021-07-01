package hadoop;
/*import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Reducer;*/

import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.StringTokenizer;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Hashtags {

    public static class TokenizerMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Pattern hashTagPattern = Pattern.compile("#(\\w+)");
            String temp = value.toString();
            int totalMaches = 0;
            try {
            JsonParser jsonParser = new JsonParser();
            Matcher matcher = hashTagPattern.matcher(
            jsonParser.parse(temp)
            .getAsJsonObject().get("data")
            .getAsJsonObject().get("text")
            .getAsString());
            while(matcher.find()){
                context.write(new Text(matcher.group(1).toLowerCase()),one);
                totalMaches++;
            }
            } catch (Exception e) {
                //e.printStackTrace();
            }

            if (totalMaches == 0) {
                System.out.println("Total Maches were zero for line : "+ temp);
            }


            /*String temp = value.toString();
            int counter = 0;
            for( int i=0; i<temp.length(); i++ ) {
                if( temp.charAt(i) == ';' ){
                    counter++;
                }
            }
            if (counter==3){
                String[] s = temp.split(";");
                try{
                    int timelength = s[0].length();
                    long timestamp = Long.parseLong(s[0]);
                    Date dt = new Date(timestamp);
                    int hour = dt.getHours();
    
                    if(hour ==2) {
                        String tweet = s[2];
                        Matcher m = Pattern.compile("(#\\w+)\\b",Pattern.CASE_INSENSITIVE).matcher(s[2]);
                        while(m.find()){
                            context.write(new Text(m.group(1).toLowerCase()),one);
                        }
                    }
                }
                catch(NumberFormatException e){}
                }*/
    
        }
    }

    public static class IntSumReducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, Text> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)

        throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum+=value.get();
            }

            result.set(sum);
            context.write(new Text(key.toString()), new Text(sum+""));
            System.out.println("Tag and Value : "+key.toString()+ " "+sum);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.map.java.opts","-Dfile.encoding=UTF-8");

        Job job = new Job(conf);
        //job.setJarByClass(Barplot.class);
        job.setJarByClass(Hashtags.class);
        job.setJobName("Hashtag");
         
        /*FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);*/

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(MultiLineInputFormat.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        //job.setNumReduceTasks(3);
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //Path outputPath = new Path(output);
        //FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        //FileOutputFormat.setOutputPath(job, outputPath);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
       // outputPath.getFileSystem(conf).delete(outputPath,true);
        job.waitForCompletion(true);

        if(job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful");           
        }
    }

}


/**
 * Reads records that are delimited by a specific begin/end tag.
 */
class MultiLineInputFormat extends TextInputFormat {


    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        try {
            return new MultiLineRecordReader((FileSplit) split, context.getConfiguration());
        } catch (IOException ioe) {
            
            return null;
        }
    }

    /**
     * MultiLineRecordReader class to read through a given text document to output records containing multiple
     * lines as a single line
     *
     */
    public static class MultiLineRecordReader extends RecordReader<LongWritable, Text> {

        private final long start;
        private final long end;
        private final FSDataInputStream fsin;
        private final DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable currentKey;
        private Text currentValue;

        public MultiLineRecordReader(FileSplit split, Configuration conf) throws IOException {

            // open the file and seek to the start of the split
            start = split.getStart();
            end = start + split.getLength();
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(conf);
            fsin = fs.open(split.getPath());
            fsin.seek(start);
        }

        private boolean next(LongWritable key, Text value) throws IOException {
            if (fsin.getPos() < end) {
                try {
                    if(readUntilEnd()) {
                        key.set(fsin.getPos());
                        value.set(buffer.getData(), 0, buffer.getLength());
                        return true;
                    }
                } finally {
                    buffer.reset();
                }
            }
            return false;
        }

        @Override
        public void close() throws IOException {
            Closeables.closeQuietly(fsin);
        }

        @Override
        public float getProgress() throws IOException {
            return (fsin.getPos() - start) / (float) (end - start);
        }

        private boolean readUntilEnd() throws IOException {
            boolean insideColumn = false;
            byte[] delimiterBytes = new String("\"").getBytes("utf-8");
            byte[] newLineBytes = new String("\n").getBytes("utf-8");

            while (true) {
                int b = fsin.read();

                // end of file:
                if (b == -1) return false;

                // We encountered a Double Quote
                if(b == delimiterBytes[0]) {
                    if(!insideColumn)
                        insideColumn = true;
                    else
                        insideColumn = false;
                }

                // If we encounter a new line and we are not inside a columnt, it means end of record.
                if(b == newLineBytes[0] && !insideColumn) return true;

                // save to buffer:
                buffer.write(b);

                // see if we've passed the stop point:
                if (fsin.getPos() >= end) {
                    if(buffer.getLength() > 0) // If buffer has some data, then return true
                        return true;
                    else
                        return false;
                }
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return currentKey;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return currentValue;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            currentKey = new LongWritable();
            currentValue = new Text();
            return next(currentKey, currentValue);
        }
    }
}