package hadoop;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.omg.CORBA.StringHolder;

import com.google.common.io.Closeables;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class TestClass {

public static class TokenizerMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {
private final IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Pattern hashTagPattern = Pattern.compile("#(\\w+)");   //hashtag pattern
        String temp = value.toString();
        int totalMaches = 0;
        try {
            if (temp == null || temp.isEmpty()) {
                return;
            }
            JsonParser jsonParser = new JsonParser();
            JsonElement parsed = jsonParser.parse(temp);
            if (parsed != null && parsed.isJsonObject()) {
                parsed = parsed.getAsJsonObject().get("data");
                if (parsed != null && parsed.isJsonObject()) {
                    parsed = parsed.getAsJsonObject().get("text");
                    if (parsed != null) {
                        String parsedTweet = parsed.getAsString();
                        Matcher matcherc = hashTagPattern.matcherc(parsedTweet);
                        while (matcherc.find()) {
                            String foudnValue = matcherc.group(1).toLowerCase();
                            //System.out.println("Trying to write " + foudnValue);
                            context.write(new Text(foudnValue), one);
                            totalMaches++;
                        }
                    }
                }
            }

        } catch (Exception e) {
        }

        if (totalMaches == 0) {
         System.out.println("Total Maches were zero for line : "+ temp);
        }

    }
}

public static class IntSumReducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, Text> {

    private TreeMap<Long, String> tmap2;

    public void setup(Context context) throws IOException, InterruptedException{
        tmap2 = new TreeMap<Long, String>();
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (IntWritable value : values){
            sum += value.get();
        }

        tmap2.put(sum, key.toString());

        if (tmap2.size() > 10){
            tmap2.remove(tmap2.firstKey());
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException{
        for (Map.Entry<Long, String> entry : tmap2.descendingMap().entrySet()){
            context.write(new Text(entry.getValue()), new Text(entry.getKey()+""));
        }
    }
    
      
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.map.java.opts", "-Dfile.encoding=UTF-8");

    Job job = new Job(conf);
    job.setJarByClass(TestClass.class);
    job.setJobName("Hashtag");

    String inputFile =  args[0];
    String OutputFile = args[1];
    if (args.length > 2) {
    inputFile = args[0];
    OutputFile = args[1];
    }

    FileInputFormat.addInputPath(job, new Path(inputFile));
    FileOutputFormat.setOutputPath(job, new Path(OutputFile));

    job.setInputFormatClass(MultiLineInputFormat.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(inputFile));
    FileOutputFormat.setOutputPath(job, new Path(OutputFile));
    job.waitForCompletion(true);

    if (job.isSuccessful()) {
        System.out.println("Job was successful");
    } 
    else if (!job.isSuccessful()) {
        System.out.println("Job was not successful");
    }
}

}

 
class MultiLineInputFormat extends TextInputFormat {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        try {
            return new MultiLineRecordReader((FileSplit) split, context.getConfiguration());
        } 
        catch (IOException ioe) {
            return null;
        }
    }


    public static class MultiLineRecordReader extends RecordReader<LongWritable, Text> {

        private final long startr;
        private final long endr;
        private final FSDataInputStream fsdin;
        private final DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable currentkey;
        private Text currentValue;

        public MultiLineRecordReader(FileSplit splitf, Configuration conf) throws IOException {

            startr = splitf.getStart();
            endr = startr + splitf.getLength();
            Path file = splitf.getPath();
            FileSystem fs = file.getFileSystem(conf);
            fsdin = fs.open(splitf.getPath());
            fsdin.seek(startr);
        }

        private boolean next(LongWritable key, Text value) throws IOException {
            if (fsdin.getPos() < endr) {
                try {
                    if (readUntilEnd()) {
                        key.set(fsdin.getPos());
                        value.set(buffer.getData(), 0, buffer.getLength());
                        return true;
                    }
                }
                finally {
                    buffer.reset();
                }
            }
            return false;
        }

        @Override
        public void close() throws IOException {
            Closeables.closeQuietly(fsdin);
        }

        @Override
        public float getProgress() throws IOException {
            return (fsdin.getPos() - startr) / (float) (endr - startr);
        }

        private boolean readUntilEnd() throws IOException {
            boolean insideColl = false;
            byte[] delimiterB = new String("\"").getBytes("utf-8");
            byte[] newLineB = new String("\n").getBytes("utf-8");

            while (true) {
                int b = fsdin.read();

                if (b == -1)
                    return false;

                if (b == delimiterB[0]) {
                    if (!insideColl)
                        insideColl = true;
                    else
                        insideColl = false;
                }

                if (b == newLineB[0] && !insideColl)
                    return true;

                buffer.write(b);

            
                if (fsdin.getPos() >= endr) {
                    if (buffer.getLength() > 0) 
                        return true;
                    else
                        return false;
                }
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return currentkey;
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
            currentkey=new LongWritable();
            currentValue=new Text();
            return next(currentkey, currentValue);
        }
    }
}