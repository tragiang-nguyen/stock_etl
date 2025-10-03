import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class StockClean {

  public static class StockCleanMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text keyOut = new Text();
    private Text valueOut = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      if (line.startsWith("timestamp")) return; // Skip header
      String[] fields = line.split(",");
      if (fields.length == 6) {
        String tstamp = fields[0];
        String cleanedLine = tstamp + "," + fields[1] + "," + fields[2] + "," + fields[3] + "," + fields[4] + "," + fields[5];
        keyOut.set(tstamp);
        valueOut.set(cleanedLine);
        context.write(keyOut, valueOut);
      }
    }
  }

  public static class StockAvgReducer extends Reducer<Text, Text, Text, NullWritable> {
    private Text outputLine = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // Pass-through first value (no aggregation, keep all columns)
      for (Text value : values) {
        outputLine.set(value.toString());
        context.write(outputLine, NullWritable.get());
        break; // Only first (assuming one per timestamp)
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: stockclean <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "Stock Clean");
    job.setJarByClass(StockClean.class);
    job.setMapperClass(StockCleanMapper.class);
    job.setReducerClass(StockAvgReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}