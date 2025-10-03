import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class StockCleanMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        context.getCounter("Debug", "ProcessedLines").increment(1);
        if (line.startsWith("timestamp") || line.isEmpty()) {
            return;
        }
        String[] parts = line.split(",");
        if (parts.length >= 6 && !parts[0].isEmpty() && !parts[4].isEmpty()) {
            try {
                float close = Float.parseFloat(parts[4].trim());
                context.getCounter("Debug", "ValidLines").increment(1);
                context.write(new Text(parts[0].trim()), new FloatWritable(close));
            } catch (NumberFormatException e) {
                context.getCounter("Debug", "InvalidNumberFormat").increment(1);
            } catch (ArrayIndexOutOfBoundsException e) {
                context.getCounter("Debug", "InvalidLineFormat").increment(1);
            }
        } else {
            context.getCounter("Debug", "InvalidLineLength").increment(1);
        }
    }
}