import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class StockAvgReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;
        for (FloatWritable val : values) {
            sum += val.get();
            count++;
        }
        context.write(key, new FloatWritable(sum / count));
    }
}