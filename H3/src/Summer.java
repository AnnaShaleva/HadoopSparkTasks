import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class Summer extends Reducer<Text, MapWritable, Text, MapWritable> {
  public void reduce(Text key, Iterable<MapWritable> values, Context context)
      throws IOException, InterruptedException {
        MapWritable result = new MapWritable();
        for (MapWritable nextFreq : values) {
            nextFreq.forEach((Writable k, Writable v) -> result.merge(k, v, (Writable oldVal, Writable newVal) -> (oldVal == null) ? newVal : new IntWritable(((IntWritable)oldVal).get() + ((IntWritable)newVal).get())));
        }
        AtomicInteger maxFrequency = new AtomicInteger(0); // Java's closures are so pretty, yeah
        result.forEach((Writable k, Writable v) -> { if (((IntWritable)v).get() > maxFrequency.get()) maxFrequency.set(((IntWritable)v).get()); });
        MapWritable mostFrequentWords = new MapWritable();
        result.forEach((Writable k, Writable v) -> { if (((IntWritable)v).get() == maxFrequency.get()) mostFrequentWords.put(k, v); });
        context.write(key, mostFrequentWords);
  }
}
