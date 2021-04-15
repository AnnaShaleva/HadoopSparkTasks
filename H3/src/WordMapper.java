import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class WordMapper extends Mapper<Object, Text, Text, MapWritable> {
  public void run(Context ctx) throws IOException, InterruptedException {
      setup(ctx);
      try {
          Pattern pattern = Pattern.compile("[.?!;]");
          String remnant = "";

          while (ctx.nextKeyValue()) {
              String line = ctx.getCurrentValue().toString();
              Matcher matcher = pattern.matcher(line);
              Integer startIndex = 0;
              while (matcher.find()) {
                    map(ctx.getCurrentKey(), new Text(remnant+ " " + line.substring(startIndex, matcher.start(0))), ctx);
                    startIndex = matcher.end(0);
                    remnant = "";
              }
              remnant += " " + line.substring(startIndex);
          }
          map(ctx.getCurrentKey(), new Text(remnant), ctx);
      } finally {
          cleanup(ctx);
      }
  }

  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
        Analyzer analyzer = new RussianAnalyzer();
        TokenStream tokenStream = analyzer.tokenStream("contents", new StringReader(value.toString()));
        tokenStream.reset();
        CharTermAttribute term = tokenStream.addAttribute(CharTermAttribute.class);
        String curr = "";
        while(tokenStream.incrementToken()) {
            if (curr == "") {
               curr = term.toString();
               continue;
            }
            String next = term.toString();
            MapWritable nextFreq = new MapWritable();
            nextFreq.put(new Text(next), new IntWritable(1));
            context.write(new Text(curr), nextFreq);

            curr = next;
        }
  }
}
