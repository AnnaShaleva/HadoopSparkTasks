import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
        Analyzer analyzer = new RussianAnalyzer();
        TokenStream tokenStream = analyzer.tokenStream("contents", new StringReader(value.toString()));
        tokenStream.reset();
        CharTermAttribute term = tokenStream.addAttribute(CharTermAttribute.class);
        while(tokenStream.incrementToken()) {
            context.write(new Text(term.toString()), new IntWritable(1));
        }
  }
}
