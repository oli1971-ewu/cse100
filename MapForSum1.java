import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapForSum extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value , Context context) throws IOException, InterruptedException{
		String content =value.toString();
		StringTokenizer line = new StringTokenizer(content,"\n");
		Text ouputKey;
		while(line.hasMoreTokens()){
			String[] words = line.nextToken().split(",");
			Text outputKey = new Text(words[2]);
			String val = words[3];
			int salary = Integer.parseInt(val);
			IntWritable outputValue=  new IntWritable(salary);
			context.write(outputKey, outputValue);
			
			
				
			}
		}
	}

