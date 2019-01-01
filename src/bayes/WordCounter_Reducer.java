package bayes;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCounter_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable(); //记录词的个数
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int word_sum = 0;
		for (IntWritable val : values) {
			word_sum += val.get();
		}
		result.set(word_sum);
		context.write(key, result);
	}

}
