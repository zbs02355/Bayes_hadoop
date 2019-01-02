package bayes;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCounter_Mapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);
	private String a_word;
	private byte wordLocation = 0; //存放一行单词的个数
	private boolean class_flag;  //不同类别的标志
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		//将每行结果按照默认分隔符进行拆分
		StringTokenizer tokenizer = new StringTokenizer(line);
		//创建正则表达式
		Pattern pattern = Pattern.compile("[\\u4E00-\\u9FA5]{1,}");
		while(tokenizer.hasMoreTokens()) {
			a_word = tokenizer.nextToken();
			//创建一个用于匹配中文的匹配器
			Matcher matcher = pattern.matcher(a_word);
			if(matcher.matches() == true) {
				if(wordLocation == 0) {
					if(a_word.equals("好评")) {
						class_flag = true;
						//good_num += 1;
						word.set(a_word);
						context.write(word, one);
					}
					else if(a_word.equals("差评")) {
						class_flag = false;
						//bad_num += 1;
						word.set(a_word);
						context.write(word, one);
					}
					wordLocation += 1;
				}
				else {
					if (class_flag == true) {
						a_word = "好评_" + a_word;
						//word.set("good_" + a_word);
						word.set(a_word);
						context.write(word, one);
					}
					else if(class_flag == false) {
						a_word = "差评_" + a_word;
						//word.set("bad_" + a_word);
						word.set(a_word);
						context.write(word, one);
					}
				}
			}
		}
		//每执行完一行之后，将个数置0
		wordLocation = 0;
	}

}
