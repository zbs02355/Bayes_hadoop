package bayes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NB {
	//创建一个更改模型输出文件的类
	public static class myOutputFormat_class extends org.apache.hadoop.mapreduce.lib.output.TextOutputFormat{
		protected static void setOutputName(JobContext job, String name) {
			job.getConfiguration().set(BASE_OUTPUT_NAME, name);
		}
	}
	//使用MapReduce训练模型
	public static void train_model(String trainFile, String modelFile) throws Exception{
		if((new File(modelFile)).exists()) {
			return;
		}
		else {
			Configuration conf = new Configuration();
			//修改配置文件以适应集群环境
			/*conf.set("fs.defaultFS", "hdfs://192.168.10.100:9000");
			conf.set("hadoop.job.user", "hadoop");
			conf.set("mapreduce.framwork.name", "yarn");
			conf.set("mapreduce.jobtracker.address", "192.168.10.100:9001");
			conf.set("yarn.resourcemanager.hostname", "192.168.10.100");
			conf.set("yarn.resourcemanager.admin.address", "192.168.10.100:8033");
			conf.set("yarn.resourcemanager.address", "192.168.10.100:8032");
			conf.set("yarn.resourcemanager.resource-tracker.address", "192.168.10.100:8036");
			conf.set("yarn.resourcemanager.scheduler.address", "192.168.10.100:8030");*/
			
			Job job = Job.getInstance(conf, "WordCount");
			//job.setJar("wordCount.jar"); //设置运行的jar文件
			//设置主类
			job.setJarByClass(NB.class);
			//设置Mapper类
			job.setMapperClass(WordCounter_Mapper.class);
			//设置Reducer类
			job.setReducerClass(WordCounter_Reducer.class);
			//设置作业合成类
			job.setCombinerClass(WordCounter_Reducer.class);
			//设置输出数据类
			job.setOutputKeyClass(Text.class);
			//设置输出值类
			job.setOutputValueClass(IntWritable.class);
			//设置输出文件名
			job.setOutputFormatClass(myOutputFormat_class.class);
			myOutputFormat_class.setOutputName(job, "2016082065_模型");
			
			FileInputFormat.addInputPath(job, new Path("D:/hadoop_test/input"));
			FileOutputFormat.setOutputPath(job, new Path("D:/hadoop_test/output_model"));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
	//加载模型文件的数据到HashMap中，并计数
	public static HashMap<String, Integer> prior = null; //存放好评、差评的个数
	public static double priorNorm = 0.;  //存放总数目
	public static HashMap<String, Integer> likelihood = null; //用来存放模型中的每一个词语和数量
	public static HashMap<String, Double> likelihoodNorm = null; //存放好评、差评的个数
	public static HashSet<String> V = null; //存放不同类别的词语
	
	public static void loadModel(String modelFilePath) throws Exception{
		if(prior != null && likelihood != null) {
			return;
		}
		//初始化一些变量
		prior = new HashMap<String, Integer>(); //存放好评、差评
		likelihood = new HashMap<String, Integer>(); //用来存放模型中的每一个词语和数量
		likelihoodNorm = new HashMap<String, Double>(); //存放好评、差评的个数
		V = new HashSet<String>(); //存放不同类别的词语
		
		//MapReduce计算出的模型文件是UTF-8编码
		FileInputStream file = new FileInputStream(modelFilePath);
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
		String line = null;  //用于存放一行数据
		//循环读取
		while((line = bufferedReader.readLine()) != null) {
			String word_feature = line.substring(0, line.indexOf("\t"));  
			Integer count = Integer.parseInt(line.substring(line.indexOf("\t") + 1));
			if(word_feature.contains("_")) {
				likelihood.put(word_feature, count);
				//统计好评、差评的个数
				String word_label = word_feature.substring(0, word_feature.indexOf("_"));
				if(likelihoodNorm.containsKey(word_label)) {
					likelihoodNorm.put(word_label, likelihoodNorm.get(word_label) + (count + 0.0) );
				}
				else {
					likelihoodNorm.put(word_label, (count + 0.0));
				}
				//统计词语
				String word = word_feature.substring(word_feature.indexOf("_") + 1);
				/*当V中没有某个词语时*/
				if(!V.contains(word)) {
					V.add(word);
				}
			}
			else {
				//存放好评、差评的个数
				
				if(prior.containsKey(word_feature)) {
					prior.put(word_feature, prior.get(word_feature) + count);
					priorNorm += count;
				}
				else {
					prior.put(word_feature, count);
					priorNorm += count;
				}
			}
		}
		System.out.println(prior);
		bufferedReader.close();
	}
	//读取测试数据，并利用模型对测试集的词语进行分析，得出正确、错误个数
	public static String predict_test(String sentence_test, String modelFilePath) throws Exception{
		loadModel(modelFilePath);
		
		String predictLabel = null; //为测试数据预测标签
		String[] word = sentence_test.split("\t");
		String[] words = word[1].split(" ");
		String[] label_set = {"好评", "差评"}; //返回好评和差评
		double maxValue = Double.NEGATIVE_INFINITY;  //设置一个极小值
		double tempRate = 0.0; //临时概率
		//System.out.println(likelihood);
		for (String label : label_set) {
			if(label.equals("好评")) {
				tempRate = Math.log(prior.get(label) / priorNorm) ;
			}
			else {
				tempRate = Math.log(prior.get(label) / priorNorm);
			}
			for (String a_word : words) {
				//集合V保证测试集中不会让非中文的单词去计算概率
				if(!V.contains(a_word)) {
					continue;
				}
				String labelWord = label + "_" + a_word;
				if(likelihood.containsKey(labelWord)) {
					tempRate *= (likelihood.get(labelWord) + 1) / (likelihoodNorm.get(label) + V.size());
				}
			}
			if(tempRate > maxValue) {
				maxValue = tempRate;
				predictLabel = label;
			}
		}
		return predictLabel;
	}
	//评估模型的正确率
	public static void validate(String testFilePath, String modelFilePath, String resultFilePath) throws Exception{
		FileInputStream file = new FileInputStream(testFilePath);
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resultFilePath));
		String sentence = null;
		double correct_num = 0.0, incorrect_num = 0.0;
		double rate = 0.0;
		int line_num = 1; //序号
		DecimalFormat decimal = new DecimalFormat("0.00%"); //定义一个数字格式化变量
		//读取测试文件
		while(( sentence = bufferedReader.readLine() ) != null) {
			String predictLabel = predict_test(sentence, modelFilePath);
			String[] word = sentence.split("\t");
			//System.out.println("原本的:" + word[0]);
			//System.out.println("预测的:" + predictLabel);
			/*声明：此处是在处理那个问号，不知道是怎么出来的*/
			if(word[0].equals("好评")) {
				if(predictLabel.equals(word[0]) || predictLabel.equals("?好评")) {
					correct_num++;
				}
				else {
					incorrect_num++;
				}
			}
			else {
				if(predictLabel.equals(word[0])) {
					correct_num++;
				}
				else {
					incorrect_num++;
				}
			}
			bufferedWriter.append(line_num + "\t" + predictLabel + "\n");
			line_num++;
			
		}
		System.out.println("correct_num: " + (int)correct_num + "条");
		System.out.println("incorrect_num: " + (int)incorrect_num + "条");
		rate = correct_num / (correct_num + incorrect_num);
		
		String result = "模型正确率：" + decimal.format(rate);
		bufferedWriter.append(result);
		System.out.println(result);
		//关闭文件流
		bufferedReader.close();
		bufferedWriter.flush();
		bufferedWriter.close();
	}
	//主函数
	public static void main(String[] args) throws Exception {
		String trainFile = "D:/hadoop_test/input"; // 这里指定hdfs下training data的scheme
		String modelFilePath = "D:/hadoop_test/output_model/2016082065_模型-r-00000"; // 这里指定model文件将放置在本地的那个目录下，并给model文件命名
		//NB.train_model(trainFile, modelFilePath);
		System.out.println("MapReduce执行完成！");
		
		if ((new File(modelFilePath)).exists()) {
			String sentencesFilePath = "D:/hadoop_test/input_test/test-1000.txt"; // 这里指定sentences.txt文件的路径
			String resultFilePath = "D:/hadoop_test/output_result/2016082065_预测结果.txt"; // 这里指定结果文件路径，结果文件的名称按照指定要求描述
			NB.validate(sentencesFilePath, modelFilePath, resultFilePath);
			System.out.println("评估模型执行完成！");
		}
	}
}
