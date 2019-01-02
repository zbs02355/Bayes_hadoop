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
	//����һ������ģ������ļ�����
	public static class myOutputFormat_class extends org.apache.hadoop.mapreduce.lib.output.TextOutputFormat{
		protected static void setOutputName(JobContext job, String name) {
			job.getConfiguration().set(BASE_OUTPUT_NAME, name);
		}
	}
	//ʹ��MapReduceѵ��ģ��
	public static void train_model(String trainFile, String modelFile) throws Exception{
		if((new File(modelFile)).exists()) {
			return;
		}
		else {
			Configuration conf = new Configuration();
			//�޸������ļ�����Ӧ��Ⱥ����
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
			//job.setJar("wordCount.jar"); //�������е�jar�ļ�
			//��������
			job.setJarByClass(NB.class);
			//����Mapper��
			job.setMapperClass(WordCounter_Mapper.class);
			//����Reducer��
			job.setReducerClass(WordCounter_Reducer.class);
			//������ҵ�ϳ���
			job.setCombinerClass(WordCounter_Reducer.class);
			//�������������
			job.setOutputKeyClass(Text.class);
			//�������ֵ��
			job.setOutputValueClass(IntWritable.class);
			//��������ļ���
			job.setOutputFormatClass(myOutputFormat_class.class);
			myOutputFormat_class.setOutputName(job, "2016082065_ģ��");
			
			FileInputFormat.addInputPath(job, new Path("D:/hadoop_test/input"));
			FileOutputFormat.setOutputPath(job, new Path("D:/hadoop_test/output_model"));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
	//����ģ���ļ������ݵ�HashMap�У�������
	public static HashMap<String, Integer> prior = null; //��ź����������ĸ���
	public static double priorNorm = 0.;  //�������Ŀ
	public static HashMap<String, Integer> likelihood = null; //�������ģ���е�ÿһ�����������
	public static HashMap<String, Double> likelihoodNorm = null; //��ź����������ĸ���
	public static HashSet<String> V = null; //��Ų�ͬ���Ĵ���
	
	public static void loadModel(String modelFilePath) throws Exception{
		if(prior != null && likelihood != null) {
			return;
		}
		//��ʼ��һЩ����
		prior = new HashMap<String, Integer>(); //��ź���������
		likelihood = new HashMap<String, Integer>(); //�������ģ���е�ÿһ�����������
		likelihoodNorm = new HashMap<String, Double>(); //��ź����������ĸ���
		V = new HashSet<String>(); //��Ų�ͬ���Ĵ���
		
		//MapReduce�������ģ���ļ���UTF-8����
		FileInputStream file = new FileInputStream(modelFilePath);
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
		String line = null;  //���ڴ��һ������
		//ѭ����ȡ
		while((line = bufferedReader.readLine()) != null) {
			String word_feature = line.substring(0, line.indexOf("\t"));  
			Integer count = Integer.parseInt(line.substring(line.indexOf("\t") + 1));
			if(word_feature.contains("_")) {
				likelihood.put(word_feature, count);
				//ͳ�ƺ����������ĸ���
				String word_label = word_feature.substring(0, word_feature.indexOf("_"));
				if(likelihoodNorm.containsKey(word_label)) {
					likelihoodNorm.put(word_label, likelihoodNorm.get(word_label) + (count + 0.0) );
				}
				else {
					likelihoodNorm.put(word_label, (count + 0.0));
				}
				//ͳ�ƴ���
				String word = word_feature.substring(word_feature.indexOf("_") + 1);
				/*��V��û��ĳ������ʱ*/
				if(!V.contains(word)) {
					V.add(word);
				}
			}
			else {
				//��ź����������ĸ���
				
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
	//��ȡ�������ݣ�������ģ�ͶԲ��Լ��Ĵ�����з������ó���ȷ���������
	public static String predict_test(String sentence_test, String modelFilePath) throws Exception{
		loadModel(modelFilePath);
		
		String predictLabel = null; //Ϊ��������Ԥ���ǩ
		String[] word = sentence_test.split("\t");
		String[] words = word[1].split(" ");
		String[] label_set = {"����", "����"}; //���غ����Ͳ���
		double maxValue = Double.NEGATIVE_INFINITY;  //����һ����Сֵ
		double tempRate = 0.0; //��ʱ����
		//System.out.println(likelihood);
		for (String label : label_set) {
			if(label.equals("����")) {
				tempRate = Math.log(prior.get(label) / priorNorm) ;
			}
			else {
				tempRate = Math.log(prior.get(label) / priorNorm);
			}
			for (String a_word : words) {
				//����V��֤���Լ��в����÷����ĵĵ���ȥ�������
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
	//����ģ�͵���ȷ��
	public static void validate(String testFilePath, String modelFilePath, String resultFilePath) throws Exception{
		FileInputStream file = new FileInputStream(testFilePath);
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resultFilePath));
		String sentence = null;
		double correct_num = 0.0, incorrect_num = 0.0;
		double rate = 0.0;
		int line_num = 1; //���
		DecimalFormat decimal = new DecimalFormat("0.00%"); //����һ�����ָ�ʽ������
		//��ȡ�����ļ�
		while(( sentence = bufferedReader.readLine() ) != null) {
			String predictLabel = predict_test(sentence, modelFilePath);
			String[] word = sentence.split("\t");
			//System.out.println("ԭ����:" + word[0]);
			//System.out.println("Ԥ���:" + predictLabel);
			/*�������˴����ڴ����Ǹ��ʺţ���֪������ô������*/
			if(word[0].equals("����")) {
				if(predictLabel.equals(word[0]) || predictLabel.equals("?����")) {
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
		System.out.println("correct_num: " + (int)correct_num + "��");
		System.out.println("incorrect_num: " + (int)incorrect_num + "��");
		rate = correct_num / (correct_num + incorrect_num);
		
		String result = "ģ����ȷ�ʣ�" + decimal.format(rate);
		bufferedWriter.append(result);
		System.out.println(result);
		//�ر��ļ���
		bufferedReader.close();
		bufferedWriter.flush();
		bufferedWriter.close();
	}
	//������
	public static void main(String[] args) throws Exception {
		String trainFile = "D:/hadoop_test/input"; // ����ָ��hdfs��training data��scheme
		String modelFilePath = "D:/hadoop_test/output_model/2016082065_ģ��-r-00000"; // ����ָ��model�ļ��������ڱ��ص��Ǹ�Ŀ¼�£�����model�ļ�����
		//NB.train_model(trainFile, modelFilePath);
		System.out.println("MapReduceִ����ɣ�");
		
		if ((new File(modelFilePath)).exists()) {
			String sentencesFilePath = "D:/hadoop_test/input_test/test-1000.txt"; // ����ָ��sentences.txt�ļ���·��
			String resultFilePath = "D:/hadoop_test/output_result/2016082065_Ԥ����.txt"; // ����ָ������ļ�·��������ļ������ư���ָ��Ҫ������
			NB.validate(sentencesFilePath, modelFilePath, resultFilePath);
			System.out.println("����ģ��ִ����ɣ�");
		}
	}
}
