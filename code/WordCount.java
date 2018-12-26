package org.myorg;

import java.io.IOException;
import java.util.*;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class WordCount extends Configured implements Tool {

	public static final int StartYear = 1900;
	public static final int TotalYears = 100;

	public static class HadoopMap extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				String year = tokenizer.nextToken();
				String matches = tokenizer.nextToken();
				String pages = tokenizer.nextToken();
				String volumes = tokenizer.nextToken();	

				if(!isNeedYead(Integer.parseInt(year))){
					continue;
				}

				Text text = new Text();
				text.set(word);

				int[] arr = new int[TotalYears];
				updateArr(arr, year, matches);
				context.write(text, getValue(arr));
			}
		}

		private boolean isNeedYead(int year) {
			return StartYear <= year && year < (StartYear + TotalYears);
		}

		private static void updateArr(int[] arr, String yearString, String matches) {
			int count = Integer.parseInt(matches);
			int year = Integer.parseInt(yearString);
			arr[year - StartYear] += count;
		}

	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int[] arr = new int[TotalYears];

			for (Text val : values) {
				updateArr(arr, val.toString());
			}
			context.write(key, getValue(arr));
		}

		private static void updateArr(int[] arr, String line) {
			StringTokenizer tokenizer = new StringTokenizer(line);

			tokenizer.nextToken(); // skip dispersion

			for (int i = 0; i < TotalYears; i++) {
				int count = Integer.parseInt(tokenizer.nextToken());
				arr[i] += count;
			}
		}
	}

	private static Text getValue(int[] arr) {
		StringBuilder sb = new StringBuilder();

		double sqsum = 0;
		double sum = 0;

		for (int x : arr) {
			sb.append(x);
			sb.append('\t');

			sqsum += ((double) x*x) / TotalYears;
			sum += x;
		}

		double d = sqsum - sum * sum / TotalYears / TotalYears;

		sb.setLength(sb.length() - 1);
		Text text = new Text();
		text.set(d + "\t" + sb.toString());

		return text;
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
        job.getConfiguration().setStrings("mapreduce.reduce.shuffle.memory.limit.percent", "0.15");

		job.setJarByClass(WordCount.class);
		job.setJobName("wordcount");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(HadoopMap.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new WordCount(), args);
		System.exit(ret);
	}
}