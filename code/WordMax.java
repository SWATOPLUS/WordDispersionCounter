package org.myorg;

import java.io.IOException;
import java.util.*;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class WordMax extends Configured implements Tool {
	public static final int MaxCount = 20;
	
	private static class LineModel implements Comparable<LineModel> {
        public final String line;
        public final double value;

        LineModel(String line, double value) {
            this.line = line;
            this.value = value;
        }

        @Override
        public int compareTo(LineModel o) {
            return Double.compare(value, o.value); // i'm not sure about '-'
        }
	}

	private static LineModel[] ParseValue(String s){
		String[] lines;

		if(s.contains("!")){
			lines = s.split("!");
		} else{
			lines = s.split("\\r?\\n");
		}

		LineModel[] result = new LineModel[lines.length];

		int i = 0;

		for(String line : lines){
			StringTokenizer tokenizer = new StringTokenizer(line);
			String word = tokenizer.nextToken();
			double variation = Double.parseDouble(tokenizer.nextToken());
			result[i] = new LineModel(line, variation);
			i++;
		}

		return result;
	}

	private static String PackValue(LineModel[] values){
		StringBuilder sb = new StringBuilder();

		for (LineModel model : values) {
			if(model == null){
				continue;
			}

			sb.append(model.line.trim());
			sb.append("\n");
		}
		sb.setLength(sb.length() - 1);

		return sb.toString();
	}

	public static Text MaxValuesKey = new Text("");

	public static class HadoopMap extends Mapper<LongWritable, Text, Text, Text> {

		private final PriorityQueue<LineModel> topN = new PriorityQueue<LineModel>();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			LineModel[] models = ParseValue(value.toString());

			for (LineModel model : models) {
				topN.add(model);
			}

			while (topN.size() > MaxCount) {
				topN.poll();
			}
		}
	
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while (topN.size() > MaxCount) {
				topN.poll();
			}

			LineModel[] models = new LineModel[topN.size()];
			int i = 0;

			for (LineModel model : topN) {
				models[i] = model;
				i++;
			}

			Text text = new Text();
			text.set(PackValue(models));

			context.write(MaxValuesKey, text);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private final PriorityQueue<LineModel> topN = new PriorityQueue<LineModel>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text value : values) {
				
				LineModel[] models = ParseValue(value.toString());

				for (LineModel model : models) {
					topN.add(model);
				}

				while (topN.size() > MaxCount) {
					topN.poll();
				}
			}

			LineModel[] models2 = new LineModel[topN.size()];
			int i = 0;

			for (LineModel model : topN) {
				models2[i] = model;
				i++;
			}

			Text text = new Text();
			text.set(PackValue(models2));

			context.write(key, text);
		}
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(WordMax.class);
		job.setJobName("wordmax");

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
		int ret = ToolRunner.run(new WordMax(), args);
		System.exit(ret);
	}
}