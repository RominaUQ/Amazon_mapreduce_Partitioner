package amazonpartitioner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Amazon_Rev extends Configured implements Tool {
	public static class Amazon_Mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

		private FloatWritable rate = new FloatWritable();
		private Text tokenValue = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// As the first line of the data file named "will-ockenden-metadata.csv"
			// contains attributes but not actual data
			// the line should be excluded.

			if (key.get() == 0L)
				return;

			// For the rest, we are going to compute total phone calls the person made per
			// day.
			//
			String productid = value.toString().split("\t")[3];
			String rating = value.toString().split("\t")[7];

			Float ratings = Float.parseFloat(rating);

			tokenValue.set(productid);
			rate.set(ratings);

			context.write(tokenValue, rate);

		}

	}

	public static class Amazon_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

		private FloatWritable result = new FloatWritable();

		@Override
		protected void reduce(Text token, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			FloatWritable result = new FloatWritable();
			Float average = 0f;
			int count = 0;
			float sum = 0;

			for (FloatWritable val : values) {
				sum += val.get();
				count += 1;
			}

			average = sum / count;
			result.set(average);
			context.write(token, result);

		}
	}

	public static class Amazon_Partitioner extends Partitioner<Text, FloatWritable> {

		public int getPartition(Text key, FloatWritable value, int numReduceTasks) {
			String productName = value.toString().split("\t")[5];
			if (numReduceTasks == 0) {
				return 0;
			}

			if (productName.substring(0, 1).toUpperCase().compareTo("A") >= 0
					&& productName.substring(0, 1).toUpperCase().compareTo("G") <= 0) {
				return 0;

			} else {
				if (productName.substring(0, 1).toUpperCase().compareTo("H") >= 0
						&& productName.substring(0, 1).toUpperCase().compareTo("N") <= 0) {
					return 1 % numReduceTasks;
				} else {
					return 2 % numReduceTasks;
				}
			}
		}

	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Amazon_Rev(), args);
		System.exit(res);
		//System.exit(ToolRunner.run(new Reviews_AveRating_Main(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();

		// configuration.set("mapreduce.job.jar");

		Job job = new Job(configuration, "Amazon Average Rating");

		job.setJarByClass(Amazon_Rev.class);
		job.setNumReduceTasks(3);

		// set Output key class
		job.setOutputKeyClass(Text.class);

		// set Output value class
		job.setOutputValueClass(FloatWritable.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		// Set Mapper class
		job.setMapperClass(Amazon_Mapper.class);

		// Set Combiner class
		job.setCombinerClass(Amazon_Reducer.class);

		// set Reducer class
		job.setReducerClass(Amazon_Reducer.class);
        
		// set partitioner
		job.setPartitionerClass(Amazon_Partitioner.class);
		
		// set Input Format
		job.setInputFormatClass(TextInputFormat.class);

		// set Output Format
		job.setOutputFormatClass(TextOutputFormat.class);

		String inputPath = "s3://amazon-reviews-pds/tsv/amazon_reviews_us_Books_v1_02.tsv.gz";
		FileInputFormat.addInputPath(job, new Path(inputPath));

		String outputPath = "/tmp/results/";

		FileSystem fs = FileSystem.newInstance(configuration);
		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}

		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job.waitForCompletion(true) ? 0 : -1;
	}

}
