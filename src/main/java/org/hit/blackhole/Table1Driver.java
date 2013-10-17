package org.hit.blackhole;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Table1Driver extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Table1Driver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length  != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output> duration SMSlen\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Table1RecordMapper.duration = Integer.parseInt(args[2]);
		Table1RecordMapper.SMSLen = Integer.parseInt(args[3]);
		
		Job job = new Job(getConf(), "Create Table1");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Table1RecordMapper.class);
		job.setReducerClass(Table1CollectionReducer.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
