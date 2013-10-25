package org.hit.blackhole;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import HBaseIndexAndQuery.HBaseDao.HBaseDao;


public class Table1Driver extends Configured implements Tool {

	public static Map<String,Pair<Integer, Integer>> para = new HashMap<String, Pair<Integer, Integer>>();
	public static final int DURATION = 0;
	public static final int SMSLEN = 1;
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
		if (args.length  != 1) {
			System.err.printf("Usage: % <param_file>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		List<Pair<Integer, Integer>> param_list = readParameterFromFile(args[0]);
		List<Job> jobs = new ArrayList<Job>();
		for (int i=0; i<param_list.size(); i++) {
			int duration = param_list.get(i).getFirst();
			int SMSLen = param_list.get(i).getSecond(); 
			jobs.add( createNewJob(""+i, duration, SMSLen) );
		}
		
		JobControl jc = new JobControl("Create Table 1");
		return  0;
	}
	
	
	
	private List<Pair<Integer, Integer>> readParameterFromFile(String file) throws IOException {
		Path path = new Path(file);
		FileSystem fs = FileSystem.get(getConf());
		// Read File
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)) );
		String line;
		while ( (line = br.readLine()) != null ) {
			List<String> split = StringUtil.split(line, ',');
			new Pair<Integer, Integer>(
					Integer.parseInt(split.get(DURATION)), 
					Integer.parseInt(split.get(SMSLEN))
					);
		}
		return null;
	}

	private Job createNewJob(String jobname, int duration, int SMSLen) throws IOException {
		Job job = new Job(getConf(), "Create Table1");
		job.setJarByClass(getClass());
		HBaseDao dao = HBaseConnection.getDao();
		
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		
		TableMapReduceUtil.initTableMapperJob(
				RecordSchema.TABLE_NAME,
				scan, 
				Table1RecordMapper.class, 
				Text.class, 
				Text.class, 
				job);
		
		String outTableName = jobname;
		para.put(outTableName, new Pair<Integer, Integer>(duration, SMSLen));
		dao.CreateTable(Bytes.toBytes(outTableName), false);
		TableMapReduceUtil.initTableReducerJob(
				outTableName, 
				Table1HbaseReducer.class, 
				job);
				
		return job;
	}
	public static String getTableName(String jobName) {
		return jobName;
	}
}
