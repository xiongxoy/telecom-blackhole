package org.hit.blackhole;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import HBaseIndexAndQuery.HBaseDao.HBaseDao;

public class BlackHoleDriver extends Configured implements Tool {
	public static Configuration conf = null;

	class CreateTable1JobsCreator {
		List<Job> create(String param_file) throws IOException {
			List<Pair<Integer, Integer>> param_list = readParameterFromFile(param_file);
			List<Job> jobs = new ArrayList<Job>();
			for (int i=0; i<param_list.size(); i++) {
				int duration = param_list.get(i).getFirst();
				int SMSLen = param_list.get(i).getSecond(); 
				jobs.add( createNewJob(""+i, duration, SMSLen) ); // job name is the same as talbe name 
			}
			return jobs;
		}

		private Job createNewJob(String jobname, int duration, int SMSLen) throws IOException {
			Job job = new Job(BlackHoleDriver.conf, jobname);
			job.setJarByClass(BlackHoleDriver.class);
			HBaseDao dao = HBaseConnection.getDao(conf);

			para.put(jobname, new Pair<Integer, Integer>(duration, SMSLen));
			String outTableName = jobname;
			dao.CreateTable(Bytes.toBytes(outTableName), false);
			
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);

			TableMapReduceUtil.initTableMapperJob(
					RecordSchema.TABLE_NAME,
					scan, 
					Table1HBaseMapper.class, 
					Text.class, 
					Text.class, 
					job);
			TableMapReduceUtil.initTableReducerJob(
					outTableName, 
					Table1HbaseReducer.class, 
					job);

			return job;
		}

		private List<Pair<Integer, Integer>> readParameterFromFile(String file) throws IOException {
			Path path = new Path(file);
			FileSystem fs = FileSystem.get(BlackHoleDriver.conf);
			List<Pair<Integer, Integer>> list = new ArrayList<Pair<Integer, Integer>>();
			// Read File
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)) );
			String line;
			while ( (line = br.readLine()) != null ) {
				List<String> split = StringUtil.split(line, ',');
				list.add( new Pair<Integer, Integer>(
							Integer.parseInt(split.get(DURATION)), 
							Integer.parseInt(split.get(SMSLEN))
							)
						) ;
			}
			return list;
		}
	}
	class ImporterJobCreator {
		private final String time_end;
		private final String time_start; 

		public ImporterJobCreator() {
			time_start = convertTimeFormat(Record.T_START);
			time_end = convertTimeFormat(Record.T_END);
		}
		/* help function for converting date format
		 * from	"2012-03-02 09:00:00.000"
		 * to	"20120302090000000"
		 */
		private String convertTimeFormat(String time) {
			String time_new = "";
			time_new += time.substring(0, 4); // Year
			time_new += time.substring(5, 7); // Month
			time_new += time.substring(8, 10); // Day
			time_new += time.substring(11, 13); // Hour
			time_new += time.substring(14, 16); // Minute
			time_new += time.substring(17, 19); // Second
			time_new += time.substring(20, 23); // MilliSecond

			return time_new;
		}
		private Job create(String file_name) throws IOException {
			// Create Table for Importing Data 
			HBaseDao dao = HBaseConnection.getDao(conf);
			if (dao.TableExists(RecordSchema.TABLE_NAME)) {
				System.out.println("=================== Has table " + Bytes.toString(RecordSchema.TABLE_NAME) );
				System.exit(-1);
			}
			dao.CreateTable(RecordSchema.TABLE_NAME, false);
			dao.TableAddFaminly(RecordSchema.TABLE_NAME, RecordSchema.COLUMN_FAMILY);

			// Load Files
			Job job;
			Path path;
			FileSystem fs;

			path = new Path(file_name);
			job = new Job(BlackHoleDriver.conf, "Import Data to SchemaTable");
			fs = FileSystem.get(BlackHoleDriver.conf);
			if ( fs.getFileStatus(path).isDir() ) {
				FileStatus[] status = fs.listStatus(path);
				for (FileStatus f : status) {
					String file = f.getPath().getName();
					System.out.println(file);
					if (isValid(file)) {
						System.out.println("================= add "+file );
						FileInputFormat.addInputPath(job, f.getPath());
					}
				}
			} else {
				System.out.println("===========================Not a dir !!! ");
				if (isValid(path.getName())) {
					FileInputFormat.addInputPath(job, path);
				}
			}

			job.setJarByClass(BlackHoleDriver.class);
//			job.setInputFormatClass(FileInputFormat.class);
			job.setMapperClass(HBasePagingDataMapper.class);
//			job.setOutputFormatClass(NullOutputFormat.class);
			
			TableMapReduceUtil.initTableReducerJob(
					Bytes.toString(RecordSchema.TABLE_NAME),
					null,
					job);
			job.setNumReduceTasks(0);

			return job;
		}
		/*
		 * get time stamp from file name
		 */
		private String getTime(String file) {
			int start=0, end=0;
			char ch = '.';

			start = file.indexOf(ch, 0);
			start++;
			start = file.indexOf(ch, start);
			start++;
			end = file.indexOf(ch, start);

			return file.substring(start, end);
		}
		private boolean isValid(String file) {
			String t = getTime(file);

			if (time_start.compareTo(t) <= 0 && 
					t.compareTo(time_end) < 0) {
				return true;
			} else {
				return false;
			}
		}
	}
	class UpdateTable1JobsCreator {
		List<Job> create(int s) throws IOException {
			List<Job> jobs = new ArrayList<Job>();
			for (int i = 0; i < s; i++) {
				jobs.add( createNewJob(i+"") );
			}
			return jobs;
		}
		private Job createNewJob(String jobname) throws IOException {
			Job job = new Job(BlackHoleDriver.conf, jobname);
			job.setJarByClass(BlackHoleDriver.class); // TODO by which class?
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			
			TableMapReduceUtil.initTableMapperJob(
					RecordSchema.TABLE_NAME,
					scan, 
					Table2HBaseMapper.class, 
					Text.class, 
					Text.class,
					job);
			TableMapReduceUtil.initTableReducerJob(
					jobname, 
					Table2HbaseReduer.class, 
					job);
			return job;
		}
	}
	
	public static Map<String,Pair<Integer, Integer>> para = new HashMap<String, Pair<Integer, Integer>>();
	public static final int DURATION = 0;
	public static final int SMSLEN = 1;

	/**
	 * @param args
	 * hadoop black.jar [input] [output] [param_file] 
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new BlackHoleDriver(), args);
		System.exit(exitCode);
	}

	private void cleanUp(int s) {
		HBaseDao dao = HBaseConnection.getDao(conf);
		dao.CreateTable(RecordSchema.TABLE_NAME, true); // delete table when set true.
		for (int i = 0; i < s; i++) {
			dao.CreateTable(Bytes.toBytes(i+""), true);
		}
	}

	private int runAllJob(Job importer, List<Job> jobs1,
			List<Job> jobs2, List<Job> printers) throws InterruptedException, IOException {
		JobControl jc = new JobControl("All Jobs");
		
		ControlledJob cji = new ControlledJob(BlackHoleDriver.conf);
		cji.setJob(importer);
		jc.addJob(cji);
		for (int i = 0; i < jobs1.size(); i++) {
			ControlledJob cj1,cj2,cjp;
			cj1 = new ControlledJob(BlackHoleDriver.conf);
			cj2 = new ControlledJob(BlackHoleDriver.conf);
			cjp = new ControlledJob(BlackHoleDriver.conf);
			cj1.setJob(jobs1.get(i));
			cj2.setJob(jobs2.get(i));
			cjp.setJob(printers.get(i));
			
			cj1.addDependingJob(cji);
			cj2.addDependingJob(cj1);
			cjp.addDependingJob(cj2);
			jc.addJob(cj1);
			jc.addJob(cj2);
			jc.addJob(cjp);
		}
		
		Thread t = new Thread(new JobRunner(jc));
		t.start();
		while ( !jc.allFinished() ) {
			System.out.println("Still running...");
			Thread.sleep(5000);
		}
		System.out.println("Completed !!!");
		
		for( ControlledJob cj: jc.getFailedJobList() ) {
			System.out.println( cj.getMessage() );
		}
		return 0;
	}

	private static void Usage() {
		String s = "hadoop black.jar [param_file] [input] [output] ";
		System.out.println(s);
	}
	private List<Job> createCreateTable1Jobs(String param_file) throws IOException {
		CreateTable1JobsCreator createTable1JobsCreator = new CreateTable1JobsCreator();
		return createTable1JobsCreator.create(param_file);
	}
	private Job createImporterJob(String file_name) throws IOException {
		ImporterJobCreator importerJobCreator = new ImporterJobCreator();
		return importerJobCreator.create(file_name);
	}
	private List<Job> createPrinterJob(String outpath, int i) throws IOException {
		PrinterJobCreator printerJobCreator = new PrinterJobCreator();
		return printerJobCreator.create(outpath, i);
	}
	private List<Job> createUpdateTable1Jobs(int i) throws IOException {
		UpdateTable1JobsCreator updateTable1JobsCreator = new UpdateTable1JobsCreator();
		return updateTable1JobsCreator.create(i);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		for (String s : args)
			System.out.println(s);
		if ( args.length != 3 ) {
			Usage();
			System.exit(-1);
		}
		
		BlackHoleDriver driver = new BlackHoleDriver();
		conf = getConf();
		if (conf == null) {
			System.err.println("!!! OMG conf is null.");
			System.exit(-1);
		}
		
		driver.cleanUp(4);
		
		int exitCode;
		Job importer = driver.createImporterJob(args[0]);
		List<Job> jobs1 = driver.createCreateTable1Jobs(args[2]);
		List<Job> jobs2  = driver.createUpdateTable1Jobs(jobs1.size());
		List<Job> printers = driver.createPrinterJob(args[1], jobs1.size());
		
		exitCode = driver.runAllJob(importer, jobs1, jobs2, printers);
//		driver.cleanUp(jobs1.size());
		return exitCode;
	}
}
