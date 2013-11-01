package org.hit.blackhole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


class PrinterJobCreator {
		private Path getPath(String output, String jobname) {
			String path;
			if( output.endsWith("/") ) {
				path = output+jobname;
			} else {
				path = output + "/" + jobname;
			}
			return new Path(path);
		}
		public List<Job> create(String output, int s) throws IOException {
			List<Job> jobs = new ArrayList<Job>();
			for (int i = 0; i < s; i++) {
				jobs.add( create(output, ""+i) );
			}
			return jobs;
		}
		private Job create(String outpath, String jobname) throws IOException {	
			Job job = new Job(BlackHoleDriver.conf, jobname);
			job.setJarByClass(TablePrinterMapper.class);     // class that contains mapper

			Scan scan = new Scan();
			scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs

			TableMapReduceUtil.initTableMapperJob(
					jobname,        // input HBase table name
					scan,             			  // Scan instance to control CF and attribute selection
					TablePrinterMapper.class,	 	  // mapper
					Text.class,   // mapper output key
					Text.class,             	  // mapper output value
					job);

			FileOutputFormat.setOutputPath(job, getPath(outpath, jobname));
			job.setOutputFormatClass(FileOutputFormat.class);   // because we aren't emitting anything from mapper

			return job;
		}
	}
