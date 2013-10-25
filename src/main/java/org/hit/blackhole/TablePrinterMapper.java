package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class TablePrinterReducer extends Reducer<ImmutableBytesWritable, Result, NullWritable, Text> {
	
	@Override
	protected void reduce(ImmutableBytesWritable row, Iterable<Result> values,
			Context context)
					throws IOException, InterruptedException {
		for (Result v :values) {
			
		}
	}
}

public class TablePrinterMapper extends TableMapper<ImmutableBytesWritable, Result>{
	public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		    // process data for the row from the Result instance.
		context.write(row, value);
		
//		items[PAGING_NUM] = "0";
//		items[PAGING_FAIL_NUM] = String.valueOf(pagingFailNum);
//		items[PAGING_FAIL_RATE] = "0";
//		items[PAGING_FAIL_CI_RATE] = String.valueOf(pagingFailCIRate);
//		items[PAGING_FAIL_CI_NUM] = String.valueOf(pagingFailCINum);
//		items[PAGING_FAIL_USER_NUM] = String.valueOf(pagingFailUserNum);
//		items[PAGING_FAIL_USER_RATE] = String.valueOf(pagingFailUserRate);
//		items[TCH_CONGESTION_NUM] = "0";
//		items[ROW_NUM] = String.valueOf(row_num);
		
	}
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "Write Table to File");
		job.setJarByClass(TablePrinterMapper.class);     // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob(
		  Table1Record.TABLE_NAME,        // input HBase table name
		  scan,             // Scan instance to control CF and attribute selection
		  TablePrinterMapper.class,	 // mapper
		  ImmutableBytesWritable.class,  // mapper output key
		  Result.class,             // mapper output value
		  job);
		
		job.setOutputFormatClass(FileOutputFormat.class);   // because we aren't emitting anything from mapper

		boolean b = job.waitForCompletion(true);
		if (!b) {
		  throw new IOException("error with job!");
		}
	}
}
