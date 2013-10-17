package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Table1RecordMapper extends Mapper<LongWritable, Text, Text, Text> {
	public static int SMSLen;  
	public static int duration;
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String s = value.toString();
		Table1Record record = new Table1Record(s, SMSLen, duration);
		if (record.isValid()) {
			String key2 = record.getKey();
			String v1 = record.getPagingFailCINum();
			String v2 = record.getItem(Record.VC_CALLED_IMSI);
			context.write(new Text(key2), new Text(v1+','+v2));
			
		} else {
			return;
		}
	}
}
