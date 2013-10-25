package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Table2RecordMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {
			String s = value.toString();
		Table2Record record = new Table2Record(s);
		String key2 = record.getKey();
		
		if ( record.isValid() && HBaseConnection.hasRow(Table1Record.TABLE_NAME, Bytes.toBytes(key2)) ) {
			String v1 = record.getPagingRspNum();
			String v2 = record.getTCHCongestionNum();
			context.write(new Text(key2), new Text(v1+','+v2));
		} else {
			return;
		}
	}	
}
