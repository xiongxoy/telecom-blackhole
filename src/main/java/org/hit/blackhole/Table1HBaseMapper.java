package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;

public class Table1HBaseMapper extends TableMapper<Text, Text> {
	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
	
		String jobname = context.getJobName();
		Pair<Integer, Integer> pair = BlackHoleDriver.para.get(jobname);
		int duration=pair.getFirst().intValue();  
		int SMSLen=pair.getSecond().intValue();
		
		byte[] v = value.getValue(RecordSchema.COLUMN_FAMILY, RecordSchema.ATTRIBUTE);
		String v_str = Bytes.toString(v);
		Table1Record record = new Table1Record(v_str, duration, SMSLen);
		if (record.isValid()) {
			String key2 = record.getKey();
			String v1 = record.getPagingFailCINum();
			String v2 = record.getItem(RecordSchema.VC_CALLED_IMSI);
			context.write(new Text(key2), new Text(v1+','+v2));
		} else {
			return;
		}
	}
}
