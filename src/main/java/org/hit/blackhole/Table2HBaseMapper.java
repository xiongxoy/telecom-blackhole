package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class Table2HBaseMapper extends TableMapper<Text, Text>{
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Context context)
					throws IOException, InterruptedException {
		String schema_str = Bytes.toString( value.getValue(RecordSchema.COLUMN_FAMILY, RecordSchema.ATTRIBUTE) );
		RecordSchema record_schema = new RecordSchema( schema_str );
		Table2Record record_tb2 = new Table2Record( record_schema ); 
		String key2 = record_tb2.getKey();
		
		String tablename = context.getJobName();
		if ( record_tb2.isValid() && HBaseConnection.hasRow(Bytes.toBytes(tablename), Bytes.toBytes(key2)) ) {
			String v1 = record_tb2.getPagingRspNum();
			String v2 = record_tb2.getTCHCongestionNum();
			context.write(new Text(key2), new Text(v1+','+v2));
		} else {
			return;
		}
	}	
}
