package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;


public class Table1HbaseReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
	enum RecordCount {
		ROW_NUM
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		if ( context.getCounter(RecordCount.ROW_NUM).getValue() >= 10000 ) {
			return;
		}
		
		context.getCounter(RecordCount.ROW_NUM).increment(1);
		long c = context.getCounter(RecordCount.ROW_NUM).getValue();
		
		Table1Value table1Value = new Table1Value(values, c);
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(Table1Record.COLUMN_FAMILY, Table1Record.ATTRIBUTE, Bytes.toBytes(table1Value.toString()));
		context.write(null, put);
	}
}
