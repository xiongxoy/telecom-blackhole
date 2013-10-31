package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;


public class TablePrinterMapper extends TableMapper<Text, Text>{
	public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		// process data for the row from the Result instance.
		Key tb1key = new Key(row);
		byte[] v = value.getValue( Table1Record.COLUMN_FAMILY, Table1Record.ATTRIBUTE );
		Table1Value tb1value = new Table1Value( Bytes.toString(v) );
		
		context.write(new Text(tb1key.toString()), new Text(tb1value.toString()));
	}

}
