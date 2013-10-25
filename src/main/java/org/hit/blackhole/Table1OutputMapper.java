package org.hit.blackhole;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

public class Table1OutputMapper extends TableMapper<ImmutableBytesWritable, Result>{
	public void map(ImmutableBytesWritable row, Result values, Context context) {
//		System.out.format(arg0, arg1)
		values.getColumnLatest(Table1Record.COLUMN_FAMILY, Table1Record.ATTRIBUTE);
	}
}
