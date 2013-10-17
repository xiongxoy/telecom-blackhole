package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Reducer;

import HBaseIndexAndQuery.HBaseDao.HBaseDao;
import HBaseIndexAndQuery.HBaseDao.HBaseDaoImp;


public class Table1HbaseReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
	static final String COLUM_FAMILY = "cf1";
	static final String ATTRIBUTE = "record";
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
		put.add(Bytes.toBytes(COLUM_FAMILY), Bytes.toBytes(ATTRIBUTE), Bytes.toBytes(table1Value.toString()));
		context.write(null, put);
	}
}
