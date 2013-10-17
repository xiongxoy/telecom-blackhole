package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Table1CollectionReducer extends
		Reducer<Text, Text, Text, Text> {
	enum RecordCount {
		ROW_NUM
	}
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		if ( context.getCounter(RecordCount.ROW_NUM).getValue() >= 10000 ) {
			return;
		}
		
		context.getCounter(RecordCount.ROW_NUM).increment(1);
		long c = context.getCounter(RecordCount.ROW_NUM).getValue();
		Table1Value table1Value = new Table1Value(values, c);
		
		context.write( key, new Text(String.valueOf(c) + "," + table1Value.toString()) );
	}
}
