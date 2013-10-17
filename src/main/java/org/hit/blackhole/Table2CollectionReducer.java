package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Table2CollectionReducer extends
		Reducer<Text, Text, Text, Text> {
		
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Table2Value table2Value = new Table2Value(values);
		context.write( key, new Text(table2Value.toString()) );
	}
}

