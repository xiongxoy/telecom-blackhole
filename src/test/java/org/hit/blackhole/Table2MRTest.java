package org.hit.blackhole;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

public class Table2MRTest {
	
	/**
	 * 第二个查询的Where操作
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void processRecordAndFilter() throws IOException, InterruptedException {
		new MapDriver<LongWritable, Text, Text, Text>()
			.withMapper(new Table2RecordMapper())
			.withInput(
					new LongWritable(1000),
					new Text("32,72138319189557,1,0,0,2012-12-10 13:08:21.000,2012-12-10 13:08:21.000," +
			//       ^^ ^^^^^^^^^^^^^^       ^^^^^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^
			//		 ^^                      dtSTime                 dtETime
			//      Protocol 
					"2012,12,10,13,8,10.125.84.38,10.125.29.7,10816,11008,11008,10816,9975,24187," +
					"9975,24187,0,3E648097,39649D5A,460078898184530,,,,,,3,255,0,42,5,1,255,0,1,9," +
					"255,255,255,255,255,255,738974893301761,35218731900928,0,262215,0,0,255,255,255," +
					"0,,0,0,0,0,0,255,9239,24,24,940,1024,1640,0,0,0,2120,2194,2260,2377,0,1684,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,"))
			.withOutput(new Text("intFirstLac,intFirstCi"), new Text("pagingFailCINum"))
			.runTest();
	}
	
		/**
	 * 在上一步中，应当保留LongWritable在输出的Value中，而在Reduce中将其舍弃，从而形成以<cgi,bsc>为key的输出
	 * 应当将结果输出到Hbase中
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void countPangingNum() throws IOException, InterruptedException {
		Text key = new Text("Get");
		List<Text> values = Arrays.asList(new Text("121"));
		new ReduceDriver<Text, Text, Text, Text>()
			.withReducer(new Table2CollectionReducer())
			.withInput( new Text("intFirstLac,intFirstCi"), values) 
			.withOutput( new Pair<Text,Text>(new Text("intFirstLac,intFirstCi"), new Text("pagingNumber,THCjamNumber")))
			.runTest();
	}
		
}


