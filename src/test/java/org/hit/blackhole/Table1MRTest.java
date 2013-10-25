package org.hit.blackhole;

import java.util.List;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;


public class Table1MRTest {
	/**
	 * 假设主办方给定的是文本数据，每一行文本表示数据库中的一行，这个Map得到的是每一行解析之后的结果
	 * 执行Where的过滤操作
	 * 要完成cig和bsc的定义步骤，然后完成过滤 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void processRecordAndFilter() throws IOException, InterruptedException {
		Text value = new 
			   Text("32,72138319189557,1,0,0,2012-03-02 08:00:00.000,2012-03-02 08:00:01.000," +
			//       ^^ ^^^^^^^^^^^^^^       ^^^^^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^
			//		 ^^                      dtSTime                 dtETime
			//      Protocol 
					"2012,12,10,13,8,10.125.84.38,10.125.29.7,10816,11008,11008,10816,9975,24187," +
					"9975,24187,0,3E648097,39649D5A,460078898184530,,,,,,3,255,0,42,5,1,255,0,1,9," +
					"255,255,255,255,255,255,738974893301761,35218731900928,0,262215,0,0,255,255,255," +
					"0,,0,0,0,0,0,255,9239,24,24,940,1024,1640,0,0,0,2120,2194,2260,2377,0,1684,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,");
		Record record = new Record(value.toString());
		new MapDriver<LongWritable, Text, Text, Text>()
			.withMapper(new Table1RecordMapper())
			.withInput(new LongWritable(0), value)
			.withOutput(new Text("lac,ci"), new Text("1,vcCalledIMSI"))
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
//		Text key = new Text("lac,ci");
//		List<Text> values = (List<Text>) Arrays.asList(new Text("1,vcCalledIMSI"));
//		
//		new ReduceDriver<Text, Text, NullWritable, ImmutableBytesWritable>()
//			.withReducer(new Table1HbaseReducer())
//			.withInput(key, values)
//			.withOutput(new Text("lac,ci"), 
////					new Text("rowNum," +
////							"pagingNum,pagingFailNum," +
////							"pagingFailRate,pagingFailCINum," +
////							"pagingFailCIRate,pagingFailUserNum," +
////							"pagingFailUserRate"))
////							.runTest();
//						new Text( "1,0,1,0,1,1.0,1,0,0,1"))
//							.runTest();
	}
	
}
