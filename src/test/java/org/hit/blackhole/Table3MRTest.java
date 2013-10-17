package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class Table3MRTest {
	
	/**
	 * 不需要Join，直接通过Hbase查表即可，然后update相应的项，最后更新Hbase数据
	 */
	@Test
	public void updateTable1() {
		
	}

}
