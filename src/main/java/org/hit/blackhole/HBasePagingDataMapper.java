package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import HBaseIndexAndQuery.HBaseDao.HBaseDao;

public class HBasePagingDataMapper extends Mapper<LongWritable, Text, NullWritable, Put> {
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			Record record = new Record(value.toString());
			if (!record.isValid()) {
				return;
			}
		
			System.out.println("=================VALID==================");
			RecordSchema schema = new RecordSchema(record);
			Put put = new Put(Bytes.toBytes(key.get()));
			put.add(RecordSchema.COLUMN_FAMILY, 
					RecordSchema.ATTRIBUTE, 
					Bytes.toBytes(schema.toString()) );
			context.write(null, put);
		}
}

