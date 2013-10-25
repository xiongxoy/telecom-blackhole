package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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

