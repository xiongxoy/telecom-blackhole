package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import HBaseIndexAndQuery.HBaseDao.HBaseDao;

public class HBasePagingDataMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			System.out.println("=================In Map==================");
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
			HBaseDao dao = HBaseConnection.getDao();
			HTable table = dao.getHBaseTable(RecordSchema.TABLE_NAME);
			table.put(put);
		}
}



