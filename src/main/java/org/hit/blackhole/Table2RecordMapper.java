package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import HBaseIndexAndQuery.HBaseDao.HBaseDao;
import HBaseIndexAndQuery.HBaseDao.HBaseDaoImp;


public class Table2RecordMapper extends Mapper<LongWritable, Text, Text, Text>{
	private static HBaseDao dao = null;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {
			String s = value.toString();
		Table2Record record = new Table2Record(s);
		if (record.isValid()) {
			String key2 = record.getKey();
			String v1 = record.getPagingRspNum();
			String v2 = record.getTCHCongestionNum();
			context.write(new Text(key2), new Text(v1+','+v2));
		} else {
			return;
		}
	}	
	
	boolean existRow(String row) throws IOException {
		if (dao == null) {
			System.err.println("Use default Hbase.");
			dao = HBaseDaoImp.GetDefaultDao();
		}
		return dao.hasRow(Table2Record.TABLE_NAME, row);
	}

	public static void setDao(HBaseDao dao) {
		Table2RecordMapper.dao = dao;
	}
}
