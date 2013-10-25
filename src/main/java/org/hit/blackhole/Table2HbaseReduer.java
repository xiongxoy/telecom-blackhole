package org.hit.blackhole;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import HBaseIndexAndQuery.HBaseDao.HBaseDao;

public class Table2HbaseReduer extends TableReducer<Text, Text, NullWritable>{
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Table2Value table2Value = new Table2Value(values);
		String tableName = context.getJobName();
		Put put = getNewTable1Value(key, table2Value, tableName);
		context.write(null, put);
	}
			
	private Put getNewTable1Value(Text key, Table2Value table2Value, String table1Name ) throws IOException {
		HBaseDao dao = HBaseConnection.getDao();
		byte[] v1 = dao.getTableValue(Bytes.toBytes( table1Name ), 
				Bytes.toBytes( key.toString() ), 
				Table1Record.COLUMN_FAMILY, 
				Table1Record.ATTRIBUTE);

		Table1Value table1Value = new Table1Value(Bytes.toString(v1));
		updateTable1WithTable2(table1Value, table2Value);
		
		byte[] value =  Bytes.toBytes(table1Value.toString());
		Put put = new Put( Bytes.toBytes(key.toString()) );
		put.add(Table1Record.COLUMN_FAMILY, Table1Record.ATTRIBUTE, value);
		
		return put;
	}

	/*	
	 * set a.TCH信道拥塞次数=b.TCH信道拥塞次数, 
	 *	   a.寻呼总次数=a.寻呼失败总次数+b.寻呼响应总次数
	 *		 
	 * update #tmpAnalyse_1350284638652 
	 *	      set 寻呼失败率=case when 寻呼总次数>0 
	 *		  then 1.0*寻呼失败总次数/寻呼总次数 else 0 end
	 */
	private void updateTable1WithTable2(Table1Value v1, Table2Value v2) {
		// Update TCH_CONGESTION_NUM 
		String v1_tch = v1.getItem(Table1Value.TCH_CONGESTION_NUM);
		String v2_tch = v2.getItem(Table2Value.TCH_CONGESTION_NUM);
		String v1_tch_new = String.valueOf( Long.parseLong(v1_tch) + Long.parseLong(v2_tch) );
		v1.setItem(Table1Value.TCH_CONGESTION_NUM, v1_tch_new);
		
		// Update PAGING_NUM
		String v1_pfn = v1.getItem(Table1Value.PAGING_FAIL_NUM);
		String v2_pr = v2.getItem(Table2Value.PAGING_RSP_NUM);
		String v1_pn = String.valueOf(Long.parseLong(v1_pfn) + Long.parseLong(v2_pr));
		v1.setItem(Table1Value.PAGING_NUM, v1_pn);
		
		// Update PAGING_FAIL_NUM
		if (Long.parseLong(v1_pn) > 0) {
			String v1_pfr =  String.valueOf(1.0 * Long.parseLong(v1_pfn) / Long.parseLong(v1_pn));
			v1.setItem(Table1Value.PAGING_FAIL_RATE, v1_pfr);
		} 
	}
}
