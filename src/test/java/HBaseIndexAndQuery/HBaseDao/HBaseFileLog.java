package HBaseIndexAndQuery.HBaseDao;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseFileLog {

	HBaseDaoImp imp ;
	String tableName="FileLog";
	String famliyKey ="n";
	HTable table = null;
	public HBaseFileLog(HBaseDaoImp limp)
	{
		imp = limp;
	}

	public HBaseFileLog()
	{
		imp = HBaseDaoImp.GetDefaultDao();
	}


	public boolean InsertFileAndID(String fileName,long id)
	{

		try
		{
		table = imp.getHBaseTable(tableName);
		Put put = new Put(Bytes.toBytes(id));
		put.add(famliyKey.getBytes(), null, fileName.getBytes());
		table.put(put);
		table.flushCommits();
		return true;
		}catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}


}
