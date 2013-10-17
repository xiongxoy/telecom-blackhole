package HBaseIndexAndQuery.HBaseDao;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public abstract interface HBaseDao
{
  public abstract void CreateHbaseConnect();
  public abstract void CreateTable(String table, boolean isdelete);
  public abstract void TableAddFaminly(String table, String family);
  public abstract void TableAddData(String table, String row, 
		  							String family, String column, byte[] value) throws IOException;
  public abstract HTable getHBaseTable(String table) throws IOException;
  public abstract Result TableGetResult(String table, String row) throws IOException;
  public abstract byte[] TableGetValue(String table, String row, String family, String colum) throws IOException;
  public abstract boolean hasRow(String table, String row) throws IOException;
}
