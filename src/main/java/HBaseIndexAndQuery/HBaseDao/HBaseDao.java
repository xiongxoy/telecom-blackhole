package HBaseIndexAndQuery.HBaseDao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.mapreduce.Job;

public abstract interface HBaseDao
{
  public abstract void CreateHbaseConnect();
  public abstract void CreateTable(byte[] table, boolean isdelete);
  public abstract void TableAddFaminly(byte[] table, byte[] family);
  public abstract void TableAddData(byte[] table, byte[] row, 
		  							byte[] family, byte[] column, byte[] value) throws IOException;
  public abstract HTable getHBaseTable(byte[] table) throws IOException;
  public abstract Result getTableResult(byte[] table, byte[] row) throws IOException;
  public abstract byte[] getTableValue(byte[] table, byte[] row,  byte[] family, byte[] column) throws IOException;
  public abstract boolean hasRow(byte[] table, byte[] row) throws IOException;
  public abstract void setTableValue(byte[] table, byte[] row,
		  							 byte[] columFamily, byte[] column, byte[] value) throws IOException;
  public abstract Configuration getConf();
}
