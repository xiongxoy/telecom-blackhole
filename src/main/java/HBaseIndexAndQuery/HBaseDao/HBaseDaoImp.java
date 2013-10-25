package HBaseIndexAndQuery.HBaseDao;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDaoImp extends Configured
  implements HBaseDao
{
  private Configuration configuration = null;
  private String master = null;
  private String zookeeper = null;
  private String port = null;
  private HBaseAdmin admin = null;
  private HTableDescriptor htd = null;
  ByteBuffer test = null;
  ByteArrayOutputStream out = new ByteArrayOutputStream();
  int length = 0;

  public HBaseDaoImp(String lmaster, String lzookeeper, String lport)
  {
    this.master = lmaster;
    this.zookeeper = lzookeeper;
    this.port = lport;
    System.out.println("init is ok" + this.master + " " + this.zookeeper + " " + this.port);
  }
 
  public static HBaseDaoImp GetDefaultDao()
  {
	   String defaultMaster = "10.120.68.24:8020";
	   String defaultZookeeper = "10.120.68.24,10.120.31.97,10.120.68.27";
	   String defaultPort = "2181";
	   HBaseDaoImp reslut = new HBaseDaoImp(defaultMaster, defaultZookeeper , defaultPort);
	   reslut.CreateHbaseConnect();
	   return reslut;
  }
  
  public void CreateHbaseConnect()
  {
    try
    {
      this.configuration = getConf(); 
    }
    catch (Exception e) {
      System.out.println("connect is error");
      e.printStackTrace();
    }
  }

  public boolean TableExists(byte[] tableName)
  {
	  try
	  {
		  this.admin = new HBaseAdmin(this.configuration);
		  if(this.admin.tableExists(tableName))
		   {
			  return true;
		   }

	  }catch (Exception e) {
		e.printStackTrace();
		return false;
	}
	return false;
  }

  public void CreateTable(byte[] strtable, boolean isdelete)
  {
	  try
	  {
		  this.admin = new HBaseAdmin(this.configuration);
		  if ((this.admin.tableExists(strtable)) && (!isdelete)) {
			  System.out.println("##################################################" +
					  "#########table " +Bytes.toString(strtable) + " exists" +
					  "##################################################");
			  return;
		  }
		  if ((this.admin.tableExists(strtable)) && (isdelete)) {
			  System.out.println("delete " + Bytes.toString(strtable) );
			  this.admin.disableTable(strtable);
			  this.admin.deleteTable(strtable);
		  }

		  System.out.println("##################################################" +
				  "#########create "+ Bytes.toString(strtable) +  
				  "##################################################");
		  this.admin.createTable(new HTableDescriptor(strtable));
	  }
	  catch (Exception e) {
		  e.printStackTrace();
	  }
  }

  public void TableAddFaminly(byte[] table, byte[] family)
  {
    try
    {
      if (this.htd == null)
      {
        this.htd = this.admin.getTableDescriptor(table);
      }
      if (!htd.hasFamily(family)) {
    	  this.htd.addFamily(new HColumnDescriptor(family));
    	  this.admin.disableTable(table);
    	  this.admin.modifyTable(table, this.htd);
    	  this.admin.enableTable(table);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public HTable getHBaseTable(byte[] tableName) throws IOException
  {
    return new HTable(this.configuration, tableName);
  }


  public void TableAddData(byte[] table, byte[] row, byte[] family, byte[] column, byte[] value)
    throws IOException
  {
    System.out.println("begin insert one data");
    for (int i = 0; i < value.length; i++)
    {
      System.out.println(family + "  " + column + "  " + value[i]);
    }
    HTable htable = new HTable(this.configuration, table);
    Put p = new Put(row);
    p.add(family, column, value);
    htable.put(p);
    htable.flushCommits();
    htable.close();
    System.out.println("end insert one data");
  }

  void InitByte(byte[] buffer, int length)
  {
    for (int i = 0; i < length; i++)
    {
      buffer[i] = (byte)(i & 0xFF);
    }
  }

  public static byte[] long2bytes(long num) {
    byte[] b = new byte[8];
    for (int i = 0; i < 8; i++) {
      b[i] = (byte)(int)(num >>> 56 - i * 8);
    }
    return b;
  }

  public static void main(String[] args)
  {
    try
    {
    	HBaseDaoImp hdi = HBaseDaoImp.GetDefaultDao();
    	hdi.CreateHbaseConnect();
    	HTable table = hdi.getHBaseTable("hbase_bssap_cdr_CalledIndex".getBytes());
		Scan scan = new Scan();

		ResultScanner scanner = table.getScanner(scan);
		System.out.println("I am  here");
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

@Override
public Result getTableResult(byte[] table, byte[] row) throws IOException {
	HTable htable = getHBaseTable(table);
	Get g = new Get(row);
	return htable.get(g);
}
	
@Override
public byte[] getTableValue(byte[] table, byte[] row, byte[] family, byte[] column) throws IOException {
	Result r = getTableResult(table, row);
	return r.getColumnLatest(family, column).getValue();
}

@Override
public boolean hasRow(byte[] table, byte[] row) throws IOException {
	Result r  = getTableResult(table, row);
	return r.isEmpty();
}

@Override
public void setTableValue(byte[] table, byte[] row, byte[] columnFamily,
		byte[] column, byte[] value) throws IOException {
	Put put = new Put(row);
	put.add(columnFamily, column, value);
	HTable hBaseTable = getHBaseTable(table);
	hBaseTable.put(put);
}


}
