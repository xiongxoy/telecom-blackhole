package HBaseIndexAndQuery.HBaseDao;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDaoImp
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
	   String defaultMaster = "192.168.1.8:60000";
	   String defaultZookeeper = "192.168.1.8,192.168.1.14,192.168.1.16";
	   String defaultPort = "2181";
	   HBaseDaoImp reslut = new HBaseDaoImp(defaultMaster, defaultZookeeper , defaultPort);
	   reslut.CreateHbaseConnect();
	   return reslut;
  }

  public void CreateHbaseConnect()
  {
    try
    {
      this.configuration = HBaseConfiguration.create();
      this.configuration.set("hbase.master", this.master);
      this.configuration.set("hbase.zookeeper.quorum", this.zookeeper);
      this.configuration.set("hbase.zookeeper.property.clientPort", this.port);
      System.out.println("connect is ok");
      System.out.println("before init class");

      System.out.println("init class is OK");

    }
    catch (Exception e) {
      System.out.println("connect is error");
      e.printStackTrace();
    }
  }

  public boolean  TableExists(String tableName)
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


  public void CreateTable(String strtable, boolean isdelete)
  {
    try
    {
      this.admin = new HBaseAdmin(this.configuration);
      if ((this.admin.tableExists(strtable)) && (!isdelete))
      {
        System.out.println("table is exists");
        return;
      }
      if ((this.admin.tableExists(strtable)) && (isdelete)) {
        System.out.println("É¾³ý " + strtable);
        this.admin.disableTable(strtable);
        this.admin.deleteTable(strtable);
      }

      System.out.println("´´½¨ table");
      this.admin.createTable(new HTableDescriptor(strtable.getBytes()));
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void TableAddFaminly(String table, String family)
  {
    try
    {
      if (this.htd == null)
      {
        this.htd = this.admin.getTableDescriptor(table.getBytes());
      }
      this.htd.addFamily(new HColumnDescriptor(family));
      this.admin.disableTable(table.getBytes());
      this.admin.modifyTable(table.getBytes(), this.htd);
      this.admin.enableTable(table.getBytes());
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public HTable getHBaseTable(String tableName) throws IOException
  {
    return new HTable(this.configuration, tableName);
  }

  public void TableAddData(String table, String row, String family, String column, byte[] value)
    throws IOException
  {
	  TableAddData(table, Bytes.toBytes(row), family, column, value);
  }

  public void TableAddData(String table, byte[] row, String family, String column, byte[] value)
    throws IOException
  {
    System.out.println("begin insert one data");
    for (int i = 0; i < value.length; i++)
    {
      System.out.println(family + "  " + column + "  " + value[i]);
    }
    HTable htable = new HTable(this.configuration, table);
    byte[] bfamily = Bytes.toBytes(family);
    byte[] bcolumn = Bytes.toBytes(column);
    Put p = new Put(row);
    p.add(bfamily, bcolumn, value);
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
    	HTable table = hdi.getHBaseTable("hbase_bssap_cdr_CalledIndex");
		Scan scan = new Scan();

		ResultScanner scanner = table.getScanner(scan);
		System.out.println("I am  here");
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }


}
