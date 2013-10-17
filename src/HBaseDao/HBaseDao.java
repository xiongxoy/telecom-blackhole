package HBaseIndexAndQuery.HBaseDao;
import java.io.IOException;
import java.util.List;

public abstract interface HBaseDao
{
  public abstract void CreateHbaseConnect();

  public abstract void CreateTable(String paramString, boolean paramBoolean);

  public abstract void TableAddFaminly(String paramString1, String paramString2);

  public abstract void TableAddData(String paramString1, String paramString2, String paramString3, String paramString4, byte[] paramArrayOfByte)
    throws IOException;

}
