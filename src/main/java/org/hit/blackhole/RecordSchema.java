package org.hit.blackhole;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Joiner;

public class RecordSchema {
	public static final byte[] TABLE_NAME = Bytes.toBytes("raw_data");
	public static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
	public static final byte[] ATTRIBUTE = Bytes.toBytes("data");
	
	public static final int DT_S_TIME = 0;
	public static final int DT_E_TIME = 1;
	public static final int INT_SESS_TYPE = 2;
	public static final int INT_FIRST_CI = 3;
	public static final int INT_END_CI = 4;
	public static final int INT_SMS_LEN = 5;
	public static final int INT_DURATION = 6;
	public static final int VC_CALLED_IMSI= 7;
	public static final int INT_FIRST_LAC = 8;
	public static final int INT_END_LAC = 9;
	public static final int BI_KPI_FLAG = 10;
	
	private List<String> items = new ArrayList<String>(11);
	
	public RecordSchema(String s) {
		items = StringUtil.split(s, ',');
	}
	public RecordSchema(Record r) {
		items.set( DT_S_TIME, r.getItem(Record.DT_S_TIME) );
		items.set( DT_E_TIME, r.getItem(Record.DT_E_TIME) );
		items.set( INT_SESS_TYPE, r.getItem(Record.INT_SESS_TYPE) );
		items.set( INT_FIRST_CI, r.getItem(Record.INT_FIRST_CI) );
		items.set( INT_END_CI, r.getItem(Record.INT_END_CI) );
		items.set( INT_SMS_LEN, r.getItem(Record.INT_SMS_LEN) );
		items.set( INT_DURATION, r.getItem(Record.INT_DURATION) );
		items.set( VC_CALLED_IMSI, r.getItem(Record.VC_CALLED_IMSI) );
		items.set( INT_FIRST_LAC, r.getItem(Record.INT_FIRST_LAC) );
		items.set( INT_END_LAC, r.getItem(Record.INT_END_LAC) );
		items.set( BI_KPI_FLAG, r.getItem(Record.BI_KPI_FLAG) );
	}
	
	public String toString() {
		return Joiner.on(',').join(items);
	}

	public String getItem(int i) {
		return items.get(i);
	}

	public void setItem(int i, String v) {
		items.set(i, v);
	}
}
