package org.hit.blackhole;

import java.util.List;

import com.google.common.base.Splitter;

public class Record {
	public static final int DT_S_TIME = 6-1;
	public static final int DT_E_TIME = 7-1;
	public static final int INT_SESS_TYPE = 32-1;
	public static final int INT_FIRST_CI = 20-1;
	public static final int INT_END_CI = 22-1;
	public static final int INT_SMS_LEN = 57-1;
	public static final int INT_DURATION = 53-1;
	public static final int VC_CALLED_IMSI= 27-1;
	public static final int INT_FIRST_LAC = 19-1;
	public static final int INT_END_LAC = 21-1;
	public static final int BI_KPI_FLAG = 51-1;
	
	private List<String> items;

	/**
	 * XXX
	 * @param s
	 */
	public Record( String s ) {
		items = Splitter.on(',').splitToList(s);
	}
	
	public String getItem (int i) {
		return items.get(i);
	}

}
