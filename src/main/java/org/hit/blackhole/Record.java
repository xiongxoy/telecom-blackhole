package org.hit.blackhole;

import java.util.List;


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
//	20121210100021002
//	20121210130821000
	public static final String T_START = "2012-12-10 11:00:00.000";
	public static final String T_END = "2012-12-10 12:00:00.000";
//	public static final String T_START = "2012-03-02 08:00:00.000";
//	public static final String T_END = "2012-03-02 09:00:00.000";
	
	private List<String> items;

	/**
	 * @param s
	 */
	public Record( String s ) {
		items = StringUtil.split(s, ',');
	}
	
	public String getItem(int i) {
		return items.get(i);
	}
	
	public boolean isValid() {
		return checkTime();
	}
	boolean checkTime() {
		String start = this.getItem(DT_S_TIME);
		if (start.compareTo(Record.T_START) >= 0 
				&& start.compareTo(Record.T_END) < 0) {
			return true;
		}
		return false;
	}

}
