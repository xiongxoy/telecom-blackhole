package org.hit.blackhole;

import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.el.Logger;
import org.apache.commons.logging.Log;
import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;

class Table1Key {
	private String[] items = new String[2];
	
	public static final int LAC=0;
	public static final int CI=1;
	
	public Table1Key() {
	}
	public Table1Key(String key) {
		items = key.split(",");
	}
	public void set(int i, String v) {
		items[i] = v;
	}
	public String toString() {
		return Joiner.on(',').join(items);
	}
}
class Table1Value {
	private String [] items = new String[9];
	
	// Reduce之前需要计算的量
	public static final int PAGING_FAIL_CI_NUM = 3; 
	// Reduce之后才需要求的量
	public static final int PAGING_NUM = 0;  
	public static final int PAGING_FAIL_NUM = 1; 
	public static final int PAGING_FAIL_RATE = 2; 
	public static final int PAGING_FAIL_CI_RATE = 4; 
	public static final int PAGING_FAIL_USER_NUM = 5; 
	public static final int PAGING_FAIL_USER_RATE = 6; 
	public static final int TCH_CONGESTION_NUM = 7; 
	public static final int ROW_NUM = 8;
	
	public Table1Value(Iterable<Text> values, long row_num) {
		long pagingFailCINum = 0;
		Set<String> calledIMSIs = new TreeSet<String>();
		long pagingFailNum = 0;
		double pagingFailCIRate = 0;
		
		
		for(Text v:values) {
			System.out.println(v);
			String [] s = v.toString().split(",");
			pagingFailCINum += (s[0].charAt(0) - '0');
			calledIMSIs.add(s[1]);
			pagingFailNum++;
		}
		
		items[PAGING_NUM] = "0";
		items[PAGING_FAIL_NUM] = String.valueOf(pagingFailNum);
		items[PAGING_FAIL_RATE] = "0";
		if ( pagingFailNum != 0 ) {
			pagingFailCIRate = pagingFailCINum * 1.0 / pagingFailNum;
		} else {
			pagingFailCIRate = 0;
		}
		items[PAGING_FAIL_CI_RATE] = String.valueOf(pagingFailCIRate);
		items[PAGING_FAIL_CI_NUM] = String.valueOf(pagingFailCINum);
		items[PAGING_FAIL_USER_NUM] = String.valueOf(calledIMSIs.size());
		items[PAGING_FAIL_USER_RATE] = "0";
		items[TCH_CONGESTION_NUM] = "0";
		items[ROW_NUM] = String.valueOf(row_num);
	}
	public Table1Value(String value) {
		items = value.split(",");
	}
	public void set(int i, String v) {
		items[i] = v;
	}
	public String toString() {
		System.out.println(Joiner.on(',').join(items));
		return Joiner.on(',').join(items);
		
	}
}

public class Table1Record {
	private boolean valid;
	private Table1Key key;
	private int SMSLen;
	private int duration;
	private Record record;
	
	/**
	 * XXX 这里分裂的方法可以改进，应该用indexof来改写
	 * @param s
	 */
	public Table1Record(String s, int SMSLen2, int duration2) {
		record = (new Record(s));
		SMSLen = SMSLen2;
		duration = duration2;
		check();
		setKey();
	}
	private void check() {
		valid = false;
		if ( checkTime() 
				&& checkSessType() 
				&& checkCI()
				&& checkThreshold()) {
			valid = true;
		}
	}
	/**
	 * get PagingFailCINum for this record only, 
	 * when intFirstCi=intEndCi, return "1", else return "0"
	 */
	public String getPagingFailCINum() {
		String intFirstCI = getRecord().getItem(Record.INT_FIRST_CI);
		String intEndCI = getRecord().getItem(Record.INT_END_CI);
		if ( intFirstCI.compareTo(intEndCI) == 0 ) {
			return "1";
		} else {
			return "0";
		}
	}
	public boolean isValid() {
		return valid;
	}
	private boolean checkThreshold() {
		int SMSLen2 = Integer.parseInt( getRecord().getItem(Record.INT_SMS_LEN) );
		int duration2 = Integer.parseInt( getRecord().getItem(Record.INT_DURATION) );
		if ( SMSLen2 <= SMSLen && duration2 <= duration ) {
			return true;
		}
		return false;
	}
	private boolean checkCI() {
		String firstCI = getRecord().getItem(Record.INT_FIRST_CI);
		String endCI = getRecord().getItem(Record.INT_END_CI);
		if ( firstCI.compareTo("0") != 0 || endCI.compareTo("0") != 0 ) {
			return true;
		}
		return false;
	}
	private boolean checkSessType() {
		String sessType = getRecord().getItem(Record.INT_SESS_TYPE);
		if ( sessType.compareTo("17") == 0 ) {
			return true;
		}	
		return false;
	}
	private void setKey() {
		String intFirstCI = getRecord().getItem(Record.INT_FIRST_CI);
		String intEndCI = getRecord().getItem(Record.INT_END_CI);
		String intDuration = getRecord().getItem(Record.INT_DURATION);
		String intSMSLen = getRecord().getItem(Record.INT_SMS_LEN);
		String intFirstLAC = getRecord().getItem(Record.INT_FIRST_LAC);
		String intEndLAC = getRecord().getItem(Record.INT_END_LAC);
		String LAC, CI;
		
		if ( (intFirstCI.compareTo("0") > 0 && intEndCI.compareTo("0") == 0) || 
			 (intDuration.compareTo(intSMSLen) < 0 && intFirstCI.compareTo("0") != 0 && intEndCI.compareTo("0") != 0)) {	
			LAC = intFirstLAC;
			CI = intFirstCI;
		} else {
			LAC = intEndLAC;
			CI = intEndCI;
		}
		
		key = new Table1Key();
		key.set(Table1Key.CI, CI);
		key.set(Table1Key.LAC, LAC);
	}
	public String getKey() { 
		return key.toString();
	}
	private boolean checkTime() {
		String start = getRecord().getItem(Record.DT_S_TIME);
		if (start.compareTo("2012-03-02 08:00:00.000") >= 0 
				&& start.compareTo("2012-03-02 09:00:00.000") < 0) {
			return true;
		}
		return false;
	}
	public String getItem(int i) {
		return record.getItem(i);
	}
	private Record getRecord() {
		return record;
	}
}
