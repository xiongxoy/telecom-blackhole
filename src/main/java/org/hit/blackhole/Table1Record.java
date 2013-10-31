package org.hit.blackhole;

import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;


class Table1Value {
	private String [] items = new String[9];
	
	// Reduce之前需要计算的量
	public static final int PAGING_FAIL_CI_NUM = 3; 
	// Reduce之后才需要求的量
	public static final int PAGING_NUM = 8;  
	public static final int PAGING_FAIL_NUM = 1; 
	public static final int PAGING_FAIL_RATE = 2; 
	public static final int PAGING_FAIL_CI_RATE = 4; 
	public static final int PAGING_FAIL_USER_NUM = 5; 
	public static final int PAGING_FAIL_USER_RATE = 6; 
	public static final int TCH_CONGESTION_NUM = 7; 
	public static final int ROW_NUM = 0;
	
	public Table1Value(Iterable<Text> values, long row_num) {
		long pagingFailCINum = 0;
		Set<String> calledIMSIs = new TreeSet<String>();
		long pagingFailNum = 0;
		long pagingFailUserNum = 0;
		double pagingFailCIRate = 0;
		double pagingFailUserRate = 0;
		
		for(Text v:values) {
			System.out.println(v);
			String [] s = v.toString().split(",");
			pagingFailCINum += (s[0].charAt(0) - '0');
			calledIMSIs.add(s[1]);
			pagingFailNum++;
		}
		
		pagingFailUserNum = calledIMSIs.size();
		if ( pagingFailNum != 0 ) {
			pagingFailCIRate = pagingFailCINum * 1.0 / pagingFailNum;
			pagingFailUserRate = pagingFailUserNum * 1.0 / pagingFailNum;
		} else {
			pagingFailCIRate = 0;
			pagingFailUserRate = 0;
		}
		
		items[PAGING_NUM] = "0";
		items[PAGING_FAIL_NUM] = String.valueOf(pagingFailNum);
		items[PAGING_FAIL_RATE] = "0";
		items[PAGING_FAIL_CI_RATE] = String.valueOf(pagingFailCIRate);
		items[PAGING_FAIL_CI_NUM] = String.valueOf(pagingFailCINum);
		items[PAGING_FAIL_USER_NUM] = String.valueOf(pagingFailUserNum);
		items[PAGING_FAIL_USER_RATE] = String.valueOf(pagingFailUserRate);
		items[TCH_CONGESTION_NUM] = "0";
		items[ROW_NUM] = String.valueOf(row_num);
	}
	public Table1Value(String value) {
		items = value.split(",");
	}
	public void setItem(int i, String v) {
		items[i] = v;
	}
	public String getItem(int i) {
		return items[i];
	}
	public String toString() {
		System.out.println(Joiner.on(',').join(items));
		return Joiner.on(',').join(items);
		
	}
}

public class Table1Record {
	private boolean valid;
	private Key key;
	private int SMSLen;
	private int duration;
	private RecordSchema record;
	
//	public static final byte[] TABLE_NAME = Bytes.toBytes("black_table1");
	public static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
	public static final byte[] ATTRIBUTE = Bytes.toBytes("record");
	
	/**
	 * @param s
	 */
	public Table1Record(String s, int duration2, int SMSLen2) {
		record = new RecordSchema(s);
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
		String intFirstCI = getRecord().getItem(RecordSchema.INT_FIRST_CI);
		String intEndCI = getRecord().getItem(RecordSchema.INT_END_CI);
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
		int SMSLen2 = Integer.parseInt( getRecord().getItem(RecordSchema.INT_SMS_LEN) );
		int duration2 = Integer.parseInt( getRecord().getItem(RecordSchema.INT_DURATION) );
		if ( SMSLen2 <= SMSLen && duration2 <= duration ) {
			return true;
		}
		return false;
	}
	private boolean checkCI() {
		String firstCI = getRecord().getItem(RecordSchema.INT_FIRST_CI);
		String endCI = getRecord().getItem(RecordSchema.INT_END_CI);
		if ( firstCI.compareTo("0") != 0 || endCI.compareTo("0") != 0 ) {
			return true;
		}
		return false;
	}
	private boolean checkSessType() {
		String sessType = getRecord().getItem(RecordSchema.INT_SESS_TYPE);
		if ( sessType.compareTo("17") == 0 ) {
			return true;
		}	
		return false;
	}
	private void setKey() {
		String intFirstCI = getRecord().getItem(RecordSchema.INT_FIRST_CI);
		String intEndCI = getRecord().getItem(RecordSchema.INT_END_CI);
		String intDuration = getRecord().getItem(RecordSchema.INT_DURATION);
		String intSMSLen = getRecord().getItem(RecordSchema.INT_SMS_LEN);
		String intFirstLAC = getRecord().getItem(RecordSchema.INT_FIRST_LAC);
		String intEndLAC = getRecord().getItem(RecordSchema.INT_END_LAC);
		String LAC, CI;
		
		if ( (intFirstCI.compareTo("0") > 0 && intEndCI.compareTo("0") == 0) || 
			 (intDuration.compareTo(intSMSLen) < 0 && intFirstCI.compareTo("0") != 0 && intEndCI.compareTo("0") != 0)) {	
			LAC = intFirstLAC;
			CI = intFirstCI;
		} else {
			LAC = intEndLAC;
			CI = intEndCI;
		}
		
		key = new Key();
		key.setItem(Key.CI, CI);
		key.setItem(Key.LAC, LAC);
	}
	public String getKey() { 
		return key.toString();
	}
	private boolean checkTime() {
		String start = getRecord().getItem(RecordSchema.DT_S_TIME);
		if (start.compareTo(Record.T_START) >= 0 
				&& start.compareTo(Record.T_END) < 0) {
			return true;
		}
		return false;
	}
	public String getItem(int i) {
		return record.getItem(i);
	}
	private RecordSchema getRecord() {
		return record;
	}
}
