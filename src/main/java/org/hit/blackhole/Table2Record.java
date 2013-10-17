package org.hit.blackhole;

import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;

/* 
 * Same as Table1Key
 */

class Table2Value {
	String [] items = new String[2]; 
	
	public static final int PAGING_RSP_NUM = 0; 
	public static final int TCH_CONGESTION_NUM = 1; 
	
	public Table2Value(Iterable<Text> values) {
		long pagingRspNum=0;
		long TCHCongestionNum=0;
		
		for( Text v:values ) {
			String [] s = v.toString().split(",");
			pagingRspNum += (s[0].charAt(0) - '0');
			TCHCongestionNum += (s[1].charAt(1) - '0'); 
		}
		
		items[PAGING_RSP_NUM] = String.valueOf(pagingRspNum);
		items[TCH_CONGESTION_NUM] = String.valueOf(TCHCongestionNum);
	}
	public String toString() {
		return Joiner.on(',').join(items);
	}
}
public class Table2Record {

	private Record record;
	private boolean valid;
	private String key;

	/**
	 * (8)根据条件选出数据表中对应的行
	 * where (( t1.dtSTime >= '2012-03-02 08:00:00.000' and t1.dtSTime < '2012-03-02 09:00:00.000' )) and intFirstCI<>0 
	 * group by intFirstLac , intFirstCi

	 * (9)在(8)中选出满足条件的列，并定义寻呼响应总次数和TCH信道拥塞次数，同时将结果加载到表tmpAnalyse_1350284638652_u 中
	 * select intFirstLac , intFirstCi ,sum(case when biKpiFlag&power(2,30)>0 then 1 else 0 end) as 寻呼响应总次数(pagingRspNum)
	 * sum(case when biKpiFlag&power(2,22)>0 or biKpiFlag&power(2,25)>0 then 1.0 else 0.0 end) as TCH信道拥塞次数 (TCHCongestionNum)
	 * into #tmpAnalyse_1350284638652_u

	 * (10) 排序 intFirstLac , intFirstCi
	 * group by intFirstLac , intFirstCi

	 * @param s
	 */
	public String getPagingRspNum() {
		String biKpiFlag = record.getItem(Record.BI_KPI_FLAG);
		if (biKpiFlag.charAt(30) > '0') {
			return "1";
		} else {
			return "0";
		}
	}
	public String getTCHCongestionNum() {
		String biKpiFlag = record.getItem(Record.BI_KPI_FLAG);
		if (biKpiFlag.charAt(22) > '0' || biKpiFlag.charAt(25) > '0') {
			return "1";
		} else {
			return "0";
		}
	}
	public Table2Record(String s) {
		record = new Record(s);
		check();
		setKey();
	}
	private void setKey() {
		String intFirstLAC = record.getItem(Record.INT_FIRST_LAC);
		String intFirstCI = record.getItem(Record.INT_FIRST_CI);
		
		key = intFirstLAC + "," + intFirstCI;
	}
	private void check() {
		if ( checkTime() && checkCI() ) {
			valid = true;
		} else {
			valid = false;
		}
	}
	private boolean checkCI() {
		String intFirstCI = record.getItem(Record.INT_FIRST_CI);
		if ( intFirstCI.compareTo("0") != 0 ) {
			return true;
		} else {
			return false;
		}
	}
	private boolean checkTime() {
		String start = record.getItem(Record.DT_S_TIME);
		if (start.compareTo("2012-03-02 08:00:00.000") >= 0 
				&& start.compareTo("2012-03-02 09:00:00.000") < 0) {
			return true;
		} else {
			return false;
		}
	}
	public boolean isValid() {
		return valid;
	}
	public String getKey() {
		return key;
	}
}
