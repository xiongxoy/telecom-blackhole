package org.hit.blackhole;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Joiner;

class Key {
	private String[] items = new String[2];
	
	public static final int LAC=0;
	public static final int CI=1;
	
	public Key(ImmutableBytesWritable row) {
		items =  Bytes.toString( row.copyBytes() ).split(",");
	}
	public Key(String key) {
		items = key.split(",");
	}
	public Key() {
	}
	public void setItem(int i, String v) {
		items[i] = v;
	}
	public String toString() {
		return Joiner.on(',').join(items);
	}
}