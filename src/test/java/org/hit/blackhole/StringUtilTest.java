package org.hit.blackhole;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.Test;

public class StringUtilTest {
	
	@Test
	public void test_time_convert() {
		String s1 = "2012-03-02 09:00:00.000";
		String s2 = "20120302090000000";
		String time_new = convetTimeFormat(s1);
		assertEquals(s2, time_new);
	}
	
	
	@Test 
	public void test_get_time() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		String s = "ftbBssRanSession.out.20121210125621003.??????";
		HBasePagingDataMapper importer = new HBasePagingDataMapper("AAA");
		Method method = HBasePagingDataMapper.class.getDeclaredMethod("getTime", String.class);
		method.setAccessible(true);
		String ret = (String)method.invoke(importer, (Object)s);
		
		assertEquals("20121210125621003" , ret);
	}
	private String convetTimeFormat(String time) {
//		"2012-03-02 09:00:00.000"
//		"20120302090000000"
		String time_new = "";
		time_new += time.substring(0, 4); // Year
		time_new += time.substring(5, 7); // Month
		time_new += time.substring(8, 10); // Day
		time_new += time.substring(11, 13); // Hour
		time_new += time.substring(14, 16); // Minute
		time_new += time.substring(17, 19); // Second
		time_new += time.substring(20, 23); // MilliSecond
		
		return time_new;
	}
	@Test
	public void test() {
		String [] a = null;
		String [] b = {"12", "12" ,"12"};
		a = StringUtil.split("12,12,12", ',').toArray(new String[0]);
		System.out.println(a.length);
		System.out.println(b.length);
		assertArrayEquals(b, a);
	}
	
	@Test
	public void test2() {
		String [] a = null;
		String [] b = {"", "" ,""};
		a = StringUtil.split(",,", ',').toArray(new String[0]);
		System.out.println(a.length);
		System.out.println(b.length);
		assertArrayEquals(b, a);
	}

	@Test
	public void test3() {
		String [] a = null;
		String [] b = {"a","","","b"};
		a = StringUtil.split("a,,,b", ',').toArray(new String[0]);
		System.out.println(a.length);
		System.out.println(b.length);
		assertArrayEquals(b, a);
	}
}
