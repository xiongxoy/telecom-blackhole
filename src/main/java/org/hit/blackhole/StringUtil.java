package org.hit.blackhole;

import java.util.ArrayList;
import java.util.List;

class StringUtil {
	static List<String> split(String s, char c) {
		List<String> items = new ArrayList<String>();
            int pos = 0, end;
            while ((end = s.indexOf(',', pos)) >= 0) {
                items.add(s.substring(pos, end));
                pos = end + 1;
            }
            items.add( s.substring(pos, s.length()) );
            
		return items;
	}
}