package org.hit.blackhole;

import java.io.IOException;

import HBaseIndexAndQuery.HBaseDao.HBaseDao;
import HBaseIndexAndQuery.HBaseDao.HBaseDaoImp;

public class HBaseConnection {

	private static HBaseDao dao = null;
	
	static boolean hasRow(byte[] table, byte[] row) throws IOException {
		return getDao().hasRow(table, row);
	}

	public static HBaseDao getDao() {
		if (dao == null) {
			System.err.println("Use default Hbase.");
			dao = HBaseDaoImp.GetDefaultDao();
		}
		return dao;
	}
	
}
