package org.hit.blackhole;

import org.apache.hadoop.conf.Configuration;

import HBaseIndexAndQuery.HBaseDao.HBaseDao;
import HBaseIndexAndQuery.HBaseDao.HBaseDaoImp;

public class HBaseConnection {

	private static HBaseDao dao = null;

	public static HBaseDao getDao() {
		if (dao == null) {
			System.err.println("No Dao Here");
			System.exit(-1);
		}
		return dao;
	}
	public static HBaseDao getDao(Configuration conf) {
		if (dao == null) {
			System.err.println("Use default Hbase.");
			dao = HBaseDaoImp.GetDefaultDao(conf);
		}
		return dao;
	}
	
}
