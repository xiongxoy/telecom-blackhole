package org.hit.blackhole;

import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class JobRunner implements Runnable {
	private JobControl jc;
	JobRunner(JobControl jc) {
		this.jc = jc;
	}
	@Override
	public void run() {
		jc.run();
	}
}
