package org.apache.cassandra.hirudinea;

import java.util.Date;

public class StationEntry {
	public Long value;
	public Date date;

	public StationEntry(Long value, Date date) {
		this.value = value;
		this.date = date;
	}
}