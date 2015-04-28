package org.apache.cassandra.hirudinea;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.Iterator;
import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hirudinea
{
	public static final String KEYSPACE_NAME = "tp";
	public static final String TABLE_NAME = "station";
	public static final String NUMBER_COLUMN_NAME = "n";

	private static final Logger logger = LoggerFactory.getLogger(Hirudinea.class);

	public static final ConcurrentHashMap<Long, ArrayList<StationEntry>> stations = new ConcurrentHashMap();

	/*
	 *  Request analyze
	 */
	public static void extract(Keyspace ks, Mutation m) {

		String keyspace_name = ks.getName();
		ByteBuffer key = m.key();
		String table_name;
		String cell_name;
		ByteBuffer cell_value;

		Collection<ColumnFamily> values = m.getColumnFamilies();

        for (ColumnFamily c: values) {
            table_name = c.metadata().cfName;

            for (Cell cell: c) {
                cell_name = c.getComparator().getString(cell.name());
                cell_value = cell.value();

                analyze(keyspace_name, table_name, key, cell_name, cell_value);
            }
        }
	}

	public static void analyze(String keyspace_name, String table_name, ByteBuffer key, String cell_name, ByteBuffer cell_value) {

		Long station_id;
		Long new_value;
		Date date;

		if (keyspace_name.equals(KEYSPACE_NAME) && table_name.equals(TABLE_NAME) && cell_name.equals(NUMBER_COLUMN_NAME)) {

			station_id = ByteBufferUtil.toLong(key);
			new_value = ByteBufferUtil.toLong(cell_value);
			date = new Date();

			logger.info("Station {} modified at {}, new value: {}", station_id, date, new_value);

			addStationEntry(station_id, new_value, date);
		}
	}

	/*
	 *  Stations
	 */
	public static void addStationEntry(Long station_id, Long value, Date date) {

		ArrayList<StationEntry> entries;
		if (stations.get(station_id) == null) {
			entries = new ArrayList<StationEntry>();
		}
		else {
			entries = stations.get(station_id);
		}

		entries.add(new StationEntry(value, date));
		stations.put(station_id, entries);

		displayStations();
	}

	public static void displayStations() {
		logger.info("-- Display stations --");
		for (Map.Entry<Long, ArrayList<StationEntry>> entry : stations.entrySet()) {
			Long key = entry.getKey();
			ArrayList<StationEntry> value = entry.getValue();

			logger.info("\t Station {}", key);
			logger.info("\t\t Time serie: {}", timeSerieToString(getTimeSerie(key, 1000L*60, 1000L)));

			for (StationEntry se: value) {
				logger.info("\t\t {}: {}", se.date, se.value);
			}
		}
	}

	/*
	 *  Time series
	 */
	public static ArrayList<Long> getTimeSerie(Long station_id, Date start, Date end, Long interval) {
		ArrayList<StationEntry> list_entries = stations.get(station_id);

		ArrayList<Long> time_serie = new ArrayList<Long>();

		Long total_number = (end.getTime() - start.getTime()) / interval;

		for (int i = 0; i < total_number; i++) {
			time_serie.add(0L);
		}

		for (StationEntry se: list_entries) {
			int index = (int) ((se.date.getTime() - start.getTime()) / interval);
			for (int i = index; i < time_serie.size(); i++) {
				if (i >= 0) {
					time_serie.set(i, se.value);
				}
			}
		}

		return time_serie;
	}

	public static String timeSerieToString(ArrayList<Long> ts) {
		String s = "[";

		for (Long l: ts) {
			s += l + ", ";
		}
		s += "]";

		return s;
	}

	public static ArrayList<Long> getTimeSerie(Long station_id, Long duration, Long interval) {

		Date start = new Date((new Date()).getTime() - duration);
		Date end = new Date();

		return getTimeSerie(station_id, start, end, interval);

	}
}
