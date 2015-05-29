package org.apache.cassandra.hirudinea;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.Iterator;
import java.nio.ByteBuffer;
import java.util.Date;
import java.lang.Math;

import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weka.clusterers.SimpleKMeans;

public class Hirudinea
{
	public static final String KEYSPACE_NAME = "tp";
	public static final String TABLE_NAME = "station";
	public static final String NUMBER_COLUMN_NAME = "n";
	public static final Long TIME_WINDOW = 1000L*60;
	public static final Long INTERVAL = 1000L;

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

		kmean(3);
		//displayStations();
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

	public static void kmean(int n) {

		ArrayList<ArrayList<Long>> timeSeries = new ArrayList<ArrayList<Long>>();

		for (Long s_id : stations.keySet()) {
			timeSeries.add(getTimeSerie(s_id, TIME_WINDOW, INTERVAL));
		}

		for (ArrayList<Long> t1 : timeSeries) {
			for (ArrayList<Long> t2 : timeSeries) {
				System.out.println(distance(t1, t2));
			}
		}

	}

	public static ArrayList<Long> dba(ArrayList<ArrayList<Long>> timeSeries) {

		return new ArrayList<Long>(0);
	}

	public static Double distance(ArrayList<Long> series1, ArrayList<Long> series2) {
		int n = series1.size();
		int m = series2.size();

		ArrayList<ArrayList<Double>> dtw = new ArrayList<ArrayList<Double>>(n); 
		for (int i = 0; i < n; i++) {
			ArrayList<Double> row = new ArrayList<Double>(m);
			for (int j = 0; j < m; j++) {
				if (j == 0 || i == 0) {
					row.add(Double.POSITIVE_INFINITY); 
				}
				else{
					row.add(0D); 
				}
				
			}
			dtw.add(row);
		}

		dtw.get(0).set(0, 0D);

		for (int i = 1; i < n; i++) {
			for (int j = 1; j < m ; j++) {
				Double cost = Math.abs(series1.get(i).doubleValue() - series2.get(j).doubleValue());
				dtw.get(i).set(j, cost + (Double)(Math.min(Math.min(dtw.get(i-1).get(j), dtw.get(i).get(j-1)), dtw.get(i-1).get(j-1))));
			}
		}

		return dtw.get(n-1).get(m-1);
	}
}
