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

		displayStations();
		kmean(3);
		
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

		ArrayList<Long> ts1 = new ArrayList<Long>();
		ArrayList<Long> ts2 = new ArrayList<Long>();
		ArrayList<Long> ts3 = new ArrayList<Long>();

		for (int i = 0; i < 60; i++) {
			ts1.add(Math.round(Math.random() * 100));
			ts2.add(Math.round(Math.random() * 100));
			ts3.add(Math.round(Math.random() * 100));
		}

		for (ArrayList<Long> t : timeSeries) {
			Double d1 = dtwDistance(t, ts1);
			Double d2 = dtwDistance(t, ts2);
			Double d3 = dtwDistance(t, ts3);

			System.out.println(d1 + " " + d2 + " " + d3);

			if (d1 < d2 && d1 < d3) {
				System.out.println("D1 !!");
			} else if (d2 < d1 && d2 < d3) {
				System.out.println("D2 !!");
			} else {
				System.out.println("D3 !!");
			}
		}

		/*
		3 kTS au hasard
		Pour toutes les TS :
			calcul DTW avec les 3 TS
			associer avec la kTS la plus proche

		Faire la moyenne (dba) pour les trois groupes

		LOOP

		 */

		/*ArrayList<ArrayList<Long>> testDBA = new ArrayList<ArrayList<Long>>();

		for (ArrayList<Long> t1 : timeSeries) {
			for (ArrayList<Long> t2 : timeSeries) {
				System.out.println("DTW");
				System.out.println(distance(t1, t2));
				System.out.println(dtwDistance(t1, t2));
			}
			testDBA.add(t1);
		}

		System.out.println("DBA:");
		ArrayList<Long> C = null;

		for (int i = 0; i < 4; i++) {
			C = dba(testDBA, C);
			System.out.println(timeSerieToString(C));
		}*/
	}

	public static ArrayList<Long> dba(ArrayList<ArrayList<Long>> timeSeries, ArrayList<Long> c) {
		if (c == null) {
			c = timeSeries.get(0);
		}

		int T = timeSeries.get(0).size();
		int Tprime = c.size();

		ArrayList<ArrayList<Long>> assocTable = new ArrayList<ArrayList<Long>>();
		for (int i = 0; i < T; i++) {
			assocTable.add(new ArrayList<Long>());
		}

		Double[][][][] m;
		Double[] path;
		for (ArrayList<Long> seq : timeSeries) {
			m = dtw(c, seq);
			//printDTW(m);
			int i = Tprime - 1;
			int j = T - 1;

			while (i >= 0 && j >= 0) {
				
				assocTable.get(i).add(seq.get(j));
				path = second(m[i][j]);
				i = path[0].intValue();
				j = path[1].intValue();
				/*logger.info("AAAAAAAAAAAAA");
				logger.info("{} {}", i, j);*/
			}
		}

		ArrayList<Long> Cprime = new ArrayList<Long>();
		for (int i = 0; i < T; i++) {
			Cprime.add(barycenter(assocTable.get(i)));
		}

		return Cprime;
	}

	public static Long barycenter(ArrayList<Long> listLong) {
		if (listLong.size() == 0) {
			return 0L;
		}
		Long avg = 0L;
		for (Long n : listLong) {
			avg += n;
		}
		return avg/listLong.size();
	}

	public static Double euclideanDistance(ArrayList<Long> series1, ArrayList<Long> series2, int i, int j) {
		return Math.abs(series1.get(i).doubleValue() - series2.get(j).doubleValue());
	}

	public static void printDTW(Double[][][][] dtw) {
		for (int i = 0; i < dtw.length; i++) {
			for (int j = 0; j < dtw[i].length; j++) {
				System.out.print("[" + dtw[i][j][0][0] + ", (" + dtw[i][j][1][0]+ ", " + dtw[i][j][1][1] + ")]");
			}
			System.out.println("");
		}
	}

	public static Double[][][][] dtw(ArrayList<Long> series1, ArrayList<Long> series2) {
		int n = series1.size();
		int m = series2.size();

		Double[][][][] mST = new Double[n][m][2][2];

		// Algo
		mST[0][0][0][0] = euclideanDistance(series1, series2, 0, 0);

		mST[0][0][1][0] = -1D;
		mST[0][0][1][1] = -1D;

		for (int i = 1; i < n; i++) {
			mST[i][0][0][0] = mST[i - 1][0][0][0] + euclideanDistance(series1, series2, i, 0);

			mST[i][0][1][0] = i - 1D;
			mST[i][0][1][1] = 0D;
		}

		for (int j = 1; j < m; j++) {
			mST[0][j][0][0] = mST[0][j - 1][0][0] + euclideanDistance(series1, series2, 0, j);

			mST[0][j][1][0] = 0D;
			mST[0][j][1][1] = j - 1D;
		}

		Double[][] minimum;
		for (Integer i = 1; i < n; i++) {
			for (Integer j = 1; j < m; j++) {
				minimum = minVal(mST[i - 1][j - 1], mST[i - 1][j], mST[i][j - 1]);

				mST[i][j][0][0] = first(minimum) + euclideanDistance(series1, series2, i, j);

				// Path
				//mST[i][j][1] = second(minimum);

				if (mST[i - 1][j - 1][0][0] == minimum[0][0]) {
					mST[i][j][1][0] = i.doubleValue() - 1D;
					mST[i][j][1][1] = j.doubleValue() - 1D;
				} else if (mST[i - 1][j][0][0] == minimum[0][0]) {
					mST[i][j][1][0] = i.doubleValue() - 1D;
					mST[i][j][1][1] = j.doubleValue();
				} else if (mST[i][j - 1][0][0] == minimum[0][0]) {
					mST[i][j][1][0] = i.doubleValue();
					mST[i][j][1][1] = j.doubleValue() - 1D;
				}
			}
		}

		return mST;
	}

	public static Double dtwDistance(ArrayList<Long> series1, ArrayList<Long> series2) {
		return dtw(series1, series2)[series1.size() - 1][series2.size() - 1][0][0];
	}

	public static Double first(Double[][] v) {
		return v[0][0];
	}

	public static Double[] second(Double[][] v) {
		return v[1];
	}

	public static Double[][] minVal(Double[][] v1, Double[][] v2, Double[][] v3) {
		if (first(v1) <= Math.min(first(v2), first(v3))) {
			return v1;
		} else if (first(v2) <= first(v3)) {
			return v2;
		} else {
			return v3;
		}
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
