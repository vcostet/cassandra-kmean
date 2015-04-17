import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.Iterator;

public class Hirudinea
{
	private static final ConcurrentHashMap<String, ArrayList> stations = new ConcurrentHashMap();

	public static void newStationEntry(String nom_station, ArrayList entry) {
		if (stations.get(nom_station) == null) {
			ArrayList<ArrayList> info_station = new ArrayList<ArrayList>();
			info_station = stations.get(nom_station);
			info_station.add(entry);
			stations.put(nom_station, info_station);
		}
		else {
			ArrayList<ArrayList> info_station = new ArrayList<ArrayList>();
			info_station.add(entry);
			stations.putIfAbsent(nom_station, info_station);
		}

    }


}