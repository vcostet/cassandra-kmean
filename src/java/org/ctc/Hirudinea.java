
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public static class Hirudinea
{
	private static final ConcurrentHashMap<String, Array> stations = new ConcurrentHashMap();

	public void newStationEntry(String station, Array entry) {
        Hirudinea.stations.putIfAbsent(station, entry);
    }




}