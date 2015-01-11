package adbfinalproject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * @author Kun Liu
 **/
public class WaitForGraph {

	private static WaitForGraph instance = null;
	private Map<String, String> waitForMap;
	private Map<String, HashSet<String>> waitedByMap;

	public String graphSnapshot() {
		StringBuffer graph = new StringBuffer("\n> Wait for map:");
		for (String TID : this.waitForMap.keySet()) {
			graph.append("\n" + TID + " is waiting for: " + waitForMap.get(TID));
		}
		for (String TID : this.waitedByMap.keySet()) {
			graph.append("\n" + TID + " is waited by: ");
			for (String waitedByTID : this.waitedByMap.get(TID)) {
				graph.append(waitedByTID + "\t");
			}
		}
		return graph.toString();
	}

	public WaitForGraph() {
		this.waitForMap = new HashMap<String, String>();
		this.waitedByMap = new HashMap<String, HashSet<String>>();
	}

	public static WaitForGraph getInstance() {
		if (instance == null) {
			instance = new WaitForGraph();
		}
		return (instance);
	}

	public HashSet<String> getWaitedBySet(String youngerTID) {
		if (this.waitedByMap.containsKey(youngerTID)) {
			return this.waitedByMap.get(youngerTID);
		} else {
			return new HashSet<String>();
		}
	}

	public String getWaitingFor(String olderTID) {
		if (this.waitForMap.containsKey(olderTID)) {
			return this.waitForMap.get(olderTID);
		} else {
			return "";
		}
	}

	public boolean checkCycle(String olderTID, String youngerTID) {
		if (this.waitForMap.containsKey(youngerTID)
				&& this.waitForMap.get(youngerTID).equals(olderTID)) {
			return true;
		}
		if (this.waitedByMap.containsKey(olderTID)
				&& this.waitedByMap.get(olderTID).contains(youngerTID)) {
			return true;
		}
		return false;
	}

	public boolean addEdge(String olderTID, String youngerTID) {
		if (this.checkCycle(olderTID, youngerTID)) {
			return false;
		} else if (this.waitForMap.containsKey(olderTID)
				&& !this.waitForMap.get(olderTID).equals(youngerTID)) {
			return false;
		} else {
			waitForMap.put(olderTID, youngerTID);
			if (this.waitedByMap.get(youngerTID) != null) {
				this.waitedByMap.get(youngerTID).add(olderTID);
			} else {
				HashSet<String> tempSet = new HashSet<String>();
				tempSet.add(olderTID);
				this.waitedByMap.put(youngerTID, tempSet);
			}
			return true;
		}
	}

	public boolean removeEdge(String olderTID, String youngerTID) {
		if (this.waitForMap.containsKey(olderTID)
				&& this.waitForMap.get(olderTID).equals(youngerTID)) {
			this.waitForMap.put(olderTID, "");
			this.waitedByMap.get(youngerTID).remove(olderTID);
			return true;
		} else {
			return false;
		}
	}

	public boolean clearEdge(String targetTID) {
		if (this.waitForMap.containsKey(targetTID)) {
			for (String olderTID : this.waitForMap.keySet()) {
				if (this.waitForMap.get(olderTID).equals(targetTID)) {
					this.waitForMap.remove(olderTID);
				}
			}
			if (this.waitedByMap.containsKey(targetTID)) {
				this.waitedByMap.remove(targetTID);
			}
			return true;
		} else {
			return false;
		}
	}

}
