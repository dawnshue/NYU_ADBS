package adbfinalproject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author Kun Liu
 **/
public class Site {

	public static Set<Integer> getVarReplicationSite(int varIndex) {
		if (varReplicationLookup.containsKey(varIndex)) {
			return varReplicationLookup.get(varIndex);
		} else
			return null;
	}

	public static boolean isVarReplicated(int varIndex) {
		if (varReplicationLookup.containsKey(varIndex)
				&& varReplicationLookup.get(varIndex).size() == 1) {
			return false;
		} else {
			return true;
		}

	}

	private int siteIndex;
	private Map<Integer, Variable> varMap;
	private boolean isSiteUp;
	private Boolean isRecovered;

	private String status;

	private static Map<Integer, Set<Integer>> varReplicationLookup = new HashMap<Integer, Set<Integer>>();

	public Site(int index, int numVar) {
		this.siteIndex = index;
		this.varMap = new HashMap<Integer, Variable>();
		this.isSiteUp = true;
		this.isRecovered = null;
		this.setDefaultVariable(index, numVar);
		this.status = "Never fails.";
	}

	public boolean addReadLock(int varIndex, String tranID) {
		if (!this.isSiteUp) {
			return false;
		}
		if (!this.isWriteLocked(varIndex)
				|| this.isWriteLockedBy(varIndex, tranID)) {
			this.varMap.get(varIndex).addReadLock(tranID);
			return true;
		} else {
			return false;
		}
	}

	public void clearLocksOf(String tranID) {

		for (Integer varIndex : getAllAvailableVarIndex()) {
			if (this.isWriteLockedBy(varIndex, tranID)) {
				this.emptyLocks(varIndex);
			}
		}
	}

	public void emptyAllLocks() {
		for (Integer varIndex : this.varMap.keySet()) {
			this.varMap.get(varIndex).emptyLocks();
		}
	}

	public void emptyLocks(int varIndex) {
		this.varMap.get(varIndex).emptyLocks();
	}

	public boolean executeTransaction(Transaction T) {
		if (!this.isSiteUp) {
			return false;
		}
		Iterator<Operation> opIterator = T.getOperations().iterator();

		while (opIterator.hasNext()) {
			Operation opTemp = opIterator.next();
			if (!opTemp.getIsRead()) {
				int newValue = opTemp.getVarValue();
				int varIndex = opTemp.getVarIndex();
				this.updateVarValue(newValue, varIndex, T.getTID());
			}
		}
		this.clearLocksOf(T.getTID());

		return true;
	}

	public void failSite(int newFailureTime) {
		this.isSiteUp = false;
		this.isRecovered = false;
		this.status = "Failed. Not recovered";
		for (Integer var : varMap.keySet()) {
			varMap.get(var).setAccessible(false);
		}
	}

	public Set<Integer> getAllAvailableVarIndex() {
		return this.varMap.keySet();
	}

	public Set<String> getReadLockSet(int varIndex) {
		return this.varMap.get(varIndex).getReadLockSet();
	}

	public int getSiteIndex() {
		return this.siteIndex;
	}

	public Variable getVariable(int varIndex) {
		return this.varMap.get(varIndex);
	}

	public String getWriteLock(int varIndex) {
		if (!varMap.containsKey(varIndex)) {
			return "";
		}
		return varMap.get(varIndex).getWriteLock();
	}

	public boolean hasVarCopy(int varIndex) {
		return this.varMap.containsKey(varIndex);
	}

	public boolean isReadLocked(int varIndex) {
		return this.varMap.get(varIndex).getReadLockSet().isEmpty();
	}

	public boolean isReadLockedBy(int varIndex, String tranID) {
		if (!this.varMap.get(varIndex).getReadLockSet().isEmpty()
				&& this.varMap.get(varIndex).getReadLockSet().contains(tranID)) {
			return true;
		}
		return false;
	}

	public boolean isReadyForRecovery() {
		if (!this.isSiteUp && this.isRecovered != null && this.isRecovered) {
			return true;
		} else
			return false;
	}

	public boolean isSiteUp() {
		return this.isSiteUp;
	}

	public boolean isVarReadable(int varIndex) {
		if (this.isRecovered != null && this.isRecovered
				&& this.varMap.containsKey(varIndex)) {
			return this.varMap.get(varIndex).isAccessible();
		}
		return this.isSiteUp();
	}

	public boolean isWriteLocked(int varIndex) {
		if (!this.varMap.get(varIndex).getWriteLock().isEmpty()) {
			return true;
		} else {
			return false;
		}
	}

	public boolean isWriteLockedBy(int varIndex, String tranID) {
		if (this.isWriteLocked(varIndex)) {
			if (this.varMap.get(varIndex).getWriteLock()
					.equalsIgnoreCase(tranID)) {
				return true;
			} else {
				return false;
			}
		} else {
			return true;
		}
	}

	public String printData(int numVar) {
		String siteTitle = String
				.format("%4s", "S" + this.getSiteIndex() + ":");
		StringBuffer datastring = new StringBuffer(siteTitle);
		for (int i = 1; i <= numVar; i++) {
			if (varMap.containsKey(i)) {
				String value = String.format("%5s", varMap.get(i).getValue());
				if (!this.varMap.get(i).isAccessible()) {
					value = String.format("%5s", "NA");
				}
				datastring.append(value + "|");
			} else {
				String value = String.format("%6s", "|");
				datastring.append(value);
			}
		}
		datastring.append("\n Status: " + this.status);
		return datastring.toString();
	}

	public String printSnapshot() {

		StringBuffer snapshot = new StringBuffer("Site index: "
				+ this.getSiteIndex() + "\nStatus: " + this.status);

		Map<Integer, Variable> sortedMap = new TreeMap<Integer, Variable>(
				this.varMap);
		snapshot.append("\nAvailable variable copies:\n");
		Iterator<Entry<Integer, Variable>> treeMapIT = sortedMap.entrySet()
				.iterator();
		if (this.isSiteUp) {
			while (treeMapIT.hasNext()) {
				Map.Entry<Integer, Variable> map = treeMapIT.next();
				Integer index = new Integer(map.getKey().toString());
				if (this.varMap.get(index).isAccessible()) {
					snapshot.append("x" + index + "="
							+ this.varMap.get(index).getValue() + "\n");
				} else {
					snapshot.append("x" + index + "=NA\n");
				}
			}
		} else {
			while (treeMapIT.hasNext()) {
				Map.Entry<Integer, Variable> map = treeMapIT.next();
				Integer index = new Integer(map.getKey().toString());
				if (this.isVarReadable(index)) {
					snapshot.append("x" + index + "="
							+ this.varMap.get(index).getValue() + "\n");
				} else {
					snapshot.append("x" + index + "=NA\n");
				}
			}
		}
		// snapshot.append("Site status: " + this.status);

		return snapshot.toString();
	}

	public boolean recoverSite() {
		boolean result = false;
		if (!isSiteUp && !isRecovered) {
			this.isSiteUp = true;
			this.isRecovered = true;
			result = true;
		}
		for (Integer var : varMap.keySet()) {
			if (!Site.isVarReplicated(var)) {
				varMap.get(var).setAccessible(true);
			}
		}
		this.status = "Recovered from failure.";
		return result;
	}

	public void removeReadLock(String tranID, int varIndex) {
		this.varMap.get(varIndex).removeReadLock(tranID);
	}

	private void setDefaultVariable(int siteIndex, int numVar) {
		for (int varIndex = 1; varIndex <= numVar; varIndex++) {
			if (varIndex % 2 == 0) {
				Variable var = new Variable(varIndex);
				this.varMap.put(varIndex, var);
			} else if ((varIndex + 1) % 10 == siteIndex) {
				Variable var = new Variable(varIndex);
				this.varMap.put(varIndex, var);
			}
		}
		Set<Integer> replicatedSite = new HashSet<Integer>();
		replicatedSite.add(this.siteIndex);
		for (Integer varIndex : varMap.keySet()) {
			if (varReplicationLookup.containsKey(varIndex)) {
				varReplicationLookup.get(varIndex).addAll(replicatedSite);
			} else {
				varReplicationLookup.put(varIndex, replicatedSite);
			}
		}
	}

	private boolean updateVarValue(int newValue, int varIndex, String tranID) {
		if (this.varMap.containsKey(varIndex)) {
			this.varMap.get(varIndex).updateValue(newValue);
			this.varMap.get(varIndex).setAccessible(true);
			return true;
		}
		return false;
	}

	public boolean updateWriteLock(int varIndex, String tranID) {
		if (!this.isSiteUp) {
			return false;
		}
		if (!varMap.containsKey(varIndex)) {
			return false;
		}
		if (this.isWriteLocked(varIndex)) {
			if (this.isWriteLockedBy(varIndex, tranID)) {
				return true;
			} else {
				return false;
			}
		} else {
			this.varMap.get(varIndex).updateWriteLock(tranID);
			this.addReadLock(varIndex, tranID);
			return true;
		}
	}

}
