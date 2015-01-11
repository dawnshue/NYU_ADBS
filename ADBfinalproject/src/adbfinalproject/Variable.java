package adbfinalproject;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Kun Liu
 **/
public class Variable {

	private int varIndex;
	private int value;
	private Set<String> readLockTIDs;
	private String writeLockTID;
	private boolean isAccessible;

	public Variable(int index) {
		this.varIndex = index;
		this.readLockTIDs = new HashSet<String>();
		this.writeLockTID = "";
		this.setDefaultValue(index);
		this.isAccessible = true;
	}

	private void setDefaultValue(int index) {
		this.value = index * 10;
	}

	public void setAccessible(boolean isAccessible) {
		this.isAccessible = isAccessible;
	}

	public boolean isAccessible() {
		return this.isAccessible;
	}

	public int getValue() {
		return this.value;
	}

	public int getIndex() {
		return this.varIndex;
	}

	public void updateValue(int newValue) {
		this.value = newValue;
	}

	public void addReadLock(String tranID) {
		this.readLockTIDs.add(tranID);
	}

	public Set<String> getReadLockSet() {
		return this.readLockTIDs;
	}

	public void removeReadLock(String tranID) {
		readLockTIDs.remove(tranID);
	}

	public void updateWriteLock(String tranID) {
		this.writeLockTID = tranID;
	}

	public String getWriteLock() {
		return this.writeLockTID;
	}

	public void emptyLocks() {
		this.writeLockTID = "";
		this.readLockTIDs.clear();
	}

}
