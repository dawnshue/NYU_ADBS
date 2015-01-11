package adbfinalproject;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

/**
 * @author Vangie Shue
 **/
public class Transaction {

	private String TID;
	private int startTime;
	private int endTime;
	private Queue<Operation> operations;
	private boolean hasWrite;

	public Transaction(String tid, int start) {
		this.setTID(tid);
		this.setStartTime(start);
		this.initializeOperations();
		hasWrite = false;
	}

	public boolean hasWrite() {
		return this.hasWrite;
	}

	private void setTID(String id) {
		this.TID = id;
	}

	private void setStartTime(int time) {
		this.startTime = time;
	}

	public void setEndTime(int time) {
		this.endTime = time;
	}

	private void initializeOperations() {
		this.operations = new LinkedList<Operation>();
	}

	public String getTID() {
		return this.TID;
	}

	public int getStartTime() {
		return this.startTime;
	}

	public Queue<Operation> getOperations() {
		return operations;
	}

	public int getEndTime() {
		return this.endTime;
	}

	public int getLastWrite(int varIndex) {
		int result = Integer.MIN_VALUE;
		Iterator<Operation> it = operations.iterator();
		Operation itOP;
		while (it.hasNext()) {
			itOP = it.next();
			if (!itOP.getIsRead() && itOP.getVarIndex() == varIndex) {
				result = itOP.getVarValue();
			}
		}
		return result;
	}

	public void addOperation(int time, int variable, int value) {
		operations.add(new Operation(time, variable, value));
		hasWrite = true;
	}

	public void addOperation(int time, int variable) {
		operations.add(new Operation(time, variable));
	}

	public Set<Integer> getAllVarIndices() {
		Set<Integer> accessedIndices = new HashSet<Integer>();
		Iterator<Operation> opIter = operations.iterator();
		while (opIter.hasNext()) {
			Operation op = opIter.next();
			accessedIndices.add(op.getVarIndex());
		}
		return accessedIndices;
	}
}
