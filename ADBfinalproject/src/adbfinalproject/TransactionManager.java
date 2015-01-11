package adbfinalproject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * @author Vangie Shue, Kun Liu
 **/
public class TransactionManager {

	private int NUM_SITES;
	private int NUM_VARS;
	private boolean VERBOSE;
	private Site[] sites;
	private Map<String, Transaction> runningList;
	private Map<String, Transaction> waitList;
	private Map<String, ROTransaction> ROList;
	private Map<String, Transaction> abortList;
	private Map<String, Transaction> finishedList;
	private WaitForGraph waitgraph;
	private Queue<String> pendingOperations;

	private static int TIMER;

	public TransactionManager(int numsites, int numvars, boolean verbose) {
		System.out.println("Initializing Transaction Manager.");
		TIMER = 0;

		this.NUM_SITES = numsites;
		this.NUM_VARS = numvars;
		this.VERBOSE = verbose;

		setupSites();
		setupLists();

		waitgraph = WaitForGraph.getInstance();
		pendingOperations = new LinkedList<String>();
	}

	private boolean abort(Set<String> abortTIDset, String message) {
		Transaction abortT;
		Set<String> resumeTIDs = new HashSet<String>();
		if (this.VERBOSE) {
			System.out.println(message);
		}
		for (String abortTID : abortTIDset) {
			if (abortTID.isEmpty()) {
				continue;
			} else {
				if (runningList.containsKey(abortTID)) {
					abortT = runningList.get(abortTID);
					runningList.remove(abortTID);
					abortList.put(abortTID, abortT);
				}
				if (waitList.containsKey(abortTID)) {
					abortT = waitList.get(abortTID);
					waitList.remove(abortTID);
					abortList.put(abortTID, abortT);
				}
				resumeTIDs.addAll(waitgraph.getWaitedBySet(abortTID));
				waitgraph.clearEdge(abortTID);
				clearLocksByTID(abortTID);
			}
		}

		resumeWaitingTIDs(resumeTIDs);
		runPendingOperations();
		return true;
	}

	private boolean abort(String abortTID, String message) {
		Set<String> abortTIDset = new HashSet<String>();
		abortTIDset.add(abortTID);
		abort(abortTIDset, message);
		return true;
	}

	private void abortTIDsByFailure(int siteIndex) {
		Set<String> abortTIDSet = new HashSet<String>();
		Set<Integer> failVarIndex = sites[siteIndex].getAllAvailableVarIndex();

		for (Integer varIndex : failVarIndex) {
			sites[siteIndex].getReadLockSet(varIndex);
			abortTIDSet.addAll(sites[siteIndex].getReadLockSet(varIndex));
			String writeLockTID = sites[siteIndex].getWriteLock(varIndex);
			if (!writeLockTID.isEmpty()) {
				abortTIDSet.add(writeLockTID);
			}
		}
		int count = 1;
		StringBuffer message = new StringBuffer("Abort ");
		boolean flag = true;
		for (String TID : abortTIDSet) {
			if (!TID.isEmpty()) {
				if (count == abortTIDSet.size()) {
					message.append(TID);
					flag = false;
				} else {
					message.append(TID + ", ");
					flag = false;
				}
				count++;
			}
		}
		if (flag) {
			message.append("no transaction");
		}
		message.append(" due to Site " + siteIndex + " failure.");
		this.abort(abortTIDSet, message.toString());
	}

	private boolean begin(String s) {
		int firstP = s.indexOf("(");
		int lastP = s.indexOf(")");

		String TID = s.substring(firstP + 1, lastP);

		if (s.contains("beginRO(")) {
			ROTransaction t = new ROTransaction(TID, TIMER, NUM_VARS, sites);
			ROList.put(TID, t);
		} else {
			Transaction t = new Transaction(TID, TIMER);
			runningList.put(TID, t);
		}
		return true;
	}

	private boolean canReadFromSite(int varIndex) {
		for (int i = 1; i < sites.length; i++) {
			if (sites[i].hasVarCopy(varIndex)
					&& sites[i].isVarReadable(varIndex)) {
				return true;
			}
		}
		return false;
	}

	private boolean clearLocksByTID(String TID) {
		for (int i = 1; i < sites.length; i++) {
			sites[i].clearLocksOf(TID);
		}
		return true;
	}

	private boolean commitTransaction(String TID) {
		Transaction t = finishedList.get(TID);
		for (int i = 1; i < sites.length; i++) {
			sites[i].executeTransaction(t);
		}
		this.printMessage("Transaction " + TID + " committed.");
		return true;
	}

	private void printMessage(String message) {
		if (this.VERBOSE) {
			System.out.println(message);
		}
	}

	private boolean dump(String s) {
		if (s.equalsIgnoreCase("dump()")) {
			this.printAll();
		} else if (s.contains("dump(x")) {
			int varIndex = new Integer(s.substring(s.indexOf("(") + 2,
					s.indexOf(")")));
			for (int i = 1; i < sites.length; i++) {
				if (sites[i].hasVarCopy(varIndex)) {
					String value;
					if (!sites[i].getVariable(varIndex).isAccessible()) {
						value = "NA";
					} else {
						value = new Integer(sites[i].getVariable(varIndex)
								.getValue()).toString();
					}
					System.out.println("x" + varIndex + " on site " + i
							+ " value=" + value);
				}
			}
		} else {
			Integer siteIndex = new Integer(s.substring(s.indexOf("(") + 1,
					s.indexOf(")")));
			System.out.println(sites[siteIndex].printSnapshot());
		}
		return true;
	}

	private void addPendingOperation(String op) {
		this.pendingOperations.add(op);
		this.printMessage(op + " is added to pending operations.");
	}

	private boolean end(String s) {
		int firstP = s.indexOf("(");
		int lastP = s.indexOf(")");

		String TID = s.substring(firstP + 1, lastP);
		Transaction t;
		if (runningList.containsKey(TID)) {
			t = runningList.remove(TID);
			t.setEndTime(TIMER);
			finishedList.put(TID, t);
			commitTransaction(TID);

			Set<String> resumeTIDs = waitgraph.getWaitedBySet(TID);
			waitgraph.clearEdge(TID);
			clearLocksByTID(TID);
			// runPendingOperations();
			resumeWaitingTIDs(resumeTIDs);
			runPendingOperations();
		} else if (ROList.containsKey(TID)) {
			t = ROList.remove(TID);
			t.setEndTime(TIMER);
			finishedList.put(TID, t);
		} else if (abortList.containsKey(TID)) {
			this.printMessage(TID + " was aborted and thus cannot commit.");
		} else {
			addPendingOperation(s);
		}
		return true;
	}

	private boolean resumeWaitingTIDs(Set<String> resumeTIDs) {
		if (!resumeTIDs.isEmpty()) {
			Iterator<String> it = resumeTIDs.iterator();
			while (it.hasNext()) {
				String tempTID = it.next();
				if (waitList.containsKey(tempTID)) {
					Transaction tempT = waitList.remove(tempTID);
					this.printMessage(tempTID + " resumed to runningList.");
					this.waitgraph.clearEdge(tempTID);
					runningList.put(tempTID, tempT);
					this.runPendingOperations();
				}
			}
		}
		return true;
	}

	private boolean fail(String s) {
		int siteIndex = Integer.parseInt((s.substring(s.indexOf("(") + 1,
				s.indexOf(")"))));
		sites[siteIndex].failSite(TIMER);
		abortTIDsByFailure(siteIndex);
		sites[siteIndex].emptyAllLocks();
		return true;
	}

	private int getDirtyWrite(String TID, int varIndex) {
		int result = Integer.MIN_VALUE;
		Transaction t;
		if (runningList.containsKey(TID)) {
			t = runningList.get(TID);
			result = t.getLastWrite(varIndex);
		}
		return result;
	}

	private String getWriteLockTID(int varIndex) {
		String lockedTID = "";
		for (int i = 1; i < sites.length; i++) {
			if (sites[i].hasVarCopy(varIndex) && sites[i].isSiteUp()) {
				lockedTID = sites[i].getWriteLock(varIndex);
			}
		}
		return lockedTID;
	}

	public boolean parseLine(String s, boolean increment) {
		s = s.replaceAll("\\s", "");
		System.out.println(s);
		if (increment) {
			TIMER++;
		}
		if (s.contains(";") && (s.contains("W(") || s.contains("R("))) {
			String[] opArray = s.split(";");
			String[] WRArray = new String[opArray.length];
			String[] varArray = new String[opArray.length];
			for (int i = 0; i < opArray.length; i++) {
				String tempOp = opArray[i];
				if (tempOp.contains("W(")) {
					WRArray[i] = "W";
					varArray[i] = tempOp.substring(tempOp.indexOf(",") + 1,
							tempOp.lastIndexOf(","));
				} else if (tempOp.contains("R(")) {
					WRArray[i] = "R";
					varArray[i] = tempOp.substring(tempOp.indexOf(",") + 1,
							tempOp.lastIndexOf(")"));
				} else {
					WRArray[i] = "";
					varArray[i] = "";
				}
			}
			for (int i = 0; i < opArray.length; i++) {
				if (opArray[i].isEmpty()) {
					continue;
				}
				String TID1 = opArray[i].substring(opArray[i].indexOf("(") + 1,
						opArray[i].indexOf(","));
				String op1 = WRArray[i];
				for (int j = i + 1; j < WRArray.length; j++) {
					String TID2 = opArray[j].substring(
							opArray[j].indexOf("(") + 1,
							opArray[j].indexOf(","));
					boolean abort = false;
					if (op1.equalsIgnoreCase("W")
							&& varArray[i].equals(varArray[j])
							&& !TID1.equalsIgnoreCase(TID2)) {
						abort = true;
					} else if (op1.equalsIgnoreCase("R")
							&& WRArray[j].equals("W")
							&& varArray[i].equals(varArray[j])
							&& !TID1.equalsIgnoreCase(TID2)) {
						abort = true;
					}
					if (abort) {
						String message = "Abort " + TID2
								+ " due to concurrent " + opArray[i];
						abort(TID2, message);
						varArray[j] = "";
						WRArray[j] = "";
						opArray[j] = "";
					}
				}
				parseLine(opArray[i], false);
			}
		} else if (s.contains(";")) {
			String[] opArray = s.split(";");
			for (String op : opArray) {
				parseLine(op, false);
			}
		} else if (s.startsWith("end")) {
			end(s);
		} else if (s.startsWith("dump(")) {
			dump(s);
		} else if (s.startsWith("begin")) {
			begin(s);
		} else if (s.contains("W(")) {
			write(s);
		} else if (s.contains("R(")) {
			read(s);
		} else if (s.contains("fail(")) {
			fail(s);
		} else if (s.contains("recover(")) {
			recover(s);
		} else if (s.contains("querystate(")) {
			querystate(s);
		}
		return true;
	}

	public void printAll() {
		System.out.print(String.format("%4s", ""));
		for (int i = 1; i <= NUM_VARS; i++) {
			String print = String.format("%5s", "x" + i);
			System.out.print(print + "|");
		}
		System.out.println();
		for (int i = 1; i < sites.length; i++) {
			String print = String.format("%6s", sites[i].printData(NUM_VARS));
			System.out.println(print);
		}
	}

	private boolean read(String s) {
		String TID = s.substring(s.indexOf("(") + 1, s.indexOf(","));
		String var = s.substring(s.indexOf(",") + 1, s.lastIndexOf(")"));
		int varIndex = Integer.parseInt(var.substring(1));

		if (abortList.containsKey(TID)) {
			System.out.println(s + " failed becaused " + TID + " was aborted.");
			return false;
		}
		if (ROList.containsKey(TID)) {
			Integer varValue = ROList.get(TID).getVarValue(varIndex);
			if (varValue != null) {
				System.out.println("Read-only " + TID + " reads x" + varIndex
						+ "=" + varValue);
			} else {
				System.out.println("Read-only " + TID + " can't read x"
						+ varIndex + " as it is unavailable");
			}
			ROList.get(TID).addOperation(TIMER, varIndex);
		} else if (runningList.containsKey(TID)) {
			if (canReadFromSite(varIndex)) {
				String WLTID = getWriteLockTID(varIndex);
				if (WLTID.equals("")) {
					readFromSite(varIndex, TID);
					runningList.get(TID).addOperation(TIMER, varIndex);
				} else if (WLTID.equals(TID)) {
					System.out.println(TID + " reads x" + varIndex
							+ " from its previous write, value="
							+ getDirtyWrite(TID, varIndex));
				} else {
					int waitResult = waitDieProtocol(WLTID, TID);
					if (waitResult == 1) {
						addPendingOperation(s);
					} else if (waitResult == -1) {
						// this.abort(TID, "Abort "+TID+ "due to wait die ");
					}

				}
			} else {
				Transaction temp = runningList.remove(TID);
				System.out.println(TID + " moved to waitList.");
				this.addPendingOperation(s);
				waitList.put(TID, temp);
			}
		}

		return true;
	}

	private int readFromSite(int varIndex, String TID) {
		int result = Integer.MIN_VALUE;

		List<Integer> indexList = new ArrayList<Integer>();
		for (int i = 0; i < this.NUM_SITES; i++) {
			indexList.add(i + 1);
			sites[i + 1].addReadLock(varIndex, TID);
		}
		java.util.Collections.shuffle(indexList);
		for (Integer index : indexList) {
			if (sites[index].hasVarCopy(varIndex)
					&& sites[index].isVarReadable(varIndex)) {
				sites[index].addReadLock(varIndex, TID);
				result = sites[index].getVariable(varIndex).getValue();
				System.out.println(TID + " reads x" + varIndex + " from Site "
						+ index + " value=" + result);
				break;
			}
		}
		return result;
	}

	private boolean recover(String s) {
		int siteIndex = Integer.parseInt((s.substring(s.indexOf("(") + 1,
				s.indexOf(")"))));
		sites[siteIndex].recoverSite();
		System.out.println("Failed Site " + siteIndex + " is recovered.");
		for (ROTransaction t : ROList.values()) {
			t.updateSnapshot(sites);
		}
		Set<String> resumeTafterRecover = new HashSet<String>();
		for (String waitT : waitList.keySet()) {
			if (this.waitgraph.getWaitingFor(waitT).isEmpty()) {
				resumeTafterRecover.add(waitT);
				this.waitgraph.clearEdge(waitT);
			}
		}
		this.resumeWaitingTIDs(resumeTafterRecover);
		this.runPendingOperations();
		return true;
	}

	private int waitDieProtocol(String lockingTID, String requestTID) {
		// -1: aborted
		// 1: waitlisted
		Transaction requestT;
		if (runningList.containsKey(requestTID)) {
			requestT = runningList.get(requestTID);
		} else {
			requestT = waitList.get(requestTID);
		}

		Transaction lockingT;
		if (runningList.containsKey(lockingTID)) {
			lockingT = runningList.get(lockingTID);
		} else {
			lockingT = waitList.get(lockingTID);
		}

		if (lockingT != null
				&& lockingT.getStartTime() < requestT.getStartTime()) {
			String message = "Abort " + requestTID + " for Wait-die by "
					+ lockingTID;
			abort(requestTID, message);
			return -1;
		} else if (lockingT != null) {
			waitgraph.addEdge(requestTID, lockingTID);
			waitList.put(requestTID, requestT);
			System.out.println(requestTID + " moved to waitList.");
			runningList.remove(requestTID);
			return 1;
		}
		return 0;
	}

	private String getOldestTID(Set<String> readlockTs) {
		String oldTID = "";
		Iterator<String> readit = readlockTs.iterator();
		if (readit.hasNext()) {
			oldTID = readit.next();
			Transaction oldT;
			if (runningList.containsKey(oldTID)) {
				oldT = runningList.get(oldTID);
			} else {
				oldT = waitList.get(oldTID);
			}

			String currTID;
			while (readit.hasNext()) {
				currTID = readit.next();
				Transaction currT;
				if (runningList.containsKey(currTID)) {
					currT = runningList.get(currTID);
				} else {
					currT = waitList.get(currTID);
				}

				if (currT != null && oldT.getStartTime() > currT.getStartTime()) {
					oldTID = currTID;
					oldT = currT;
				}
			}
		}
		return oldTID;
	}

	private int requestWriteLock(int variable, String requestTID) {
		// 0: no locks
		// 1: locked, requestTID is waitlisted
		// -1: locked, requestTID is aborted
		// -2: all sites failed
		int result = -2;
		Transaction requestT = runningList.get(requestTID);
		String lockingTID;
		boolean testFlag = false;
		for (int i = 1; i < sites.length; i++) {
			if (sites[i].hasVarCopy(variable)) {
				if (sites[i].isSiteUp() || sites[i].isReadyForRecovery()) {
					testFlag = true;
					break;
				}
			}
		}
		if (testFlag) {
			for (int i = 1; i < sites.length; i++) {
				result = 0;
				if (!sites[i].hasVarCopy(variable)) {
					continue;
				}
				Set<String> readlockTs = sites[i].getReadLockSet(variable);
				if (!readlockTs.isEmpty()) {
					if (readlockTs.size() == 1) {
						String onlyTID = readlockTs.iterator().next();
						if (onlyTID != null
								&& onlyTID.equalsIgnoreCase(requestTID)) {
							return 0;
						} else {
							return waitDieProtocol(onlyTID, requestTID);
						}
					} else {
						String oldestTID = getOldestTID(readlockTs);
						int waitDieResult = waitDieProtocol(oldestTID,
								requestTID);
						if (waitDieResult == 1 || waitDieResult == -1) {
							return waitDieResult;
						}
					}
				}

				lockingTID = sites[i].getWriteLock(variable);
				if (!lockingTID.equals("") && !lockingTID.equals(requestTID)) {
					int waitDieResult = waitDieProtocol(lockingTID, requestTID);
					if (waitDieResult == 1 || waitDieResult == -1) {
						return waitDieResult;
					}
				}
			}
		}
		if (result == -2) {
			waitList.put(requestTID, requestT);
			System.out.println(requestTID + " moved to waitList.");
			runningList.remove(requestTID);
		}
		return result;
	}

	private boolean runPendingOperations() {
		Queue<String> oldPending = new LinkedList<String>(pendingOperations);
		this.pendingOperations.clear();
		Iterator<String> it = oldPending.iterator();
		while (it.hasNext()) {
			String op = it.next();
			parseLine(op, true);
		}
		return true;
	}

	private void setupLists() {
		runningList = new HashMap<String, Transaction>();
		waitList = new HashMap<String, Transaction>();
		ROList = new HashMap<String, ROTransaction>();
		abortList = new HashMap<String, Transaction>();
		finishedList = new HashMap<String, Transaction>();
	}

	private void setupSites() {
		this.sites = new Site[NUM_SITES + 1];
		for (int i = 1; i <= NUM_SITES; i++) {
			this.sites[i] = new Site(i, NUM_VARS);
		}
	}

	private boolean updateWriteLocks(int variable, String TID) {
		for (int i = 1; i < sites.length; i++) {
			sites[i].updateWriteLock(variable, TID);
		}
		return true;
	}

	private boolean write(String s) {
		String TID = s.substring(s.indexOf("(") + 1, s.indexOf(","));
		String var = s.substring(s.indexOf(",") + 1, s.lastIndexOf(","));
		int variable = Integer.parseInt(var.substring(1));
		int value = Integer.parseInt(s.substring(s.lastIndexOf(",") + 1,
				s.indexOf(")")));

		if (abortList.containsKey(TID)) {
			System.out.println(s + " failed becaused " + TID + " was aborted.");
			return false;
		}
		if (waitList.containsKey(TID)) {
			addPendingOperation(s);
			return false;
		}
		int requestResult = requestWriteLock(variable, TID);
		if (requestResult == 0) {
			updateWriteLocks(variable, TID);
			runningList.get(TID).addOperation(TIMER, variable, value);
		} else if (requestResult == 1 || requestResult == -2) {
			addPendingOperation(s);
		}
		return true;
	}

	public boolean querystate(String s) {

		System.out.print("> Running List:");
		for (String TID : runningList.keySet()) {
			System.out.print(TID + " ");
		}
		System.out.println();
		System.out.print("> Waiting List:");
		for (String TID : waitList.keySet()) {
			System.out.print(TID + " ");
		}
		System.out.println();
		System.out.print("> Read-only List:");
		for (String TID : ROList.keySet()) {
			System.out.print(TID + " ");
		}
		System.out.println();
		System.out.print("> Aborted List:");
		for (String TID : abortList.keySet()) {
			System.out.print(TID + " ");
		}
		System.out.println();
		System.out.print("> Finished List:");
		for (String TID : finishedList.keySet()) {
			System.out.print(TID + " ");
		}
		System.out.println(this.waitgraph.graphSnapshot());
		System.out.println();
		return true;
	}
}
