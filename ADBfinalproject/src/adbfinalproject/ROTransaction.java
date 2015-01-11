package adbfinalproject;

/**
 * @author Vangie Shue
 **/
public class ROTransaction extends Transaction {

	Integer[] snapshot;

	public ROTransaction(String tid, int start, int NUM_VARS, Site[] sites) {
		super(tid, start);
		snapshot = new Integer[NUM_VARS + 1];

		for (int k = 1; k < snapshot.length; k++) {
			for (int i = 1; i < sites.length; i++) {
				if (sites[i].hasVarCopy(k)
						&& sites[i].isVarReadable(k)) {
					snapshot[k] = sites[i].getVariable(k).getValue();
					break;
				}
			}
		}
	}

	public boolean updateSnapshot(Site[] sites) {
		for (int k = 1; k < snapshot.length; k++) {
			if (snapshot[k] == null) {
				for (int i = 1; i < sites.length; i++) {
					if (sites[i].hasVarCopy(k)
							&& sites[i].isVarReadable(k)) {
						snapshot[k] = sites[i].getVariable(k).getValue();
						break;
					}
				}
			}
		}
		return true;
	}

	public Integer[] getSnapshot() {
		return this.snapshot;
	}

	public Integer getVarValue(int varIndex) {
		if (varIndex <= snapshot.length - 1) {
			return this.snapshot[varIndex];
		} else {
			return null;
		}
	}
}
