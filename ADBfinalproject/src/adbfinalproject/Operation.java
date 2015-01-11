package adbfinalproject;

/**
 * @author Vangie Shue
 **/
public class Operation {

	private int timestamp;
	private boolean isRead;
	private int varIndex;
	private int varValue;

	public Operation(int t, int variable) {
		setTimestamp(t);
		setIsRead(true);
		setVarIndex(variable);
	}

	public Operation(int t, int variable, int value) {
		setTimestamp(t);
		setIsRead(false);
		setVarIndex(variable);
		setVarValue(value);
	}

	private void setTimestamp(int t) {
		timestamp = t;
	}

	private void setIsRead(boolean b) {
		isRead = b;
	}

	private void setVarIndex(int i) {
		varIndex = i;
	}

	private void setVarValue(int v) {
		varValue = v;
	}

	public int getTimestamp() {
		return timestamp;
	}

	public boolean getIsRead() {
		return isRead;
	}

	public int getVarIndex() {
		return varIndex;
	}

	public int getVarValue() {
		return varValue;
	}

}
