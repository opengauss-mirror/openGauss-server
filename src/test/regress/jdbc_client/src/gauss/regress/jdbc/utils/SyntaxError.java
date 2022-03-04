package gauss.regress.jdbc.utils;

public class SyntaxError {
	private String sqlLine = "";
	private int positionInLine = 0;
	public String getSqlLine() {
		return sqlLine;
	}
	public void setSqlLine(String sqlLine) {
		this.sqlLine = sqlLine;
	}
	public int getPositionInLine() {
		return positionInLine;
	}
	public void setPositionInLine(int positionInLine) {
		this.positionInLine = positionInLine;
	}
	
}
