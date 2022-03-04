package gauss.regress.jdbc.utils;

public interface IInputFileParser {
	public boolean load(String path);
	public boolean  moveNext();
	public SQLCommand get();
}
