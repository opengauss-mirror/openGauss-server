package gauss.regress.jdbc.utils;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;

public class OutFileWriter implements Closeable{
	private FileWriter m_output = null;
	private String m_filename = null;
	boolean m_isDebug = false;
	
	public OutFileWriter() {
		m_isDebug = java.lang.management.ManagementFactory.getRuntimeMXBean().
				getInputArguments().toString().indexOf("jdwp") >= 0;
	}
	/**
	 *open file handler to write to 
	 * @param[in] filename
	 * @throws IOException
	 */
	public void openFile(String filename) throws IOException{
		m_filename = filename;
		m_output = new FileWriter(filename);
	}

	/**
	 * closes the file handler
	 */
	@Override
	public void close() throws IOException {
	    if (m_output != null) {
			m_output.close();
			m_output = null;
		}
	}
	public void flush() {
		if (m_output != null) {
			try {
				m_output.flush();
			} catch (IOException e) {
				System.out.println("flush failed ...");
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Writes a line to the output file
	 * @param line line data to write
	 */
	public void writeLine(String line) {
			write(line);
			write(System.getProperty( "line.separator"));
	}
	
	/**
	 * Writes data to the output file
	 * @param info data to write
	 */
	public void write(String info) {
		try {
			if (m_isDebug) {
				System.out.print(info);
			}
			m_output.write(info);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}		
	}
	
	/**
	 * get file name
	 */
	public String getFilename() {
		return m_filename;
	}
}
