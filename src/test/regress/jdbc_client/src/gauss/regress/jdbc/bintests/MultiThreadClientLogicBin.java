package gauss.regress.jdbc.bintests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;
import gauss.regress.jdbc.utils.OutFileWriter;

/**
 * @author dev
 * simulate spring web server scenario
 */
public class MultiThreadClientLogicBin implements IBinaryTest {

	List<DatabaseConnection4Test> conns = null;
	Set<Integer> usedConns = null;
	String workerInsertSql = "INSERT INTO t_varchar VALUES ((SELECT COALESCE(MAX(ID),0) FROM t_varchar) + 1, 'worker_%d')";
	String validateSql = "SELECT * FROM t_varchar ORDER BY ID";
	int numOfExtraConns = 8;
	int selectedWorkerId = -1;

	private int nextInt() {
		int ret = -1;
		do {
			++ret;
		} while (usedConns.contains(ret) && usedConns.size() < numOfExtraConns);
		usedConns.add(ret);
		return ret;
	}

	@Override
	public void execute(DatabaseConnection4Test conn) {
		usedConns = new HashSet<Integer>();
		conns = new ArrayList<DatabaseConnection4Test>();
		conns.add(conn);
		selectedWorkerId = 0;
		usedConns.add(selectedWorkerId);
		setupStep();
		createConnectionsStep();
		selectedWorkerId = nextInt();
		insertStep(new int[]{1}, false, 1);
		insertStep(new int[]{2}, false, 1);
		conns.get(selectedWorkerId).close();
		insertStep(new int[]{3}, true, 3);
		insertStep(new int[]{4, 5}, true, 3);
		cleanStep(conn);
		String tagretName = conn.getFileWriter().getFilename();
		for (DatabaseConnection4Test databaseConnection4Test : conns) {
			databaseConnection4Test.close();
		}
		conns.clear();
		conns = null;
		try {
			/* join output from all connections into single file (of original connection) 
			 * in order to compare with expected result */
			String joinCmd = String.format("cat %s?* >> %s", tagretName, tagretName);
		    String[] commands = {"/bin/sh", "-c", joinCmd};
			Runtime.getRuntime().exec(commands);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/*
	 * run a thread that initializes test env - create CL settings + table
	 */
	private void setupStep() {
		Runnable myRunnable = new Runnable() {
			public void run(){
				DatabaseConnection4Test conn = conns.get(selectedWorkerId);
				BinUtils.createCLSettings(conn);
				String sql = "CREATE TABLE IF NOT EXISTS t_varchar(id int, "
					+ "name varchar(50) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));";
				conn.executeSql(sql);
			}
		};
		Thread t = new Thread(myRunnable);
		t.start();
		try {
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * run a thread that duplicates the JDBC connection numOfExtraConns times 
	 * these connections can be used by other threads down the road
	 */
	private void createConnectionsStep() {
		DatabaseConnection4Test conn = conns.get(selectedWorkerId);
		Runnable myRunnable = new Runnable() {
			@SuppressWarnings("resource")
			public void run() {
				for (int i = 0; i < numOfExtraConns; ++i) {
					String outputFileName = conn.getFileWriter().getFilename() + Integer.toString(i); 
					OutFileWriter outputFile = new OutFileWriter();
					try {
						outputFile.openFile(outputFileName);
					} catch (IOException e) {
						e.printStackTrace();
						return;
					}
					DatabaseConnection4Test workerConn = new DatabaseConnection4Test(conn, outputFile);
					workerConn.reconnect();
					conns.add(workerConn);
				}
			}
		};
		Thread t = new Thread(myRunnable);
		t.start();
		try {
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * run threads that insert data into table, and validate
	 * @param workerIds - for naming and comparison
	 * @param useNewConnectionFromPool if true, use connection[nextInt()] from pool,
	 * 								   otherwise use connection[selectedWorkerId]
	 * @param insertsCount num of rows to insert
	 */
	private void insertStep(int[] workerIds, boolean useNewConnectionFromPool, int insertsCount) {
		final int threadCount = workerIds.length;
		AtomicInteger ai = new AtomicInteger();
		ai.set(0);
		List<Thread> threads = new ArrayList<Thread>();
		for (int threadIndex = 0 ; threadIndex < threadCount; ++threadIndex) {
			final int index = threadIndex;
			final int connIndex = (useNewConnectionFromPool) ? nextInt() : selectedWorkerId;
			Runnable myRunnable = new Runnable() {
				public void run(){
					DatabaseConnection4Test conn = conns.get(connIndex);
					for (int i=0; i < insertsCount; ++i) {
						conn.executeSql(String.format(workerInsertSql, workerIds[index]));
					}
					ai.incrementAndGet();
					while(ai.get() < threadCount) {
						;
					}
					conn.fetchData(validateSql);
				}
			};
			Thread t = new Thread(myRunnable);
			threads.add(t);
		}
		for (Thread t: threads) {
			t.start();
		}
		for (Thread t: threads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	/*
	 * run a thread that cleans up, and brings the DB to the state it was prior to test
	 */
	private void cleanStep(DatabaseConnection4Test conn) {
		Runnable myRunnable = new Runnable() {
			public void run(){
				String sql = "DROP TABLE t_varchar;";
				conn.executeSql(sql);
				BinUtils.cleanCLSettings(conn);
			}
		};
		Thread t = new Thread(myRunnable);
		t.start();
		try {
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
