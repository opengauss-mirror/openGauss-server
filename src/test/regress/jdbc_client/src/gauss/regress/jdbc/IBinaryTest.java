package gauss.regress.jdbc;

import gauss.regress.jdbc.utils.DatabaseConnection4Test;
/**
 * Interface binary tests has to implement
 *
 */
public interface IBinaryTest {
	void execute(DatabaseConnection4Test conn);
}
