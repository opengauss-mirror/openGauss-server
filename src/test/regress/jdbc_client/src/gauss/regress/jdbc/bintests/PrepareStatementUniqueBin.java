/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package gauss.regress.jdbc.bintests;

import java.util.ArrayList;
import java.util.List;

import gauss.regress.jdbc.IBinaryTest;
import gauss.regress.jdbc.utils.DatabaseConnection4Test;

public class PrepareStatementUniqueBin implements IBinaryTest{

	@Override
	public void execute(DatabaseConnection4Test conn) {
		BinUtils.createCLSettings(conn);
			conn.executeSql("CREATE TABLE IF NOT EXISTS t_unique(id INT, "
			+ "name text UNIQUE ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY=cek1, ENCRYPTION_TYPE = DETERMINISTIC));");
		
		String sqlInsert = "INSERT INTO t_unique values (?,?)";		
		List<String> parameters = new ArrayList<>();
		parameters.add("5");
		parameters.add("John");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);

		parameters = new ArrayList<>();
		parameters.add("2");
		parameters.add("Moses");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		
		parameters = new ArrayList<>();
		parameters.add("6");
		parameters.add("John");
		conn.updateDataWithPrepareStmnt(sqlInsert, parameters);
		
		conn.fetchData("SELECT * FROM t_unique order by id");
		conn.executeSql("DROP TABLE t_unique");
		BinUtils.dropCLSettings(conn);
	}
}
