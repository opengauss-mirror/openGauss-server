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
