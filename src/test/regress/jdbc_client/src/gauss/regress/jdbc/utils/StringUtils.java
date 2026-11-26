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

public class StringUtils {
	
	/**
	 * Counts the number of Chinese characters in a string 
	 * @param s input string 
	 * @return number of Chinese characters
	 */
	public static int countHanCharacters(String s) {
		int result = 0;
	    for (int i = 0; i < s.length(); ++i) {
	        int codepoint = s.codePointAt(i);
	        if (Character.UnicodeScript.of(codepoint) == Character.UnicodeScript.HAN) {
	            ++result;
	        }
	    }
	    return result;
	}
	
	/**
	 * Get a string width for printing 
	 * @param s input string
	 * @return the string width for printing
	 */
	public static int getStringWidth(String s) {
		int result = s.length();
		result += countHanCharacters(s);
		return result;
	}

}
