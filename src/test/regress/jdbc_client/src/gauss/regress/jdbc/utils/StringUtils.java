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
