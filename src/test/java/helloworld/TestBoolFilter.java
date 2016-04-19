package helloworld;

import java.util.HashSet;
import java.util.Random;

import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;

import mc.algorithm.BoolFilter;

public class TestBoolFilter {

	public static void main(String[] args) {

		System.out.println("test..."+(1 << 2));
		HashSet<String> hashSet = new HashSet<String>();
		BoolFilter boolFilter = new BoolFilter();
		int conflictCount = 0;
		
		for (int i = 0; i < 1000000; i++) {
			String randomString = getRandomString(8);
			hashSet.add(randomString);
			if(!boolFilter.contains(randomString)) {
				boolFilter.add(randomString);
			}
			else {
				conflictCount++;
			}
			}
		System.out.println("hashset size is "+hashSet.size()+" cost memory "+Runtime.getRuntime().totalMemory());
		System.out.println("bitsize = "+ boolFilter.bitSize + "  conflictCount = "+conflictCount);
		}
	
		
		
		
		


	public static String getRandomString(int length) { // length表示生成字符串的长度
		String base = "abcdefghijklmnopqrstuvwxyz0123456789";
		Random random = new Random();
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < length; i++) {
			int number = random.nextInt(base.length());
			sb.append(base.charAt(number));
		}
		return sb.toString();
	}

}
