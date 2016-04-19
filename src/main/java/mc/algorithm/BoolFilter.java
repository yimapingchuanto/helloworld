package mc.algorithm;

import java.util.BitSet;

/**
 *��ϣ��������k��λ�����Сm��������ַ�������n�Ĺ�ϵ���Բο��ο�����1��
 *������֤���˶��ڸ�����m��n���� k = ln(2)* m/n ʱ����ĸ�������С�ġ�
 *@author machuan
 * 
 */
public class BoolFilter {

	public int bitSize = 1 << 30;
	public int[] sends = new int[] { 3, 5, 7, 11, 13 };
	public BitSet bitSet = new BitSet(bitSize);
	public SimpleHash[] funcs = new SimpleHash[sends.length];

	public BoolFilter() {
		for (int i = 0; i < sends.length; i++) {
			funcs[i] = new SimpleHash(bitSize, sends[i]);
		}
	}

	public void add(String value) {
		for (SimpleHash f : funcs) {
			bitSet.set(f.hash(value), true);
		}
	}

	public boolean contains(String value) {
		if (value == null) {
			return false;
		}
		boolean ret = true;
		for (SimpleHash f : funcs) {
			ret = ret && bitSet.get(f.hash(value));
		}
		return ret;
	}
}

class SimpleHash {

	private int cap;
	private int seed;

	public SimpleHash(int cap, int seed) {
		this.cap = cap;
		this.seed = seed;
	}

	public int hash(String value) {
		int result = 0;
		int len = value.length();
		for (int i = 0; i < len; i++) {
			result = seed * result + value.charAt(i);
		}
		return (cap - 1) & result;
	}

}
