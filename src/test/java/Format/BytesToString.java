package Format;

import java.math.BigInteger;

public class BytesToString {
	// 1byte
	public static String Byte1ToString(byte[] one) {
		int reslut = (byte) one[0];
		if (reslut < 0) {
			reslut = reslut + 256;
		}
		return "" + reslut;
	}

	// 2bytes
	public static String Bytes2ToString(byte[] two) {
		int[] ints = new int[2];
		ints[0] = two[0] & 0x000000ff;
		ints[1] = two[1] & 0xff;
		int result = ints[0] << 8 | ints[1];
		return "" + result;
	}

	// 4bytes
	public static String Bytes4ToString(byte[] four) {
		long[] longs = new long[4];
		longs[0] = four[0] & 0x00000000000000ff;
		longs[1] = four[1] & 0xff;
		longs[2] = four[2] & 0xff;
		longs[3] = four[3] & 0xff;
		long result = longs[0] << 24 | longs[1] << 16 | longs[2] << 8
				| longs[3];
		return "" + result;
	}

	// 8bytes
	// =============8 BYTES TO STRING==============
	public static long Bytes8tolong(byte[] eight) {

		long[] longs = new long[8];
		longs[0] = eight[0] & 0x00000000000000ff;
		longs[1] = eight[1] & 0xff;
		longs[2] = eight[2] & 0xff;
		longs[3] = eight[3] & 0xff;
		longs[4] = eight[4] & 0xff;
		longs[5] = eight[5] & 0xff;
		longs[6] = eight[6] & 0xff;
		longs[7] = eight[7] & 0xff;
		long result = longs[0] << 56 | longs[1] << 48 | longs[2] << 40
				| longs[3] << 32 | longs[4] << 24 | longs[5] << 16
				| longs[6] << 8 | longs[7];
		return result;
	}

	public static String Bytes8toString(byte[] eight) {
		if (eight[0] >= 0) {
			return "" + Bytes8tolong(eight);
		}
		// 符号位变0
		eight[0] = (byte) (eight[0] & 0x7f);
		// 变符号后的值
		long unsigvalue = Bytes8tolong(eight);
		String v64 = "9223372036854775808";
		BigInteger Bigv64 = new BigInteger(v64);
		// 还原++
		return Bigv64.add(BigInteger.valueOf(unsigvalue)).toString();
	}

	public static String CharstoString(byte[] chars) {
		return new String(WipeOffBinZero(chars, 0, chars.length));
	}

	public static byte[] WipeOffBinZero(byte[] bytes, int offset, int len) {
		int count = 0;
		for (int i = offset; i < offset + len; i++) {
			if (bytes[i] == 0x0) {
				break;
			}
			count++;
		}// end for
		byte[] tmp = new byte[count];
		System.arraycopy(bytes, offset, tmp, 0, count);
		return tmp;
	}
}
