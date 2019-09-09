package io.openmessaging.util;

import java.util.Comparator;

import io.openmessaging.Message;

// 一些杂七杂八的工具函数
public class Util {
	
	// 返回不小于n的最小2的整数次幂
	// 如：nextSize(1000)=1024；nextSize(1024)=1024
    public static int nextSize(int n)
    {
    	int r = 1;
    	while (r < n) r <<= 1;
    	return r;
    }
    
    
    // 判断点是否在矩形内部
	public static boolean pointInRect(long lr, long bt, long rectLeft, long rectRight, long rectBottom, long rectTop)
	{
		return rectLeft <= lr && lr <= rectRight && rectBottom <= bt && bt <= rectTop;
	}
	
	// 在数组的指定范围内进行二分查找
	public static int lowerBound(long array[], int l, int r, long value)
	{
		while (r - l > 1) {
			int m = (l + r) / 2;
			if (value >= array[m]) {
				l = m;
			} else {
				r = m;
			}
		}
		return l;
	}
	
	// Message的比较函数
    private static class TComparator implements Comparator<Message> {
        @Override
        public int compare(Message a, Message b) {
            return Long.compare(a.getT(), b.getT());
        }
    }
    public static final TComparator tComparator = new TComparator();
    private static class AComparator implements Comparator<Message> {
        @Override
        public int compare(Message a, Message b) {
        	return Long.compare(a.getA(), b.getA());
        }
    }
    public static final AComparator aComparator = new AComparator();
    
    
    
    // 将Message转为人类可读的字符串
	public static String dumpMessage(Message message)
	{
		char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    	StringBuilder s = new StringBuilder();
    	s.append(String.format("%016X,", message.getT()));
    	s.append(String.format("%016X,", message.getA()));
    	byte[] bytes = message.getBody();
    	if (bytes != null) {
	    	for (int j = 0; j < bytes.length; j++) {
	            int v = bytes[j] & 0xFF;
	            s.append(HEX_ARRAY[v >>> 4]);
	            s.append(HEX_ARRAY[v & 0x0F]);
	        }
    	} else {
    		s.append("NULL");
    	}
    	return s.toString();
	}
}
