package io.openmessaging.util;

public class Compact2DLongArray {

	// 详细说明见Compact2DIntArray
	
	final long array[];
	final int width;
	final int height;
	
	public Compact2DLongArray(int _height, int _width)
	{
		width = _width;
		height = _height;
		array = new long[width * height];
	}
	
	private void check(int x, int y)
	{
		assert 0 <= x && x < height && 0 <= y && y < width;
	}
	
	public long get(int x, int y)
	{
		check(x, y);
		return array[x * width + y];
	}
	
	public void set(int x, int y, long value)
	{
		check(x, y);
		array[x * width + y] = value;
	}
	
	public void inc(int x, int y)
	{
		check(x, y);
		array[x * width + y]++;
	}
	
	public void add(int x, int y, long value)
	{
		check(x, y);
		array[x * width + y] += value;
	}
}