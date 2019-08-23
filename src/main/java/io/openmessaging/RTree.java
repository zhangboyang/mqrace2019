package io.openmessaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.nio.channels.FileChannel;
import sun.misc.Unsafe;

import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import sun.nio.ch.FileChannelImpl;
import java.nio.file.Paths;
import java.nio.file.Files;

public class RTree {

    private static final int LMhigh = 255;
    private static final int LMlow = 100;
    
    private static final int PLSIZE = (LMhigh + 1) * 16;
    private static final int DLSIZE = (LMhigh + 1) * 34;
    
    
//    private static final String storagePath = "./";
    private static final String storagePath = "/alidata1/race2019/data/";

    
    private static final Unsafe unsafe;
    private static final Method map0;
    
    private static final String PLFILE = storagePath + "index.ta.data"; 
    private static final String DLFILE = storagePath + "index.body.data";
    
    private static final int MAXLEAF = 20000000;

    private static final long PLLEN = (((long)PLSIZE * MAXLEAF - 1) / 4096 + 1) * 4096;
    private static final long DLLEN = (((long)DLSIZE * MAXLEAF - 1) / 4096 + 1) * 4096;
    
    private static final long plBase;
    private static final long dlBase;
    
    static {
        Unsafe theUnsafe;
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            theUnsafe = (Unsafe) f.get(null);
        } catch (Exception e) {
            theUnsafe = null;
        }
        unsafe = theUnsafe;
        
        Method theMap0;
        try {
        	theMap0 = FileChannelImpl.class.getDeclaredMethod("map0", int.class, long.class, long.class);
        	theMap0.setAccessible(true);
        } catch (Exception e) {
        	theMap0 = null;
        }
        map0 = theMap0;
        
		long thePlBase;
		long theDlBase;
		try {
			Files.deleteIfExists(Paths.get(PLFILE));
			final RandomAccessFile plFile = new RandomAccessFile(PLFILE, "rw");
			plFile.setLength(PLLEN);
			final FileChannel plCh = plFile.getChannel();
			thePlBase = (long) map0.invoke(plCh, 1, 0L, PLLEN);
			
			Files.deleteIfExists(Paths.get(DLFILE));
			final RandomAccessFile dlFile = new RandomAccessFile(DLFILE, "rw");
			dlFile.setLength(DLLEN);
			final FileChannel dlCh = dlFile.getChannel();
			theDlBase = (long) map0.invoke(dlCh, 1, 0L, DLLEN);
		} catch (Exception e) {
			e.printStackTrace();
			thePlBase = 0;
			theDlBase = 0;
			System.exit(-1);
		}
		plBase = thePlBase;
		dlBase = theDlBase;
    }
    
	private static long getPointLeaf(int leafId)
	{
		if (leafId >= MAXLEAF) {
			System.out.println("POINT-LEAF: OUT OF MEMORY!");
			System.exit(-1);
		}
		return plBase + (long)leafId * PLSIZE; 
	}
	
	private static long getDataLeaf(int leafId)
	{
		if (leafId >= MAXLEAF) {
			System.out.println("DATA-LEAF: OUT OF MEMORY!");
			System.exit(-1);
		}
		return dlBase + (long)leafId * DLSIZE; 
	}
	
	private static void doneLeaf(int leafId)
	{
	}
	
	
	
	
	
	private static boolean rectOverlap(long aLeft, long aRight, long aBottom, long aTop, long bLeft, long bRight, long bBottom, long bTop)
	{
		return aLeft <= bRight && aRight >= bLeft && aTop >= bBottom && aBottom <= bTop;
	}
	
	private static boolean pointInRect(long lr, long bt, long rectLeft, long rectRight, long rectBottom, long rectTop)
	{
		return rectLeft <= lr && lr <= rectRight && rectBottom <= bt && bt <= rectTop;
	}
	
	private static boolean rectInRect(long aLeft, long aRight, long aBottom, long aTop, long bLeft, long bRight, long bBottom, long bTop)
	{
		return bLeft <= aLeft && aRight <= bRight && bBottom <= aBottom && aTop <= bTop;
	}
	
    private static long getLR(Message data) { return data.getT(); }
    private static long getBT(Message data) { return data.getA(); }
    
    
    private static final int Mhigh = 15;
    private static final int Mlow = 6;
    private static final int maxK = Mhigh - 2 * Mlow + 2;
    
    private static class NodeEntry { // R-tree Node Entry
    	NodeEntry[] treeptr;
    	int leafptr;
    	
    	int nchild;
    	
    	long left;
    	long right;
    	long bottom;
    	long top;
    	
    	long sumA;
    	int cntA;
    	
    	double enlargement(Message data)
    	{
    		long newLeft = Math.min(left, getLR(data));
    		long newRight = Math.max(right, getLR(data));
    		long newBottom = Math.min(bottom, getBT(data));
    		long newTop = Math.max(top, getBT(data));
    		
    		double newArea = (double)(newRight - newLeft) * (double)(newTop - newBottom);
    		return newArea - area();
    	}
    	
    	double area()
    	{
    		return (double)(right - left) * (double)(top - bottom);
    	}
    	
    	void update()
    	{
    		assert nchild > 0;
    		left = treeptr[0].left;
    		right = treeptr[0].right;
    		bottom = treeptr[0].bottom;
    		top = treeptr[0].top;
    		sumA = treeptr[0].sumA;
    		cntA = treeptr[0].cntA;
    		for (int i = 1; i < nchild; i++) {
	    		left = Math.min(left, treeptr[i].left);
	    		right = Math.max(right, treeptr[i].right);
	    		bottom = Math.min(bottom, treeptr[i].bottom);
	    		top = Math.max(top, treeptr[i].top);
	    		sumA += treeptr[i].sumA;
	    		cntA += treeptr[i].cntA;
    		}
    	}
    }
    
    private static class LRComparator implements Comparator<NodeEntry> {
        @Override
        public int compare(NodeEntry a, NodeEntry b) {
            int r = Long.compare(a.left, b.left);
            return r != 0 ? r : Long.compare(a.right, b.right);
        }
    }
    private static class BTComparator implements Comparator<NodeEntry> {
        @Override
        public int compare(NodeEntry a, NodeEntry b) {
            int r = Long.compare(a.bottom, b.bottom);
            return r != 0 ? r : Long.compare(a.top, b.top);
        }
    }
    private static final LRComparator lrComparator = new LRComparator();
    private static final BTComparator btComparator = new BTComparator();
    
    
    
    
    private static int leafNodeCount = 0;
    private static int treeNodeCount = 0;
    private static int allocLeafNode()
    {
    	return leafNodeCount++;
    }
    private static NodeEntry treeRoot;
    static {
    	treeRoot = new NodeEntry();
    	treeRoot.leafptr = allocLeafNode();
    }
    
    private static NodeEntry[] allocTreeNode()
    {
    	treeNodeCount++;
    	return new NodeEntry[Mhigh + 1]; 
    }
    
    private static final long leafTempArray[] = new long[LMhigh + 1];
    private static NodeEntry splitLeaf(NodeEntry leaf)
    {
    	// 叶子节点的分裂：因为叶子节点里面都是点（不是矩形），所以采用简单的分裂方法，随机找一个轴，按中位数分成两半
    	int axisOffset = (leaf.leafptr % 2) * 8; // 直接用节点编号作随机数
    	
    	long pointLeaf = getPointLeaf(leaf.leafptr);
    	long dataLeaf = getDataLeaf(leaf.leafptr);
    	
    	assert leaf.nchild == LMhigh + 1;
    	for (int i = 0; i <= LMhigh; i++) {
    		leafTempArray[i] = unsafe.getLong(pointLeaf + i * 16 + axisOffset);
    	}
    	Arrays.sort(leafTempArray);
    	long pivot = leafTempArray[(LMhigh + 1) / 2];
    	
    	
    	leaf.nchild = 0;
    	NodeEntry newLeaf = new NodeEntry();
    	newLeaf.leafptr = allocLeafNode();
    	long newPointLeaf = getPointLeaf(newLeaf.leafptr);
    	long newDataLeaf = getDataLeaf(newLeaf.leafptr);
    	
    	int flag = 0;
    	for (int i = 0; i <= LMhigh; i++) {
    		long cur = unsafe.getLong(pointLeaf + i * 16 + axisOffset);
    		long lr = unsafe.getLong(pointLeaf + i * 16);
    		long bt = unsafe.getLong(pointLeaf + i * 16 + 8);
    		
    		if (cur < pivot || (cur == pivot && (flag++) % 2 == 0)) {
    			if (leaf.nchild != i) {
    				unsafe.putLong(pointLeaf + leaf.nchild * 16, lr);
    				unsafe.putLong(pointLeaf + leaf.nchild * 16 + 8, bt);
    				unsafe.copyMemory(null, dataLeaf + i * 34, null, dataLeaf + leaf.nchild * 34, 34);
    			}
    			leaf.nchild++;
    			if (leaf.nchild == 1) {
    				leaf.sumA = bt;
    				leaf.left = leaf.right = lr;
    				leaf.bottom = leaf.top = bt;
    			} else {
    				leaf.sumA += bt;
    				leaf.left = Math.min(leaf.left, lr);
    				leaf.right = Math.max(leaf.right, lr);
    				leaf.bottom = Math.min(leaf.bottom, bt);
    				leaf.top = Math.max(leaf.top, bt);
    			}
    		} else {
    			unsafe.putLong(newPointLeaf + newLeaf.nchild * 16, lr);
    			unsafe.putLong(newPointLeaf + newLeaf.nchild * 16 + 8, bt);
    			unsafe.copyMemory(null, dataLeaf + i * 34, null, newDataLeaf + newLeaf.nchild * 34, 34);
    			newLeaf.nchild++;
    			if (newLeaf.nchild == 1) {
    				newLeaf.sumA = bt;
    				newLeaf.left = newLeaf.right = lr;
    				newLeaf.bottom = newLeaf.top = bt;
    			} else {
    				newLeaf.sumA += bt;
    				newLeaf.left = Math.min(newLeaf.left, lr);
    				newLeaf.right = Math.max(newLeaf.right, lr);
    				newLeaf.bottom = Math.min(newLeaf.bottom, bt);
    				newLeaf.top = Math.max(newLeaf.top, bt);
    			}
    		}
    	}
    	
    	leaf.cntA = leaf.nchild;
    	newLeaf.cntA = newLeaf.nchild;
    	assert leaf.nchild >= LMlow && leaf.nchild < LMhigh;
    	assert newLeaf.nchild >= LMlow && newLeaf.nchild < LMhigh;
    	
    	
    	doneLeaf(leaf.leafptr);
    	doneLeaf(newLeaf.leafptr);
    	
    	return newLeaf;
    }
    
    
    private static final long bbLeft1[] = new long[maxK]; 
    private static final long bbRight1[] = new long[maxK];
    private static final long bbTop1[] = new long[maxK];
    private static final long bbBottom1[] = new long[maxK];
    private static final long bbLeft2[] = new long[maxK]; 
    private static final long bbRight2[] = new long[maxK];
    private static final long bbTop2[] = new long[maxK];
    private static final long bbBottom2[] = new long[maxK];
    
    private static void calcBB(NodeEntry[] a)
    {
    	bbLeft1[0] = a[0].left;
    	bbRight1[0] = a[0].right;
    	bbBottom1[0] = a[0].bottom;
    	bbTop1[0] = a[0].top;
    	for (int i = 1; i < Mlow; i++) {
    		bbLeft1[0] = Math.min(bbLeft1[0], a[i].left);
    		bbRight1[0] = Math.max(bbRight1[0], a[i].right);
    		bbBottom1[0] = Math.min(bbBottom1[0], a[i].bottom);
    		bbTop1[0] = Math.max(bbTop1[0], a[i].top);
    	}
    	for (int i = 1; i < maxK; i++) {
    		bbLeft1[i] = Math.min(bbLeft1[i - 1], a[Mlow + i - 1].left);
    		bbRight1[i] = Math.max(bbRight1[i - 1], a[Mlow + i - 1].right);
    		bbBottom1[i] = Math.min(bbBottom1[i - 1], a[Mlow + i - 1].bottom);
    		bbTop1[i] = Math.max(bbTop1[i - 1], a[Mlow + i - 1].top);
    	}
    	
    	bbLeft2[maxK - 1] = a[Mhigh].left;
    	bbRight2[maxK - 1] = a[Mhigh].right;
    	bbBottom2[maxK - 1] = a[Mhigh].bottom;
    	bbTop2[maxK - 1] = a[Mhigh].top;
    	for (int i = Mhigh - Mlow + 1; i < Mhigh; i++) {
    		bbLeft2[maxK - 1] = Math.min(bbLeft2[maxK - 1], a[i].left);
    		bbRight2[maxK - 1] = Math.max(bbRight2[maxK - 1], a[i].right);
    		bbBottom2[maxK - 1] = Math.min(bbBottom2[maxK - 1], a[i].bottom);
    		bbTop2[maxK - 1] = Math.max(bbTop2[maxK - 1], a[i].top);
    	}
    	for (int i = maxK - 2; i >= 0; i--) {
    		bbLeft2[i] = Math.min(bbLeft2[i + 1], a[Mlow + i].left);
    		bbRight2[i] = Math.max(bbRight2[i + 1], a[Mlow + i].right);
    		bbBottom2[i] = Math.min(bbBottom2[i + 1], a[Mlow + i].bottom);
    		bbTop2[i] = Math.max(bbTop2[i + 1], a[Mlow + i].top);
    	}
    }
    private static NodeEntry split(NodeEntry root)
    {
    	assert root.nchild == Mhigh + 1;
    	double lrMargin = 0, btMargin = 0;
    	
    	Arrays.sort(root.treeptr, lrComparator);
    	calcBB(root.treeptr);
    	for (int i = 0; i < maxK; i++) {
    		lrMargin += bbRight1[i] - bbLeft1[i];
    		lrMargin += bbTop1[i] - bbBottom1[i];
    		lrMargin += bbRight2[i] - bbLeft2[i];
    		lrMargin += bbTop2[i] - bbBottom2[i];
    	}
    	
    	Arrays.sort(root.treeptr, btComparator);
    	calcBB(root.treeptr);
    	for (int i = 0; i < maxK; i++) {
    		btMargin += bbRight1[i] - bbLeft1[i];
    		btMargin += bbTop1[i] - bbBottom1[i];
    		btMargin += bbRight2[i] - bbLeft2[i];
    		btMargin += bbTop2[i] - bbBottom2[i];
    	}
    	
    	if (lrMargin < btMargin) {
    		Arrays.sort(root.treeptr, lrComparator);
        	calcBB(root.treeptr);
    	}
    	
    	double minOverlap = 1e100;
    	double minArea = 1e100;
    	int k = -1;
    	for (int i = 0; i < maxK; i++) {
    		long ovLeft = Math.max(bbLeft1[i], bbLeft2[i]);
    		long ovRight = Math.min(bbRight1[i], bbRight2[i]);
    		long ovBottom = Math.max(bbBottom1[i], bbBottom2[i]);
    		long ovTop = Math.min(bbTop1[i], bbTop2[i]);
    		ovLeft = Math.min(ovLeft, ovRight);
    		ovBottom = Math.min(ovBottom, ovTop);
    		double overlapArea = (double)(ovRight - ovLeft) * (double)(ovTop - ovBottom);
    		double area = (double)(bbRight1[i] - bbLeft1[i]) * (double)(bbTop1[i] - bbBottom1[i]) + (double)(bbRight2[i] - bbLeft2[i]) * (double)(bbTop2[i] - bbBottom2[i]);
    		if (overlapArea == minOverlap) {
    			if (area < minArea) {
    				minArea = area;
    				k = i;
    			}
    		} else if (overlapArea < minOverlap) {
    			minOverlap = overlapArea;
    			minArea = area;
    			k = i;
    		}
    	}
    	assert k >= 0;
    	
		NodeEntry newNode = new NodeEntry();
		newNode.treeptr = allocTreeNode();
		root.nchild = Mlow + k;
		newNode.nchild = Mhigh + 1 - root.nchild; 
		System.arraycopy(root.treeptr, root.nchild, newNode.treeptr, 0, newNode.nchild);
		
		root.update();
		newNode.update();
		assert root.left == bbLeft1[k];
		assert root.right == bbRight1[k];
		assert root.bottom == bbBottom1[k];
		assert root.top == bbTop1[k];
		assert newNode.left == bbLeft2[k];
		assert newNode.right == bbRight2[k];
		assert newNode.bottom == bbBottom2[k];
		assert newNode.top == bbTop2[k];
		
    	return newNode;
    }
    
    private static NodeEntry leafInsert(NodeEntry leaf, Message data)
    {
    	long pointLeaf = getPointLeaf(leaf.leafptr);
    	unsafe.putLong(pointLeaf + leaf.nchild * 16, getLR(data));
    	unsafe.putLong(pointLeaf + leaf.nchild * 16 + 8, getBT(data));
    	
    	long dataLeaf = getDataLeaf(leaf.leafptr);
    	unsafe.copyMemory(data.getBody(), unsafe.ARRAY_BYTE_BASE_OFFSET, null, dataLeaf + leaf.nchild * 34, 34);
    	
    	doneLeaf(leaf.leafptr);
    	
    	leaf.nchild++;
    	if (leaf.nchild > LMhigh) {
    		return splitLeaf(leaf);
    	}
    	
    	leaf.left = Math.min(leaf.left, getLR(data));
    	leaf.right = Math.max(leaf.right, getLR(data));
    	leaf.bottom = Math.min(leaf.bottom, getBT(data));
    	leaf.top = Math.max(leaf.top, getBT(data));
    	leaf.sumA += data.getA();
    	leaf.cntA++;
    	
    	return null;
    }
    
    
    
    private static NodeEntry insert(NodeEntry root, Message data)
    {
    	if (root.treeptr == null) {
    		return leafInsert(root, data);
    	}
    	
    	double min_enlargement = root.treeptr[0].enlargement(data);
    	double min_area = root.treeptr[0].area();
    	int chid = 0;
    	for (int i = 1; i < root.nchild; i++) {
    		double cur_enlargement = root.treeptr[i].enlargement(data);
    		if (cur_enlargement == min_enlargement) {
    			double cur_area = root.treeptr[i].area();
    			if (cur_area < min_area) {
    				min_area = cur_area;
    				chid = i;
    			}
    		} else if (cur_enlargement < min_enlargement) {
    			min_enlargement = cur_enlargement;
    			min_area = root.treeptr[i].area();
    			chid = i;
    		}
    	}
    	
    	NodeEntry newNode = insert(root.treeptr[chid], data);
    	
    	if (newNode != null) {
    		root.treeptr[root.nchild++] = newNode;
    		if (root.nchild > Mhigh) {
    			return split(root);
    		}
    	}
    	
    	root.left = Math.min(root.left, getLR(data));
    	root.right = Math.max(root.right, getLR(data));
    	root.bottom = Math.min(root.bottom, getBT(data));
    	root.top = Math.max(root.top, getBT(data));
    	root.sumA += data.getA();
    	root.cntA++;
    	
    	return null;
    }
    
    public static NodeEntry insertToTree(NodeEntry root, Message data)
    {
    	NodeEntry oldRoot = root;
    	NodeEntry newNode = insert(root, data);
    	if (newNode == null) {
    		return oldRoot;
    	} else {
    		NodeEntry newRoot = new NodeEntry();
    		
    		newRoot.treeptr = allocTreeNode();
    		newRoot.treeptr[0] = oldRoot;
    		newRoot.treeptr[1] = newNode;
    		newRoot.nchild = 2;
    		
    		newRoot.update();
    		return newRoot;
    	}
    }
    public static void insert(Message data)
    {
    	treeRoot = insertToTree(treeRoot, data);
    }
    
    public static void finishInsert()
    {
    	System.out.println("[" + new Date() + "]: RTree.finishInsert()");
    	
    	System.out.println(String.format("tree-node: %d", treeNodeCount));
    	System.out.println(String.format("leaf-node: %d", leafNodeCount));
    }
    
    

    
    
    
    //////////////////////////////////////////////////////////////////////////////////////////////
    
    private static void queryData(NodeEntry root, ArrayList<Message> result, long left, long right, long bottom, long top)
    {
    	if (root.treeptr == null) {
    		long pointLeaf = getPointLeaf(root.leafptr);
        	long dataLeaf = getDataLeaf(root.leafptr);
        	
    		for (int i = 0; i < root.nchild; i++) {
            	long lr = unsafe.getLong(pointLeaf + i * 16);
            	long bt = unsafe.getLong(pointLeaf + i * 16 + 8);
            	if (pointInRect(lr, bt, left, right, bottom, top)) {
            		byte body[] = new byte[34];
            		unsafe.copyMemory(null, dataLeaf + i * 34, body, unsafe.ARRAY_BYTE_BASE_OFFSET, body.length);
            		result.add(new Message(bt, lr, body));
            	}
    		}
    		
    		doneLeaf(root.leafptr);
    		return;
    	}
    	for (int i = 0; i < root.nchild; i++) {
    		NodeEntry ch = root.treeptr[i];
    		if (rectOverlap(ch.left, ch.right, ch.bottom, ch.top, left, right, bottom, top)) {
    			queryData(ch, result, left, right, bottom, top);
    		}
    	}
    }
    
    public static void queryData(ArrayList<Message> result, long left, long right, long bottom, long top)
    {
    	queryData(treeRoot, result, left, right, bottom, top);
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    //////////////////////////////////////////////////////////////////////////////////////////////
    
    private static class AverageResult {
    	long sum;
    	int cnt;
    }
    
    private static void queryAverage(NodeEntry root, AverageResult result, long left, long right, long bottom, long top)
    {
    	if (root.treeptr == null) {
        	long pointLeaf = getPointLeaf(root.leafptr);
        	
    		for (int i = 0; i < root.nchild; i++) {
            	long lr = unsafe.getLong(pointLeaf + i * 16);
            	long bt = unsafe.getLong(pointLeaf + i * 16 + 8);
            	if (pointInRect(lr, bt, left, right, bottom, top)) {
            		result.sum += bt;
            		result.cnt++;
            	}
    		}
    		
    		doneLeaf(root.leafptr);
    		return;
    	}
    	for (int i = 0; i < root.nchild; i++) {
    		NodeEntry ch = root.treeptr[i];
    		if (rectOverlap(ch.left, ch.right, ch.bottom, ch.top, left, right, bottom, top)) {
    			if (rectInRect(ch.left, ch.right, ch.bottom, ch.top, left, right, bottom, top)) {
    				result.sum += ch.sumA;
    				result.cnt += ch.cntA;
    			} else {
    				queryAverage(ch, result, left, right, bottom, top);
    			}
    		}
    	}
    }
    public static long queryAverage(long left, long right, long bottom, long top)
    {
    	AverageResult result = new AverageResult();
    	queryAverage(treeRoot, result, left, right, bottom, top);
    	return result.cnt > 0 ? result.sum / result.cnt : 0;
    }
}
