package io.openmessaging;

import static io.openmessaging.FileStorageManager.storagePath;
import static io.openmessaging.SliceManager.N_ASLICE2;
import static io.openmessaging.SliceManager.N_ASLICE3;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Date;


// 文件存储管理器：主要负责文件相关工作

public class FileStorageManager {

	// 写文件时的缓存大小
	//  如果设置过小，可能会造成文件在磁盘上存储不连续，影响IO性能（此处存疑，待验证）
	//  如果设置过大，则会占用大量内存
    private static final int BUFSZ = 1024 * 1024 * 64;
    
    
    
    static final String storagePath = "./";
//    static final String storagePath = "/alidata1/race2019/data/";
    
    static final String tAxisPointFile = storagePath + "tAxis.point.data";
    static final String tAxisBodyFile = storagePath + "tAxis.bodyref.data";
    static final String tAxisCompressedPointFile = storagePath + "tAxis.zp.data";
    static final String aAxisIndexFile = storagePath + "aAxis.prefixsum.data";
    
    // 构造索引时用传统的RandomAccessFile进行seek和write操作
    static final RandomAccessFile tAxisPointData;
    static final RandomAccessFile tAxisBodyData;
    static final RandomAccessFile tAxisCompressedPointData;
    static final RandomAccessFile aAxisIndexData;
    
    // 读取索引数据时，由于要支持多线程，因此采用FileChannel进行read操作
    static final FileChannel tAxisPointChannel;
    static final FileChannel tAxisBodyChannel;
    static final FileChannel tAxisCompressedPointChannel;
    static final FileChannel aAxisIndexChannel;

    static {
    	RandomAccessFile tpFile, tbFile, tzpFile, aIndexFile;
    	FileChannel tpChannel, tbChannel, tzpChannel, aIndexChannel;
    	try {
			tpFile = new RandomAccessFile(tAxisPointFile, "rw");
			tpFile.setLength(0);
			tbFile = new RandomAccessFile(tAxisBodyFile, "rw");
			tbFile.setLength(0);
			tzpFile = new RandomAccessFile(tAxisCompressedPointFile, "rw");
			tzpFile.setLength(0);
			aIndexFile = new RandomAccessFile(aAxisIndexFile, "rw");
			aIndexFile.setLength(0);
			
			tpChannel = FileChannel.open(Paths.get(tAxisPointFile));
			tbChannel = FileChannel.open(Paths.get(tAxisBodyFile));
			tzpChannel = FileChannel.open(Paths.get(tAxisCompressedPointFile));
			aIndexChannel = FileChannel.open(Paths.get(aAxisIndexFile));
			
		} catch (IOException e) {
			tpFile = null;
			tbFile = null;
			tzpFile = null;
			aIndexFile = null;
			tpChannel = null;
			tbChannel = null;
			tzpChannel = null;
			aIndexChannel = null;
			e.printStackTrace();
			System.exit(-1);
		}
    	tAxisPointData = tpFile;
    	tAxisBodyData = tbFile;
    	tAxisCompressedPointData = tzpFile;
    	aAxisIndexData = aIndexFile;
        tAxisPointChannel = tpChannel;
        tAxisBodyChannel = tbChannel;
        tAxisCompressedPointChannel = tzpChannel;
        aAxisIndexChannel = aIndexChannel;
    }
    
    
    
    
    // 对于a轴2号和3号索引，每个a轴分片都对应一个独立文件
    static final BufferedOutputStream aAxisCompressedPoint2Data[] = new BufferedOutputStream[N_ASLICE2];
    static final FileChannel aAxisCompressedPoint2Channel[] = new FileChannel[N_ASLICE2];
    static long aAxisCompressedPoint2OutputBytes = 0;
    
    static final BufferedOutputStream aAxisCompressedPoint3Data[] = new BufferedOutputStream[N_ASLICE3];
    static final FileChannel aAxisCompressedPoint3Channel[] = new FileChannel[N_ASLICE3];
    static long aAxisCompressedPoint3OutputBytes = 0;
    
    static {
    	try {
	    	for (int i = 0; i < N_ASLICE2; i++) {
	    		String fn = storagePath + String.format("aAxis.zp2.%04d.data", i);
	    		aAxisCompressedPoint2Data[i] = new BufferedOutputStream(new FileOutputStream(fn), BUFSZ);
	    		aAxisCompressedPoint2Channel[i] = FileChannel.open(Paths.get(fn));
	    	}
	    	for (int i = 0; i < N_ASLICE3; i++) {
	    		String fn = storagePath + String.format("aAxis.zp3.%04d.data", i);
	    		aAxisCompressedPoint3Data[i] = new BufferedOutputStream(new FileOutputStream(fn), BUFSZ);
	    		aAxisCompressedPoint3Channel[i] = FileChannel.open(Paths.get(fn));
	    	}
    	} catch (IOException e) {
    		e.printStackTrace();
    		System.exit(-1);
    	}
    }
    
    

	// 为给定文件保留*连续*磁盘空间
	static void reserveDiskSpace(String fileName, long nBytes) throws IOException
	{
		System.out.println("[" + new Date() + "]: " + String.format("reserveDiskSpace: file=%s size=%d", fileName, nBytes));

		byte zeros[] = new byte[4096];
		RandomAccessFile fp = new RandomAccessFile(fileName, "rw");
		// 理论上，用fallocate()系统调用，可以不用写数据而达到预留磁盘空间的目的，但Java8不支持
		// 所以这里使用向文件填0的方法
//		fp.setLength(0);
//		while (nBytes > 0) {
//			int nWrite = (int) Math.min(nBytes, zeros.length);
//			fp.write(zeros, 0, nWrite);
//			nBytes -= nWrite;
//		}
		fp.setLength(nBytes); // FIXME:填0太耗时间了，暂时用setLength代替吧
		fp.close();
		System.out.println("[" + new Date() + "]: reserveDiskSpace: done");
	}
	

}
