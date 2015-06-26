/**
 * 
 */
package cn.edu.bistu.ngpod.crawler;

import java.io.Serializable;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase中的一个Cell是最小的存储单位，以K-V对的形式存储，
  *  其中的Key由四个维度组成：表名-列族名-列名-时间版本，
  *  当前类是用来保存Key中CF、列名以及Value的简单Bean
 * @author hadoop
 */
public class HBaseCell implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5636087215933192645L;

	/**
	 * 列族名
	 */
	private byte[] cf = null;
	/**
	 * 列限定符/列名
	 */
	private byte[] col = null;
	
	private byte[] value = null;
	
	public HBaseCell(String cf, String col,String value) {
		this.cf = Bytes.toBytes(cf);
		this.col = Bytes.toBytes(col);
		this.value = Bytes.toBytes(value);
	}

	public HBaseCell(byte[] cf, byte[] col, byte[] value) {
		super();
		this.cf = cf;
		this.col = col;
		this.value = value;
	}


	public byte[] getCf() {
		return cf;
	}

	public byte[] getCol() {
		return col;
	}
	
	public byte[] getValue(){
		return value;
	}
	
}
