/**
 * 
 */
package cn.edu.bistu.ngpod.crawler;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author hadoop
 *
 */
public class Test {
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	
	
	/**
	 * @param args
	 * @throws ConfigurationException 
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public static void main(String[] args){
		Configuration config = HBaseConfiguration.create();
		Connection connection=null;
		Table table=null;
		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf("ngpod"));
			Calendar today = Calendar.getInstance();
			Date first = sdf.parse("20090101");
			while(today.getTime().after(first)){
				Get g = new Get(Bytes.toBytes(sdf.format(today.getTime())));
				g.addColumn(Bytes.toBytes("p"), Bytes.toBytes("pageId"));
				g.addColumn(Bytes.toBytes("p"), Bytes.toBytes("desc"));
				Result result = table.get(g);
				if(result.isEmpty()){
					System.out.println("行键："+sdf.format(today.getTime())+"不存在");
				}else{
					System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("p"), Bytes.toBytes("pageId")))+"-"+Bytes.toString(result.getValue(Bytes.toBytes("p"), Bytes.toBytes("desc"))));
				}
				today.add(Calendar.DAY_OF_YEAR, -1);
			}
		} catch (IOException | ParseException e) {
			e.printStackTrace();
		} finally{
			try{
				if (table != null)
					table.close();
				if (connection != null)
					connection.close();
			}catch(IOException e){
				e.printStackTrace();
			}
		}

	}

}
