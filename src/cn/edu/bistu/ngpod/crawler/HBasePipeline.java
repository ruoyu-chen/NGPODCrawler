/**
 * 
 */
package cn.edu.bistu.ngpod.crawler;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * Write crawled data into HBase.
 * 
 * @author hadoop
 *
 */
public class HBasePipeline implements Pipeline {
	private static Logger log = Logger.getLogger(HBasePipeline.class);
	private TableName tableName = null;
	private Configuration config = null;
	private Connection connection = null;
	private Table table = null;
	/**
	 * 用于HBase行键的字段名，默认为id
	 */
	private String id="id";
	

	public HBasePipeline(byte[] table,String id) {
		this.tableName = TableName.valueOf(table);
		this.id=id;
		config = HBaseConfiguration.create();
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * us.codecraft.webmagic.pipeline.Pipeline#process(us.codecraft.webmagic
	 * .ResultItems, us.codecraft.webmagic.Task)
	 */
	@Override
	public void process(ResultItems results, Task task) {
		if(results.isSkip()){
			log.info("跳过当前页面:"+results.getRequest().getUrl());
			return;
		}
		if(results.get(id)==null){
			log.error("页面抓取的结果集中没有id字段");
			return;
		}
		try {
			connection = ConnectionFactory.createConnection(config);
			this.table = connection.getTable(tableName);
			Map<String, Object> items = results.getAll();
			HBaseCell id = (HBaseCell)results.get(this.id);
			//准备将要插入HBase的数据，用PUT方法
			Put p = new Put(id.getValue());
			for(String col:items.keySet()){
				HBaseCell cell = (HBaseCell)results.get(col);
				p.addColumn(cell.getCf(), cell.getCol(), cell.getValue());
			}
			//执行数据添加
			table.put(p);
		} catch (IOException e1) {
			e1.printStackTrace();
		}finally{
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
