/**
 * 
 */
package cn.edu.bistu.ngpod.crawler;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * 针对Redis中保存的抓取历史， 对已经抓取到的内容进行检查， 查看是否存在有已抓取但未持久化的页面， 并将这些页面重新添加到redis中的待抓取队列中。
 * 
 * @author chenruoyu
 *
 */
public class CrawlerCheck {

	/**
	 * redis服务器的地址
	 */
	private static final String host = "127.0.0.1";

	/**
	 * 历史记录（set类型）key
	 */
	private static final String history_key = "set_photography.nationalgeographic.com";

	/**
	 * 待抓取队列（list类型）key
	 */
	private static final String queue_key = "queue_photography.nationalgeographic.com";
	
	private static final String url_prefix = "http://photography.nationalgeographic.com/photography/photo-of-the-day/";

	private JedisPool pool = null;

	private static final Logger log = Logger.getLogger(CrawlerCheck.class);

	private TableName tableName = null;
	private byte[] cf = null;
	private byte[] id = Bytes.toBytes("pageId");
	private Configuration config = null;
	private Connection connection = null;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

	public CrawlerCheck(String table, String cf) throws IOException {
		pool = new JedisPool(host);
		this.tableName = TableName.valueOf(table);
		this.cf = Bytes.toBytes(cf);
	}

	public void check() {
		Jedis jedis = pool.getResource();
		Table table = null;
		long start = 0;
		try {
			Date first = sdf.parse("20090101");
			config = HBaseConfiguration.create();
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(tableName);
			// 从redis中取出历史记录集合的所有数据
			Set<String> history = jedis.smembers(history_key);
			/**
			 * 遍历HBase中的抓取结果表， 从上述Redis历史记录集合中删除HBase抓取结果表里的所有记录，
			 * 则Redis历史记录中留下的记录就是已抓取但未能及时持久化的页面。
			 */
			Calendar today = Calendar.getInstance();
			today.add(Calendar.DAY_OF_YEAR, -1);
			Result result = null;
			String pageId = null;
			start = System.currentTimeMillis();
			while (today.getTime().after(first)) {
				// 当前日期在2009年1月1日之后
				Get g = new Get(Bytes.toBytes(sdf.format(today.getTime())));
				g.addColumn(cf, id);
				result = table.get(g);
				if (result.isEmpty()) {
					// 遇到没有抓取的日期，停止循环
					break;
				} else {
					pageId = Bytes.toString(result.getValue(cf, id));
					if (!history.remove(url_prefix+pageId+"/")) {
						log.warn(pageId + "在HBase中存在而在Redis中不存在");
					}
				}
				today.add(Calendar.DAY_OF_YEAR, -1);
			}
			Iterator<String> ite = history.iterator();
			String url = null;
			// 遍历剩下的历史记录集合
			while (ite.hasNext()) {
				url = ite.next();
				jedis.rpush(queue_key, url);
				log.info("将" + url + "添加至待抓取列表");
			}
		} catch (IOException | ParseException e) {
			e.printStackTrace();
		} finally {
			pool.returnResource(jedis);
			try {
				if (table != null)
					table.close();
				if (connection != null)
					connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Check总用时:"+(System.currentTimeMillis()-start)+"ms");
	}

	/**
	 * 向Redis队列中重新添加所有任务，重新执行抓取工作
	 */
	public void resetAllJob() {
		Jedis jedis = pool.getResource();
		try {
			// 从redis中取出历史记录集合的所有数据
			Set<String> history = jedis.smembers(history_key);
			Iterator<String> ite = history.iterator();
			String url = null;
			// 遍历历史记录集合
			while (ite.hasNext()) {
				url = ite.next();
				jedis.rpush(queue_key, url);
			}
		} finally {
			pool.returnResource(jedis);
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		CrawlerCheck check = new CrawlerCheck("ngpod", "p");
		check.check();
		System.out.println("总用时:"+(System.currentTimeMillis()-start)+"ms");
	}
}
