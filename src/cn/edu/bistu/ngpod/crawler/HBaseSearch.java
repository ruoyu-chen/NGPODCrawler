/**
 * 
 */
package cn.edu.bistu.ngpod.crawler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * @author hadoop
 *
 */
public class HBaseSearch {
	private static final Logger log = Logger.getLogger(HBaseSearch.class);
	private static final TableName tableName = TableName.valueOf("ngpod");
	private static final byte[] cf_page = Bytes.toBytes("p");
	private static final byte[] cf_raw = Bytes.toBytes("r");
	private static final Configuration config = HBaseConfiguration.create();
	private static final byte[] col_title=Bytes.toBytes("title");
	private static final byte[] col_pageId=Bytes.toBytes("pageId");
	private static final byte[] col_credit=Bytes.toBytes("credit");
	private static final byte[] col_pubTime=Bytes.toBytes("pubTime");
	private static final byte[] col_ptInt=Bytes.toBytes("ptInt");
	private static final byte[] col_photo=Bytes.toBytes("photo");
	private static final byte[] col_wp=Bytes.toBytes("wp");
	private static final byte[] col_desc=Bytes.toBytes("desc");
	
	private static HBaseSearch instance=null;
	
	private HBaseSearch(){
		
	}
	
	public static HBaseSearch getInstance(){
		if(instance == null){
			instance = new HBaseSearch();
		}
		return instance;
	}
	
	/**
	 * 在HBase中搜索一个单一页面的详情
	 * @param ptInt
	 * @return
	 */
	public Ngpod getDetail(int ptInt){
		return getDetail(String.valueOf(ptInt));
	}
	/**
	 * 获得最近的一天的NGPOD
	 * @return
	 */
	public Ngpod getLatest(){
		Connection connection = null;
		Table table = null;
		ResultScanner rs = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(tableName);
			PageFilter pf = new PageFilter(1);
			//Push down predicate
			Scan scan = new Scan();
			scan.addColumn(cf_page, col_ptInt);
			scan.setReversed(true);
			scan.setFilter(pf);
			rs = table.getScanner(scan);
			Result r = rs.next();
			if(r!=null&&!r.isEmpty()){
				String ptInt = Bytes.toString(r.getValue(cf_page, col_ptInt));
				return getDetail(ptInt);
			}else{
				return null;
			}
		} catch (IOException e1) {
			e1.printStackTrace();
			return null;
		}finally{
			try{
				if(rs != null)
					rs.close();
				if (table != null)
					table.close();
				if (connection != null)
					connection.close();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 获取某一天的NGPOD的详情
	 * @param ptInt
	 * @return
	 */
	public Ngpod getDetail(String ptInt){
		if(ptInt==null||"".equals(ptInt)){
			return null;
		}
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(tableName);
			Get g = new Get(Bytes.toBytes(ptInt));			
			g.addColumn(cf_page, col_pageId);
			g.addColumn(cf_page, col_title);
			g.addColumn(cf_page, col_credit);
			g.addColumn(cf_page, col_pubTime);
			g.addColumn(cf_page, col_ptInt);
			g.addColumn(cf_page, col_photo);
			g.addColumn(cf_page, col_wp);
			g.addColumn(cf_page, col_desc);
			Result result = table.get(g);
			if(result.isEmpty()){
				return null;
			}else{
				return getPod(result);
			}
		} catch (IOException e1) {
			e1.printStackTrace();
			return null;
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
	
	private Ngpod getPod(Result r){
		Ngpod pod = new Ngpod();
		pod.setPageId(Bytes.toString(r.getValue(cf_page, col_pageId)));
		pod.setTitle(Bytes.toString(r.getValue(cf_page, col_title)));
		pod.setCredit(Bytes.toString(r.getValue(cf_page, col_credit)));
		pod.setPubTime(Bytes.toString(r.getValue(cf_page, col_pubTime)));
		pod.setPtInt(Bytes.toString(r.getValue(cf_page, col_ptInt)));
		pod.setPhoto(Bytes.toString(r.getValue(cf_page, col_photo)));
		pod.setWallpaper(Bytes.toString(r.getValue(cf_page, col_wp)));
		pod.setDesc(Bytes.toString(r.getValue(cf_page, col_desc)));
		return pod;
	}
	
	
	/**
	 * 获取从指定日期开始的一定范围内的NGPOD列表
	 * @param startDate
	 * @param limit
	 * @param reverse 为true表示获取的日期是由较新到较旧的，为false表示获取的日期是由较旧到较新的。
	 * @return
	 */
	public List<Ngpod> getPodList(String startDate,int limit, boolean reverse){
		ArrayList<Ngpod> list = new ArrayList<Ngpod>();
		if(startDate==null||"".equals(startDate)){
			return list;
		}
		Connection connection = null;
		Table table = null;
		ResultScanner rs = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(tableName);
			PageFilter pf = new PageFilter(limit);
			Scan scan = new Scan();
			scan.setReversed(reverse);
			scan.setFilter(pf);
			scan.setStartRow(Bytes.toBytes(startDate));
			rs = table.getScanner(scan);
			for(Result result : rs){
				list.add(getPod(result));				
			}
			return list;
		} catch (IOException e) {
			e.printStackTrace();
			return list;
		}finally{
			try{
				if (rs != null)
					rs.close();
				if (table != null)
					table.close();
				if (connection != null)
					connection.close();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 获取从指定日期开始的一定范围内的NGPOD列表，且这些POD必须包含壁纸信息
	 * @param startDate
	 * @param limit
	 * @param reverse
	 * @return
	 */
	public List<Ngpod> getPodListWithWP(String startDate, int limit, boolean reverse){
		ArrayList<Ngpod> podList = new ArrayList<Ngpod>();
		if(startDate==null||"".equals(startDate)){
			return podList;
		}
		Connection connection = null;
		Table table = null;
		ResultScanner rs = null;
		try {
			SingleColumnValueFilter scvf = new SingleColumnValueFilter(cf_page,col_wp,CompareOp.NOT_EQUAL,new NullComparator());
			scvf.setFilterIfMissing(true);
			PageFilter pf = new PageFilter(limit);
			List<Filter> list = new ArrayList<Filter>();
			list.add(scvf);
			list.add(pf);
			FilterList flist = new FilterList(Operator.MUST_PASS_ALL, list);
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(tableName);
			Scan scan = new Scan();
			scan.setFilter(flist);
			scan.addFamily(cf_page);
			scan.setReversed(reverse);
			scan.setStartRow(Bytes.toBytes(startDate));
			rs = table.getScanner(scan);
			for(Result result : rs){
				podList.add(getPod(result));				
			}
			return podList;
		} catch (IOException e) {
			e.printStackTrace();
			return podList;
		} finally {
			try{
				if (rs != null)
					rs.close();
				if (table != null)
					table.close();
				if (connection != null)
					connection.close();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		HBaseSearch search = new HBaseSearch();
		//Ngpod pod = search.getLatest();
		//System.out.println(pod.toString());
//		Ngpod pod = search.getDetail("20150619");
//		if(pod==null){
//			System.out.println("POD=NULL");
//			System.exit(-1);
//		}
		//System.out.println(pod.toString());
		List<Ngpod> pods = search.getPodListWithWP("20130101", 10, false);
		for(Ngpod pod : pods){
			System.out.println(pod);
		}
		System.out.println("总耗时"+(System.currentTimeMillis()-start)+"ms");
	}
}
