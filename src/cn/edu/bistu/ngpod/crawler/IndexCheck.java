/**
 * 
 */
package cn.edu.bistu.ngpod.crawler;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 针对Redis中保存的抓取历史，对Lucene的索引进行检查， 查看是否存在有已抓取但未写入索引的页面，并将这些页面重新添加到redis中的待抓取队列中。
 * 
 * @author chenruoyu
 *
 */
public class IndexCheck {

	private static HashSet<String> duplicatePhotoSet = null;
	
	private static HashSet<String> duplicateWallSet = null;
	
	private static final String[] duplicatePhotos={
		"/automat-new-york-roberts_8004_990x742.jpg",
		"/keoladeo-park-india_3660_990x742.jpg",
		"/bee-eater-szentpeteri_3712_990x742.jpg",
		"/tuareg-tribesman-libya_3673_990x742.jpg",
		"/tango-buenos_3586_990x742.jpg",
		"/dog-yawn-082009_3630_990x742.jpg",
		"/coastline-snares-lanting_3654_990x742.jpg",
		"/traveling-circus-massachusett_3587_990x742.jpg",
		"/iowa-derby-car-jump_6319_990x742.jpg",
		"/cobblestone-louisiana-wisherd_8007_990x742.jpg",
		"/cuban-tree-frog_3627_990x742.jpg",
		"/northern-spotted-owl_6327_990x742.jpg",
		"/smiths-green-eyed-gecko_8627_990x742.jpg",
		"/football-stadium-colorado_3571_990x742.jpg",
		"/lightning-beach-larkin_3694_990x742.jpg",
		"/tiger-cub-face-082109_3645_990x742.jpg",
		"/kolmanskop-namibia_3749_990x742.jpg",
		"/owachomo-bridge_3731_990x742.jpg",
		"/octopus-hawaii_3605_990x742.jpg",
		"/candle-spruces-finland_6310_990x742.jpg",
		"/koi-feeding-heisch_3748_990x742.jpg",
		"/colosseum-italy_3566_990x742.jpg",
		"/african-elephant-tanzania_3648_990x742.jpg",
		"/bathing-elephant-midha_3741_990x742.jpg",
		"/lightning-model-westinghouse_8020_990x742.jpg",
		"/triplet-policemen-yamashita_8029_990x742.jpg",
		"/pigs-africa-kopp_3756_990x742.jpg",
		"/daredevil-india_3567_990x742.jpg",
		"/manta-rays-hanifaru-bay_6326_990x742.jpg",
		"/penguins-underwater_3667_990x742.jpg",
		"/humpback-whales-macdonald_3691_990x742.jpg"};
	
	private static final String[] duplicateWalls={
		"3660_1600x1200-wallpaper-cb1267712116.jpg",
		"3712_1600x1200-wallpaper-cb1267712148.jpg",
		"3630_1600x1200-wallpaper-cb1267712095.jpg",
		"6319_1600x1200-wallpaper-cb1267712493.jpg",
		"8007_1600x1200-wallpaper-cb1332780802.jpg",
		"3627_1600x1200-wallpaper-cb1318004059.jpg",
		"6327_1600x1200-wallpaper-cb1318964613.jpg",
		"8627_1600x1200-wallpaper-cb1267712855.jpg",
		"3571_1600x1200-wallpaper-cb1267712051.jpg",
		"3694_1600x1200-wallpaper-cb1320173687.jpg",
		"3645_1600x1200-wallpaper-cb1267712107.jpg",
		"3749_1600x1200-wallpaper-cb1271854488.jpg",
		"3731_1600x1200-wallpaper-cb1317995374.jpg",
		"3605_1600x1200-wallpaper-cb1267712074.jpg",
		"6310_1600x1200-wallpaper-cb1366653754.jpg",
		"3748_1600x1200-wallpaper-cb1267712173.jpg",
		"3566_1600x1200-wallpaper-cb1267712049.jpg",
		"3648_1600x1200-wallpaper-cb1319543855.jpg",
		"3741_1600x1200-wallpaper-cb1320173687.jpg",
		"8020_1600x1200-wallpaper-cb1267712767.jpg",
		"8029_1600x1200-wallpaper-cb1320173687.jpg",
		"3756_1600x1200-wallpaper-cb1267712180.jpg",
		"6326_1600x1200-wallpaper-cb1267712497.jpg",
		"3667_1600x1200-wallpaper-cb1320173687.jpg",
		"3691_1600x1200-wallpaper-cb1267712135.jpg"
	};

	
	static{
		duplicatePhotoSet = new HashSet<String>();
		for(String str: duplicatePhotos){
			duplicatePhotoSet.add(str);
		}
		duplicateWallSet = new HashSet<String>();
		for(String str: duplicateWalls){
			duplicateWallSet.add(str);
		}
		
	}
	
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

	private JedisPool pool = null;

	private String luceneDir = "/Users/chenruoyu/Documents/workspace/NGPODCollector/index";
	
	private String photoDir = "/Users/chenruoyu/Documents/workspace/NGPODCollector/photos";

	private String wallDir = "/Users/chenruoyu/Documents/workspace/NGPODCollector/wallpaper";

	private String prefix = "http://photography.nationalgeographic.com/photography/photo-of-the-day/";

	private IndexReader reader = null;

	public IndexCheck() throws IOException {
		pool = new JedisPool(host);
		Directory dir = FSDirectory.open(new File(luceneDir).toPath());
		reader = DirectoryReader.open(dir);
	}

	public void check() throws IOException {
		Jedis jedis = pool.getResource();
		IndexSearcher searcher = new IndexSearcher(reader);
		Query query = null;
		Term term = null;
		try {
			// 从redis中取出历史记录集合的所有数据
			Set<String> history = jedis.smembers(history_key);
			Iterator<String> ite = history.iterator();
			String url = null;
			String pageId = null;
			// 遍历历史记录集合
			while (ite.hasNext()) {
				url = ite.next();
				if (url.startsWith(prefix)&&!prefix.equalsIgnoreCase(url)) {					
					pageId = url.substring(71, url.length() - 1);
				} else {
					System.out.println("URL:" + url + "格式不符合要求");
					continue;
				}
				// 检查对应的pageId在索引中是否出现过
				term = new Term("pageId", pageId);
				query = new TermQuery(term);
				TopDocs topDoc = searcher.search(query, 10);
				if (topDoc.totalHits==1) {
					//有命中纪录，且命中的条数为1，说明在索引中也有相应日期的数据
					//System.out.println("命中："+url);
					//需要进一步检查图片是否被下载下来
					ScoreDoc[] docs = topDoc.scoreDocs;
					Document doc = reader.document(docs[0].doc);
					String photo = doc.get("photo");
					if(duplicatePhotoSet.contains(photo)){
						System.out.println(doc.get("pageId"));
					}
					if(photo==null||"".equals(photo)){
						System.out.println(doc.get("pageId")+"页面的图片地址未找到");
					}
					if(!exists(photoDir,photo)){
						System.out.println(doc.get("pageId")+"页面的图片未下载");
						//图片未下载，需要放回队列中重新下载
			            jedis.rpush(queue_key, url);
					}
				}else{
					if(topDoc.totalHits>1){
						//在索引中有超过一项的命中纪录，说明存在重复抓取和索引
			            System.err.println("URL："+url+",被重复抓取！");
					}else{
						//在索引中没有对应纪录，需要在redis内存队列中添加对应的URL，实现再次抓取
						//但在添加之前并没有进一步判断待抓取队列中是否已经有了对应的URL，
						//因此，如果待抓取队列中已经存在了对应URL，会造成重复抓取.
						//但是，在LucenePipeLine中建立索引时，使用的是updateDocument方法，
						//可以避免文档被重复索引
			            jedis.rpush(queue_key, url);
			            System.out.println("将URL："+url+",重新加入待抓取队列中");						
					}
				}
			}
		} finally {
			pool.returnResource(jedis);
		}
	}

	public void checkWallpaper() throws IOException{
		Jedis jedis = pool.getResource();
		IndexSearcher searcher = new IndexSearcher(reader);
		Query query = null;
		Term term = null;
		try {
			// 从redis中取出历史记录集合的所有数据
			Set<String> history = jedis.smembers(history_key);
			Iterator<String> ite = history.iterator();
			String url = null;
			String pageId = null;
			// 遍历历史记录集合
			while (ite.hasNext()) {
				url = ite.next();
				if (url.startsWith(prefix)&&!prefix.equalsIgnoreCase(url)) {					
					pageId = url.substring(71, url.length() - 1);
				} else {
					System.out.println("URL:" + url + "格式不符合要求");
					continue;
				}
				// 检查对应的pageId在索引中是否出现过
				term = new Term("pageId", pageId);
				query = new TermQuery(term);
				TopDocs topDoc = searcher.search(query, 10);
				if (topDoc.totalHits==1) {
					//有命中纪录，且命中的条数为1，说明在索引中也有相应日期的数据
					//System.out.println("命中："+url);
					//需要进一步检查是否有壁纸信息以及壁纸是否被下载下来
					ScoreDoc[] docs = topDoc.scoreDocs;
					Document doc = reader.document(docs[0].doc);
					String wall = doc.get("wallPaper");
					if(wall!=null){
						//当前日期存在壁纸
						if(duplicateWallSet.contains(wall)){
							System.out.println(pageId);
						}
						if(!exists(wallDir,wall)){
							System.out.println(doc.get("pageId")+"页面的壁纸未下载");
							//图片未下载，需要放回队列中重新下载
				            //jedis.rpush(queue_key, url);
						}
					}
				}
			}
		} finally {
			pool.returnResource(jedis);
		}
	}
	
	public void checkCredit() throws IOException{
		int count=0;
		Jedis jedis = pool.getResource();
		IndexSearcher searcher = new IndexSearcher(reader);
		Query query = null;
		Term term = null;
		try {
			// 从redis中取出历史记录集合的所有数据
			Set<String> history = jedis.smembers(history_key);
			Iterator<String> ite = history.iterator();
			String url = null;
			String pageId = null;
			// 遍历历史记录集合
			while (ite.hasNext()) {
				url = ite.next();
				if (url.startsWith(prefix)&&!prefix.equalsIgnoreCase(url)) {					
					pageId = url.substring(71, url.length() - 1);
				} else {
					System.out.println("URL:" + url + "格式不符合要求");
					continue;
				}
				// 检查对应的pageId在索引中是否出现过
				term = new Term("pageId", pageId);
				query = new TermQuery(term);
				TopDocs topDoc = searcher.search(query, 10);
				if (topDoc.totalHits==1) {
					//有命中纪录，且命中的条数为1，说明在索引中也有相应日期的数据
					//System.out.println("命中："+url);
					//需要进一步检查图片是否被下载下来
					ScoreDoc[] docs = topDoc.scoreDocs;
					Document doc = reader.document(docs[0].doc);
					String credit = doc.get("credit");
					if(credit==null||"".equals(credit)){
						System.out.println((count++)+":"+doc.get("pageId"));						
					}
				}
			}
		} finally {
			pool.returnResource(jedis);
		}	
	}
	
	public void travelDoc() throws IOException{
		int docCount = reader.getDocCount("pageId");
		Document doc = null;
		Set<String> fields = new LinkedHashSet<String>();
		fields.add("pageId");
		fields.add("pubTime");
		fields.add("title");
		fields.add("description");
		for(int i=0;i<docCount;i++){
			doc = reader.document(i,fields);
			Iterator<String> ite =fields.iterator();		
			System.out.print(i+":");
			while(ite.hasNext()){
				System.out.print(doc.get(ite.next())+" ");
			}
			System.out.println();
			if(i%100==0){
				System.in.read();
			}
		}
	}
	
	
	private boolean exists(String dir,String file){
		File f = new File(dir,file);
		return f.exists();
	}
	
	/**
	 * 向Redis队列中重新添加所有任务，重新执行抓取工作
	 */
	public void resetAllJob(){
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
		IndexCheck check = new IndexCheck();
		check.resetAllJob();
		//check.checkWallpaper();
	}

}
