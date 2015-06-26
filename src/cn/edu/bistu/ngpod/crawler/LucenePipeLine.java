/**
 * 
 */
package cn.edu.bistu.ngpod.crawler;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
//import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * 将抓取的内容持久化到Lucene索引中的PipeLine实现
 * 
 * @author chenruoyu
 *
 */
public class LucenePipeLine implements Pipeline {
	private static final Logger log = Logger.getLogger(LucenePipeLine.class);
	private IndexWriter writer = null;
	private DateFormatter format = new DateFormatter();
	private SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
	public static final Long dayMillis=86400000L;
	private FieldType desc = null;	
	private static long counter = 0;
	
	public LucenePipeLine(String indexDir, String startPage) throws IOException {
		StandardAnalyzer analyzer = new StandardAnalyzer();
		File dir = new File(indexDir);
		Directory index = FSDirectory.open(dir.toPath());
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		writer = new IndexWriter(index, config);
		if(desc==null){
			desc = new FieldType();
			desc.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
			desc.setStoreTermVectors(true);
			desc.setStored(true);
			desc.setTokenized(true);
		}
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
			//首页不写入索引
			return;
		}
		//写入索引前，先检查当前页面是否已经索引过了
		String pageId = results.get("pageId").toString();
		Document doc = new Document();
		doc.add(new TextField("title", results.get("title").toString(),
				Store.YES));
		doc.add(new TextField("credit", results.get("credit").toString(),
				Store.YES));
		//处理时间
		String pubTime = results.get("pubTime").toString();
		if(pubTime==null||"".equals(pubTime)){
			log.error("发布日期字段为空");
		}
		long date = 0;
		try {
			pubTime=format.format(pubTime);
			//date = fmt.parse(pubTime).getTime()/dayMillis;
			date = fmt.parse(pubTime).getTime();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		//doc.add(new IntField("pubTime", (int)date, Store.YES));
		doc.add(new LongField("pubTime", date, Store.YES));
		doc.add(new StringField("pageId", pageId, Store.YES));
		doc.add(new StoredField("photo", results.get("photo").toString()));
		if(results.get("wallPaper")!=null){
			//存在壁纸，将壁纸文件名保存起来
			doc.add(new StoredField("wallPaper", results.get("wallPaper").toString()));
			//使用一个StringField作为是否存在壁纸的标识符
			doc.add(new StringField("hasWallPaper","TURE",Store.NO));
		}
				
		@SuppressWarnings("unchecked")
		List<String> descriptions = (List<String>) results.get("descriptions");
		if(descriptions.get(2).contains("This Month in Photo of the Day")){
			doc.add(new Field("description", pageId.replace('-', ' ')+" "+descriptions.get(3), desc));
			log.debug("Description:"+descriptions.get(3));
		}else{
			doc.add(new Field("description", pageId.replace('-', ' ')+" "+descriptions.get(2), desc));
			log.debug("Description:"+descriptions.get(2));
		}

		try {
			//writer.addDocument(doc);
			writer.updateDocument(new Term("pageId",pageId), doc);
			counter++;
			log.debug("["+counter+"]将页面ID为"+pageId+"的页面写入索引");
			writer.commit();
		} catch (IOException e) {
			log.error("["+counter+"]将页面ID为"+results.get("pageId").toString()+"的页面写入索引时失败");
			e.printStackTrace();
		}
	}
	
	protected IndexWriter getWriter(){
		return this.writer;
	}
	
}
