package cn.edu.bistu.ngpod.crawler;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

public class NgpodPageProcessor implements PageProcessor {

	private String startPage = "";

	private String photoDir = "";

	private String wallpaperDir = "";
	/**
	 * HBase的表名
	 */
	public static final byte[] HBASE_TABLENAME = Bytes.toBytes("ngpod");

	/**
	 * HBase中保存抓取出来的页面数据项的列族名
	 */
	public static final byte[] PAGE_CFNAME = Bytes.toBytes("p");
	
	/**
	 * HBase中保存页面原始内容及部分附加数据项的CF
	 */
	public static final byte[] RAW_CFNAME = Bytes.toBytes("r");
	
	public static final byte[] COL_TITLE = Bytes.toBytes("title");
	
	public static final byte[] COL_CREDIT = Bytes.toBytes("credit");

	public static final byte[] COL_PUBTIME = Bytes.toBytes("pubTime");

	public static final byte[] COL_PTINT = Bytes.toBytes("ptInt");
	
	public static final byte[] COL_PAGEID = Bytes.toBytes("pageId");

	public static final byte[] COL_PHOTO = Bytes.toBytes("photo");

	public static final byte[] COL_WP = Bytes.toBytes("wp");

	public static final byte[] COL_DESC = Bytes.toBytes("desc");

	public static final byte[] COL_RAW = Bytes.toBytes("raw");

	public static final byte[] COL_WPURL = Bytes.toBytes("wpUrl");

	public static final byte[] COL_PICURL = Bytes.toBytes("picUrl");

	
	private static final Logger log = Logger
			.getLogger(NgpodPageProcessor.class);

	// 抓取网站的相关配置，包括编码、抓取间隔、重试次数等
	private Site site = Site.me().setCycleRetryTimes(20).setSleepTime(2000)
			.setTimeOut(20000);

	public Site getSite() {
		return site;
	}

	private HttpHelper http = null;

	public NgpodPageProcessor() {
		http = new HttpHelper();
	}

	public void process(Page page) {
		// 定制爬虫逻辑的核心方法，在这里编写抽取逻辑
		if (page.getRequest().getUrl().equalsIgnoreCase(startPage)) {
			for (String url : page.getHtml()
					.xpath("//div[@class='nav']/p/a/@href").all()) {
				page.addTargetRequest(url);
			}
			page.addTargetRequest(page.getHtml()
					.xpath("//link[@rel=\"canonical\"]/@href").toString());
			page.setSkip(true);
			return;
		}

		// 部分二：定义如何抽取页面信息，并保存下来
		// 图片标题
		String title = page.getHtml().xpath("//div[@id='caption']/h2/text()")
				.toString();
		page.putField("title", new HBaseCell(PAGE_CFNAME, COL_TITLE, Bytes.toBytes(title)));
		// 作者信息
		String credit = page.getHtml()
				.xpath("//div[@id='caption']/p[@class='credit']/allText()")
				.toString();
		page.putField("credit", new HBaseCell(PAGE_CFNAME, COL_CREDIT, Bytes.toBytes(credit)));
		// 发布日期
		String pubTime = page
				.getHtml()
				.xpath("//div[@id='caption']/p[@class='publication_time']/text()")
				.toString();
		page.putField("pubTime", new HBaseCell(PAGE_CFNAME, COL_PUBTIME, Bytes.toBytes(pubTime)));

		// 以整数形式表示的日期
		int pubTimeInt = 0;
		try {
			pubTimeInt = DateFormatter.format2Num(pubTime);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("将发布日期：" + pubTime + "解析为整数失败！");
			page.setSkip(true);
			return;
		}	
		page.putField("ptInt", new HBaseCell(PAGE_CFNAME, COL_PTINT, Bytes.toBytes(String.valueOf(pubTimeInt))));

		// 页面ID
		String pageId = page
				.getUrl()
				.regex("http://photography.nationalgeographic.com/photography/photo-of-the-day/(.+)/")
				.toString();
		page.putField("pageId", new HBaseCell(PAGE_CFNAME, COL_PAGEID, Bytes.toBytes(pageId)));

		log.info("抓取页面：" + pageId);
		// 图片URL
		String photoURL = page
				.getHtml()
				.xpath("//div[@id=\"content_top\"]/div[@class=\"primary_photo\"]//img/@src")
				.toString();
		String fileName = photoURL.substring(photoURL.lastIndexOf('/') + 1);
		downloadFile(photoDir, fileName, "http:" + photoURL, 4);
		page.putField("photo", new HBaseCell(PAGE_CFNAME, COL_PHOTO, Bytes.toBytes(fileName)));
		page.putField("picUrl", new HBaseCell(RAW_CFNAME, COL_PICURL, Bytes.toBytes(photoURL)));


		// //*[@id="content_mainA"]/div[1]/div/div[1]/div[2]
		// 部分页面存在壁纸下载，XPath路径为：//div[@id="content_mainA"]//div[@class="download_link"]/a/@href
		String wallpaperURL = page
				.getHtml()
				.xpath("//div[@id=\"content_mainA\"]//div[@class=\"download_link\"]/a/@href")
				.toString();
		if (wallpaperURL != null) {
			String wallpaperFile = wallpaperURL.substring(wallpaperURL
					.lastIndexOf('/') + 1);
			log.debug("存在壁纸链接：" + wallpaperURL);
			downloadFile(wallpaperDir, wallpaperFile, wallpaperURL, 4);
			page.putField("wp", new HBaseCell(PAGE_CFNAME, COL_WP, Bytes.toBytes(wallpaperFile)));
			page.putField("wpUrl", new HBaseCell(RAW_CFNAME, COL_WPURL, Bytes.toBytes(wallpaperURL)));

		}
		List<String> descriptions = page.getHtml()
				.xpath("//div[@id=\"caption\"]/p/allText()").all();
		if (descriptions.get(2).contains("This Month in Photo of the Day")) {
			page.putField("desc", new HBaseCell(PAGE_CFNAME, COL_DESC, Bytes.toBytes(descriptions.get(3))));
		} else {
			page.putField("desc", new HBaseCell(PAGE_CFNAME, COL_DESC, Bytes.toBytes(descriptions.get(2))));
		}
		//将页面的原始内容放入RAW CF
		page.putField("raw", new HBaseCell(RAW_CFNAME, COL_RAW, Bytes.toBytes(page.getHtml().toString())));

		// 部分三：从页面发现后续的url地址来抓取
		List<String> urls = page.getHtml()
				.xpath("//div[@class='nav']/p/a/@href").all();
		for (String url : urls) {
			log.info("添加待抓取页面地址：" + url);
			page.addTargetRequest(url);
		}
	}

	private boolean downloadFile(String dir, String fileName, String url,
			int maxRetry) {
		if (exists(dir, fileName)) {
			// 文件已经存在，不要重复下载
			log.debug("文件存在");
			return true;
		}
		int retry = 0;
		while (retry < maxRetry) {
			if (http.getFile(url, dir, fileName)) {
				return true;
			} else {
				retry++;
				log.error("图片" + url + "下载失败" + ",重试第" + retry + "次！");
			}
		}
		log.error("图片" + url + "下载失败");
		return false;
	}

	private boolean exists(String dir, String file) {
		File f = new File(dir, file);
		return f.exists();
	}

	public String getStartPage() {
		return startPage;
	}

	public void setStartPage(String startPage) {
		this.startPage = startPage;
	}

	public String getPhotoDir() {
		return photoDir;
	}

	public void setPhotoDir(String photoDir) {
		this.photoDir = photoDir;
	}

	public String getWallpaperDir() {
		return wallpaperDir;
	}

	public void setWallpaperDir(String wallpaperDir) {
		this.wallpaperDir = wallpaperDir;
	}

	public static boolean isEmpty(String s) {
		if (s == null || "".equals(s.trim())) {
			return true;
		} else {
			return false;
		}
	}

	public static void main(String[] args) throws IOException {
		NgpodPageProcessor ngpod = new NgpodPageProcessor();
		// 读取相关配置
		Configuration config;
		String photoDir = null;
		String wallpaperDir = null;
		String startPage = null;
		try {
			config = new PropertiesConfiguration("conf.properties");
			/**
			 * 配置PHOTO目录
			 */
			if (config.containsKey("PHOTO_DIR")) {
				photoDir = config.getString("PHOTO_DIR");
				ngpod.setPhotoDir(photoDir);
			} else {
				log.error("配置文件中没有PHOTO_DIR这一配置项");
				System.exit(1);
			}

			/**
			 * 配置WALLPAPER目录
			 */
			if (config.containsKey("WALLPAPER_DIR")) {
				wallpaperDir = config.getString("WALLPAPER_DIR");
				ngpod.setWallpaperDir(wallpaperDir);
			} else {
				log.error("配置文件中没有WALLPAPER_DIR这一配置项");
				System.exit(1);
			}

			/**
			 * 配置START_PAGE
			 */
			if (config.containsKey("START_PAGE")) {
				startPage = config.getString("START_PAGE");
				ngpod.setStartPage(startPage);
			} else {
				log.error("配置文件中没有START_PAGE这一配置项");
				System.exit(1);
			}

		} catch (ConfigurationException e) {
			e.printStackTrace();
			log.error("读取配置文件失败，请检查在CLASSPATH中是否有conf.properties文件");
			System.exit(1);
		}
		if (isEmpty(startPage) || isEmpty(wallpaperDir) || isEmpty(photoDir)) {
			log.error("conf.properties配置文件中WALLPAPER_DIR、START_PAGE、PHOTO_DIR不可以为空");
		}
		ArrayList<String> whiteList = new ArrayList<String>();
		whiteList.add(startPage);
		Spider spider = Spider.create(ngpod)
		// 设置起始URL
		.addUrl(startPage).addPipeline(new HBasePipeline(HBASE_TABLENAME, "ptInt"))
		.setScheduler(new RedisScheduler("127.0.0.1",whiteList));
		spider.start();
	}
}