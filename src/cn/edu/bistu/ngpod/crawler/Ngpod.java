/**
 * 
 */
package cn.edu.bistu.ngpod.crawler;

import java.io.Serializable;

/**
 * 对NGPOD页面的封装
 * @author hadoop
 *
 */
public class Ngpod implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -737945018045969503L;
	private String title;
	private String pageId;
	private String pubTime;
	private String ptInt;
	private String desc;
	private String photo;
	private String credit;
	private String wallpaper;
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getPageId() {
		return pageId;
	}
	public void setPageId(String pageId) {
		this.pageId = pageId;
	}
	public String getPubTime() {
		return pubTime;
	}
	public void setPubTime(String pubTime) {
		this.pubTime = pubTime;
	}
	public String getPtInt() {
		return ptInt;
	}
	public void setPtInt(String ptInt) {
		this.ptInt = ptInt;
	}
	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
	}
	public String getPhoto() {
		return photo;
	}
	public void setPhoto(String photo) {
		this.photo = photo;
	}
	public String getCredit() {
		return credit;
	}
	public void setCredit(String credit) {
		this.credit = credit;
	}
	public String getWallpaper() {
		return wallpaper;
	}
	public void setWallpaper(String wallpaper) {
		this.wallpaper = wallpaper;
	}
	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append("TITLE:"+title+"."+'\n');
		buf.append("PAGEID:"+pageId+"."+'\n');
		buf.append("PHOTO:"+photo+"."+'\n');
		buf.append("CREDIT:"+credit+"."+'\n');
		buf.append("PUBTIME:"+pubTime+"."+'\n');
		buf.append("PT_INT:"+ptInt+"."+'\n');
		buf.append("WP:"+wallpaper+"."+'\n');
		buf.append("DESC:"+desc+"."+'\n');
		return buf.toString();
	}
	
}
