package cn.edu.bistu.ngpod.crawler;

import java.util.HashSet;
import java.util.List;

import com.alibaba.fastjson.JSON;

import org.apache.commons.codec.digest.DigestUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.scheduler.DuplicateRemovedScheduler;
import us.codecraft.webmagic.scheduler.MonitorableScheduler;
import us.codecraft.webmagic.scheduler.component.DuplicateRemover;

/**
 * Use Redis as url scheduler for distributed crawlers.<br>
 *
 * @author code4crafter@gmail.com <br>
 * @since 0.2.0
 */
public class RedisScheduler extends DuplicateRemovedScheduler implements
		MonitorableScheduler, DuplicateRemover {

	private JedisPool pool;

	private static final String QUEUE_PREFIX = "queue_";

	private static final String SET_PREFIX = "set_";

	private static final String ITEM_PREFIX = "item_";

	private HashSet<String> whiteList = null;

	/**
	 * RedisScheduler构造函数，在原始构造函数基础上， 增加了一个白名单，调度器启动时，应该从已抓取的URL中，将白名单删除，
	 * 也就是说，白名单中的页面，在每次爬虫启动时，都可以重复抓取
	 * 
	 * @param host
	 * @param whiteList
	 */
	public RedisScheduler(String host, List<String> whiteList) {
		this(new JedisPool(host));
		this.whiteList = new HashSet<String>(whiteList);
	}

	public RedisScheduler(JedisPool pool) {
		this.pool = pool;
		setDuplicateRemover(this);
	}

	@Override
	public void resetDuplicateCheck(Task task) {
		Jedis jedis = pool.getResource();
		try {
			// 已抓取的URL存放在以set_*为名的集合数据中，如set_photography.nationalgeographic.com
			jedis.del(getSetKey(task));
		} finally {
			pool.returnResource(jedis);
		}
	}

	@Override
	public boolean isDuplicate(Request request, Task task) {
		// 判断某个请求是否已经抓取过了
		if (whiteList.contains(request.getUrl())) {
			return false;
		}
		Jedis jedis = pool.getResource();
		try {
			// 已抓取列表的key为set_*，是一个集合类型（set），使用sismember判断URL是否已经抓取过
			boolean isDuplicate = jedis.sismember(getSetKey(task),
					request.getUrl());
			if (!isDuplicate) {
				// 未抓取过的页面，将URL加入已抓取集合中去
				jedis.sadd(getSetKey(task), request.getUrl());
			}
			return isDuplicate;
		} finally {
			pool.returnResource(jedis);
		}
	}

	@Override
	protected void pushWhenNoDuplicate(Request request, Task task) {
		Jedis jedis = pool.getResource();
		try {
			// 待抓取URL被建模为一个队列，key为queue_*，将当前URL插入这一队列的末尾
			jedis.rpush(getQueueKey(task), request.getUrl());
			if (request.getExtras() != null) {
				String field = DigestUtils.shaHex(request.getUrl());
				String value = JSON.toJSONString(request);
				// 当抓取任务存在额外信息（Extras）时，可以将他们保存在以item_*为主key，以任务URL哈希值为副key的哈希表中
				jedis.hset((ITEM_PREFIX + task.getUUID()), field, value);
			}
		} finally {
			pool.returnResource(jedis);
		}
	}

	@Override
	public synchronized Request poll(Task task) {
		Jedis jedis = pool.getResource();
		try {
			// 从待抓取队列中取出队头的数据来进行抓取
			String url = jedis.lpop(getQueueKey(task));
			if (url == null) {
				return null;
			}
			String key = ITEM_PREFIX + task.getUUID();
			String field = DigestUtils.shaHex(url);
			byte[] bytes = jedis.hget(key.getBytes(), field.getBytes());
			if (bytes != null) {
				Request o = JSON.parseObject(new String(bytes), Request.class);
				return o;
			}
			Request request = new Request(url);
			return request;
		} finally {
			pool.returnResource(jedis);
		}
	}

	protected String getSetKey(Task task) {
		return SET_PREFIX + task.getUUID();
	}

	protected String getQueueKey(Task task) {
		return QUEUE_PREFIX + task.getUUID();
	}

	@Override
	public int getLeftRequestsCount(Task task) {
		Jedis jedis = pool.getResource();
		try {
			Long size = jedis.llen(getQueueKey(task));
			return size.intValue();
		} finally {
			pool.returnResource(jedis);
		}
	}

	@Override
	public int getTotalRequestsCount(Task task) {
		Jedis jedis = pool.getResource();
		try {
			Long size = jedis.scard(getQueueKey(task));
			return size.intValue();
		} finally {
			pool.returnResource(jedis);
		}
	}
}
