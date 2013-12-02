package org.elasticsearch.plugin.river.redis;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisListSubscriber extends RedisSubscriber {
	
    private static Logger logger = LoggerFactory.getLogger(RedisListSubscriber.class);
    private boolean running = false;

	public RedisListSubscriber(JedisPool pool, String[] channels, RedisIndexer indexer) {
		super(pool, channels, indexer);
	}
	
	public void run() {
		running = true;
		super.run();
	}
	
	protected void fetchMessage(Jedis jedis) {
		while(running) {
			List<String> msg = jedis.blpop(100, getChannels());
			getIndexer().index(msg.get(0), msg.get(1));
		}
	}
	
	public void shutdown() {
		running = false;
	}

}
