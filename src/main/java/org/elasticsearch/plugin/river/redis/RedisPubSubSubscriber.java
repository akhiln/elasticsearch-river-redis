package org.elasticsearch.plugin.river.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisPubSubSubscriber extends RedisSubscriber {
	
    private static Logger logger = LoggerFactory.getLogger(RedisPubSubSubscriber.class);
    RiverPubSubListener listener;

	public RedisPubSubSubscriber(JedisPool pool, String[] channels, RedisIndexer indexer) {
		super(pool, channels, indexer);
		listener = new RiverPubSubListener(indexer);
	}
	
	protected void fetchMessage(Jedis jedis) {
		jedis.subscribe(listener, getChannels());
	}
	
	public void shutdown() {
        if (listener != null && listener.isSubscribed()) {
        	listener.unsubscribe();
        } else {
        	getIndexer().shutdown();
        }
	}

}
