package org.elasticsearch.plugin.river.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @author Stephen Samuel
 */
abstract class RedisSubscriber implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(RedisSubscriber.class);

    private final RedisIndexer indexer;
    private final JedisPool pool;
    private final String[] channels;

    public RedisSubscriber(JedisPool pool, String[] channels, RedisIndexer indexer) {
        this.pool = pool;
        this.indexer = indexer;
        this.channels = channels;
    }

	public void run() {
		try {
			Jedis jedis = getPool().getResource();
			logger.debug("Subscribing to channels [{}]", getChannels());
			fetchMessage(jedis);
			logger.debug("Subscribe completed; closing down");
			getPool().returnResource(jedis);
		} catch (Exception e) {
			logger.warn("Error running subscriber task {}", e);
		}
	}
	
	protected abstract void fetchMessage(Jedis jedis);

	public RedisIndexer getIndexer() {
		return indexer;
	}

	public JedisPool getPool() {
		return pool;
	}

	public String[] getChannels() {
		return channels;
	}
    
}
