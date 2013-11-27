package org.elasticsearch.plugin.river.redis;


public interface RedisIndexer extends Runnable{

	public void index(String channel, String message);

	public void shutdown();
	
}
