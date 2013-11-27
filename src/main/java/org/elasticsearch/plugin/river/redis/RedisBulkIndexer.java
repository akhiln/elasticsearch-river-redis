package org.elasticsearch.plugin.river.redis;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


class RedisBulkIndexer implements RedisIndexer {

    private static final String[] POISON = new String[]{"jekkyl", "hyde"};
    private static Logger logger = LoggerFactory.getLogger(RedisBulkIndexer.class);

    private final Client client;
    private final long timeout;
    private final int requestSize;

    private BlockingQueue<String[]> queue = new LinkedBlockingQueue<String[]>();
    private BulkRequestBuilder requestBuilder;

    public RedisBulkIndexer(Client client, long timeout, int requestSize) {
        this.client = client;
        this.requestSize = requestSize;
        this.timeout = timeout;
        this.requestBuilder = client.prepareBulk();
    }

    public void index(String channel, String message) {
        logger.debug("Queuing... [channel={}, message={}]", channel, message);
        queue.offer(new String[]{channel, message}); // pragmatically we're not bounded
        logger.debug("... {} now queued", queue.size());
    }

    public void shutdown() {
        queue.offer(POISON);
    }

    @Override
    public void run() {
        logger.debug("Starting indexer");
        while (true) {
            try {
                String[] msg = queue.poll(timeout, TimeUnit.SECONDS);
                if (msg == POISON) {
                    logger.info("Poison pill eaten - shutting down subscriber thread");
                    logger.debug("Indexer shutdown");
                    return;
                } 

                try {
                    if (msg != null) {
                    	queueMessage(msg[1]);
                    } else {
                    	processBulkRequest(true);
                    }
                } catch (Exception e) {
                    logger.warn("{}", e);
                }
            } catch (InterruptedException ignored) {
            }
        }
    }

    void queueMessage(String msg) throws Exception {
        logger.debug("Adding to request... {}", msg);
    	requestBuilder.add(msg.getBytes(), 0, msg.getBytes().length, false);

    	processBulkRequest(false);
    }
    
    void processBulkRequest(boolean force) {
    	if (force || requestBuilder.numberOfActions() >= requestSize) {
    		logger.debug("executing bulk request...");
			BulkResponse response = requestBuilder.execute().actionGet();
			if(response.hasFailures()){
				logger.error("failed to execute" + response.buildFailureMessage());
			}
			requestBuilder = client.prepareBulk();
    	}
    }
}
