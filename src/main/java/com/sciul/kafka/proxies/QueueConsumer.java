package com.sciul.kafka.proxies;

import java.util.Map;

/**
 * @author GauravChawla
 */
public interface QueueConsumer<T> {
	void consume(Map<String, String> headers, T pojo);
	String queue();
}