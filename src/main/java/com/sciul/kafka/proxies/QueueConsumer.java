package com.sciul.kafka.proxies;

/**
 * @author GauravChawla
 */
public interface QueueConsumer<T> {
	<K> void consume(String message);
	String queue();
}