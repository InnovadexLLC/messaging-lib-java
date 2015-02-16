package com.sciul.kafka.proxies;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.queue.DistributedDelayQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.github.ddth.kafka.IKafkaMessageListener;
import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

import javax.json.Json;

/**
 * @author GauravChawla
 */
public class KafkaConfiguration {

	private static Logger logger = LoggerFactory.getLogger(KafkaConfiguration.class);

	private static final int BASE_SLEEP_TIME_MS = 1000;
	private static final int MAX_RETRIES = 3;
	private static final String PAYLOAD = "Payload";
	private static final String QUEUE = "Queue";
	private static final String DELAY_Q_PATH = "/delayQ";

	private static final ThreadLocal<Boolean> ASYNC_CALL = new ThreadLocal<Boolean>() {
		protected Boolean initialValue() {
			return false;
		}
	};

	private String zookeeperConnString;

	private String consumerGroupId;

	private boolean kafkaTurnedOff;

	private boolean consumeFromBegining = false;

	private KafkaClient kafkaClient;

	private CuratorFramework curatorFramework;

	private DistributedDelayQueue<String> delayQ;

	@SuppressWarnings("rawtypes")
	private Map<String, QueueConsumer> listenerMap;

	private static ObjectMapper mapper = new ObjectMapper();

	public KafkaConfiguration() {
	}

	public KafkaConfiguration(Boolean kafkaTurnedOff) {
		this.kafkaTurnedOff = kafkaTurnedOff;
		init();
	}

	public KafkaConfiguration(String zookeeperConnString, String consumerGroupId) {
		this.zookeeperConnString = zookeeperConnString;
		this.consumerGroupId = consumerGroupId;
		init();
	}

	@SuppressWarnings("rawtypes")
	public KafkaConfiguration(String zookeeperConnString, String consumerGroupId, Map<String, QueueConsumer> listenerMap) {
		this.zookeeperConnString = zookeeperConnString;
		this.consumerGroupId = consumerGroupId;
		this.listenerMap = listenerMap;
		init();
	}

	@SuppressWarnings("rawtypes")
	public KafkaConfiguration(String zookeeperConnString, String consumerGroupId,
			Map<String, QueueConsumer> listenerMap, boolean consumeFromBegining) {
		this.zookeeperConnString = zookeeperConnString;
		this.consumerGroupId = consumerGroupId;
		this.listenerMap = listenerMap;
		this.consumeFromBegining = consumeFromBegining;
		init();
	}

	private void init() {
		curatorFramework = curatorFramework();
		delayQ = distributedDelayQueue();
		kafkaClient = kafkaClient();
		// startListeners();
	}

	public CuratorFramework curatorFramework() {
		if (kafkaTurnedOff) {
			curatorFramework = null;
			return curatorFramework;
		}

		RetryPolicy retryPolicy = new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_RETRIES);
		curatorFramework = CuratorFrameworkFactory.newClient(zookeeperConnString, retryPolicy);
		curatorFramework.start();
		return curatorFramework;
	}

	public DistributedDelayQueue<String> distributedDelayQueue() {
		if (kafkaTurnedOff) {
			return null;
		}

		org.apache.curator.framework.recipes.queue.QueueConsumer<String> consumer = new org.apache.curator.framework.recipes.queue.QueueConsumer<String>() {
			@Override
			public void consumeMessage(String message) throws Exception {
				kafkaClient.sendMessage(new KafkaMessage(QUEUE.toString(), message));
			}

			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				if (newState.equals(ConnectionState.SUSPENDED)) {
					logger.error("screwed");
				}
			}
		};

		QueueSerializer<String> serializer = new QueueSerializer<String>() {
			@Override
			public byte[] serialize(String item) {
				return item.getBytes(Charset.defaultCharset());
			}

			@Override
			public String deserialize(byte[] bytes) {
				return new String(bytes, Charset.defaultCharset());
			}
		};

		QueueBuilder<String> builder = QueueBuilder.builder(curatorFramework, consumer, serializer, DELAY_Q_PATH);

		delayQ = builder.buildDelayQueue();

		return delayQ;
	}

	public KafkaClient kafkaClient() {
		if (kafkaTurnedOff) {
			logger.debug("kafka is turned off");
			return new KafkaClient();
		}

		kafkaClient = new KafkaClient(zookeeperConnString);
		try {
			kafkaClient.init();
			logger.debug("connected to kafka!!");
		} catch (Exception e) {
			logger.warn("unable to instantiate kafkaClient!");
		}

		return kafkaClient;
	}

	public KafkaClient client() {
		return kafkaClient;
	}

	public String startListeners() {
		if (kafkaTurnedOff) {
			return "Kafka not started";
		}

		Set<String> queues = listenerMap.keySet();

		for (final String queue : queues) {
			kafkaClient.addMessageListener(consumerGroupId, consumeFromBegining, queue, new IKafkaMessageListener() {
				@Override
				public void onMessage(KafkaMessage message) {
					MDC.put("Service", queue);

					logger.debug("*************************************");
					ASYNC_CALL.set(true);
					try {
						logger.debug("message: {}", message.contentAsString());
						listenerMap.get(queue).consume(message.contentAsString());
					} catch (Exception e) {
						logger.error("unknown exception while processing queue: " + queue, e);
					}
					ASYNC_CALL.set(false);
					logger.debug("*************************************");
				}
			});
		}

		return "Started";
	}

	public <T, K> void publish(QueueConsumer<T> queueConsumer, K pojo) throws Exception {
		if (kafkaTurnedOff) {
			return;
		}
		String queue = queueConsumer.queue();
		logger.debug("queue: {}, message: {}", queue, mapper.writeValueAsString(pojo));
		kafkaClient.sendMessage(new KafkaMessage(queue, mapper.writeValueAsString(pojo)));
	}

	public <T, K> void publish(QueueConsumer<T> queueConsumer, K pojo, long timeSinceEpoch) throws Exception {
		if (kafkaTurnedOff) {
			return;
		}

		QueueItem queueItem = QueueItem.build(mapper, pojo);

		queueItem.setJsonObject(Json.createObjectBuilder().add(QUEUE, queueConsumer.queue())
				.add(PAYLOAD, queueItem.getJsonObject()).build());

		delayQ.put(queueItem.toString(), timeSinceEpoch);
	}
}