package com.sciul.kafka.proxies;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.json.Json;
import javax.json.JsonObject;

import com.sciul.kafka.exception.KafkaException;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ddth.kafka.IKafkaMessageListener;
import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaMessage;

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
  private boolean consumeFromBeginning = false;
  private KafkaClient kafkaClient;
  private CuratorFramework curatorFramework;
  private DistributedDelayQueue<String> delayQ;

  @SuppressWarnings("rawtypes")
  private Map<String, QueueConsumer> listenerMap;

  private final static ObjectMapper mapper = new ObjectMapper();

  public KafkaConfiguration(String zookeeperConnString, String consumerGroupId) throws Exception {
    this(zookeeperConnString, consumerGroupId, null, true);
  }

  @SuppressWarnings("rawtypes")
  public KafkaConfiguration(String zookeeperConnString, String consumerGroupId, Map<String, QueueConsumer> listenerMap)
        throws Exception {
    this(zookeeperConnString, consumerGroupId, listenerMap, true);
  }

  @SuppressWarnings("rawtypes")
  public KafkaConfiguration(String zookeeperConnString, String consumerGroupId, Map<String, QueueConsumer> listenerMap,
        boolean consumeFromBeginning) throws Exception {
    this.zookeeperConnString = zookeeperConnString;
    this.consumerGroupId = consumerGroupId;
    this.listenerMap = listenerMap;
    this.consumeFromBeginning = consumeFromBeginning;
    init();
  }

  public KafkaClient getKafkaClient() {
    return kafkaClient;
  }

  private void init() throws KafkaException {
    curatorFramework = curatorFramework();
    delayQ = distributedDelayQueue();
    kafkaClient = new KafkaClient(zookeeperConnString);
    try {
      kafkaClient.init();
      logger.debug("connected to kafka!!");
    } catch (Exception e) {
      logger.warn("unable to instantiate kafkaClient!", e);
      throw new KafkaException("unable to instantiate kafkaClient!", e);
    }
  }

  public CuratorFramework curatorFramework() {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_RETRIES);
    curatorFramework = CuratorFrameworkFactory.newClient(zookeeperConnString, retryPolicy);
    curatorFramework.start();
    return curatorFramework;
  }

  public DistributedDelayQueue<String> distributedDelayQueue() throws KafkaException {
    org.apache.curator.framework.recipes.queue.QueueConsumer<String> consumer =
          new org.apache.curator.framework.recipes.queue.QueueConsumer<String>() {
            @Override
            public void consumeMessage(String message) {
              logger.debug("message received from delayQ: {}", message);
              JsonObject jsonObject = Json.createReader(new StringReader(message)).readObject();
              String queue = jsonObject.getJsonObject(PAYLOAD).getString(QUEUE);
              String queueMessage = jsonObject.getJsonObject(PAYLOAD).getJsonObject(PAYLOAD).toString();
              kafkaClient.sendMessage(new KafkaMessage(queue, queueMessage));
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
    try {
      delayQ.start();
    } catch (Exception e) {
      logger.error("unable to start delayQ", e);
      throw new KafkaException("unable to start delayQ", e);
    }

    return delayQ;
  }

  public <K> void publish(QueueConsumer<K> queueConsumer, Map<String, String> headers, Class<K> clazz, K pojo) throws KafkaException {
    QueueItem queueItem = null;
    try {
      queueItem = QueueItem.build(mapper, headers, clazz, pojo);
    } catch (IOException e) {
      logger.error("Unable to map/ build queueItem", e);
      throw new KafkaException("Unable to map/ build queueItem", e);
    }

    String queue = queueConsumer.queue();
    logger.debug("queue: {}, message: {}", queue, queueItem);
    kafkaClient.sendMessage(new KafkaMessage(queue, queueItem.toString()));
  }

  public <K> void publish(QueueConsumer<K> queueConsumer,
        Map<String, String> headers,
        Class<K> clazz,
        K pojo,
        long timeSinceEpoch) throws KafkaException {
    QueueItem inner = null;
    try {
      inner = QueueItem.build(mapper, headers, clazz, pojo);
    } catch (IOException e) {
      logger.error("Unable to map/ build queueItem", e);
      throw new KafkaException("Unable to map/ build queueItem", e);
    }

    QueueItem outer = new QueueItem();
    outer.setClassName(this.getClass().getCanonicalName());
    outer.setTryNumber(0);

    outer.setJsonObject(Json.createObjectBuilder().add(QUEUE, queueConsumer.queue())
          .add(PAYLOAD, Json.createReader(new StringReader(inner.toString())).read()).build());

    logger.debug("queuing delayQ message: {}", outer);
    try {
      delayQ.put(outer.toString(), timeSinceEpoch);
    } catch (Exception e) {
      logger.error("Unable to set timer for delayQ", e);
      throw new KafkaException("Unable to set timer for delayQ", e);
    }
  }

  public String consumeMessage(String topic) throws InterruptedException {
    KafkaMessage msg = kafkaClient.consumeMessage(consumerGroupId, consumeFromBeginning, topic, 3, TimeUnit.SECONDS);
    if (msg != null)
      return msg.contentAsString();
    return null;
  }

  public Map<String, Object> consumeMessage(String topic, String consumerGroupId, boolean consumeFromBeginning) throws KafkaException {
    Map<String, Object> message = new HashMap<String, Object>();
    QueueItem queueItem = null;
    KafkaMessage msg = null;
    try {
      msg = kafkaClient.consumeMessage(consumerGroupId, consumeFromBeginning, topic, 3, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.error("Unable to read from message from kafka", e);
      throw new KafkaException("Unable to read from message from kafka", e);
    }

    if (msg != null) {
      queueItem = QueueItem.build(msg.contentAsString());
      message.put("headers", queueItem.getHeaders());
      try {
        message.put("payload", queueItem.payload(mapper));
      } catch (ClassNotFoundException e) {
        logger.error("Unable to map message from kafka to class", e);
        throw new KafkaException("Unable to map message from kafka to class", e);
      } catch (IOException e) {
        logger.error("Unable to convert json into class instance", e);
        throw new KafkaException("Unable to convert json into class instance", e);
      }
      return message;
    }
    return null;
  }

  public void startListeners() {
    if (listenerMap == null) {
      logger.warn("no listeners started!");
      return;
    }

    for (final Map.Entry<String, QueueConsumer> listener : listenerMap.entrySet()) {
      logger.info("starting listener for: {}", listener.getKey());

      kafkaClient.addMessageListener(consumerGroupId, consumeFromBeginning, listener.getKey(),
            new IKafkaMessageListener() {

              @SuppressWarnings("unchecked")
              @Override
              public void onMessage(KafkaMessage message) {
                MDC.put("Service", listener.getValue().getClass().getCanonicalName());

                logger.debug("*************************************");
                ASYNC_CALL.set(true);
                try {
                  logger.debug("message: {}", message.contentAsString());

                  QueueItem queueItem = QueueItem.build(message.contentAsString());

                  listener.getValue().consume(queueItem.getHeaders(), queueItem.payload(mapper));
                } catch (Exception e) {
                  logger.error("exception while processing queue: " + listener, e);
                }
                ASYNC_CALL.set(false);
                logger.debug("*************************************");
              }
            });
    }
  }
}