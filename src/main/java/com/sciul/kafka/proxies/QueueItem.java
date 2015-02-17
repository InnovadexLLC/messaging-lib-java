package com.sciul.kafka.proxies;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import javax.json.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @author GauravChawla
 */
public class QueueItem {
  private static final String PAYLOAD = "Payload";
	private static final String TRY_NUMBER = "TryNumber";
  public static final String JAVA_CLASS = "JavaClass";
  public static final String HEADERS = "Headers";

  private Integer tryNumber = 0;
  private String className;
  private JsonObject jsonObject;
  private Map<String, String> headers;

  public Map<String, String> getHeaders() {
    return headers;
  }

  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public Integer getTryNumber() {
		return tryNumber;
	}

	public void setTryNumber(Integer tryNumber) {
		this.tryNumber = tryNumber;
	}

	public JsonObject getJsonObject() {
		return jsonObject;
	}

	public void setJsonObject(JsonObject jsonObject) {
		this.jsonObject = jsonObject;
	}

  /**
   * used for serializing to json string
   * @return <code>String</code>
   */
	public String toString() {
		return Json.createObjectBuilder()
				.add(PAYLOAD, jsonObject)
				.add(TRY_NUMBER, tryNumber)
        .add(JAVA_CLASS, className)
        .add(HEADERS, convert(headers))
				.build()
				.toString();
	}

  public Object payload(ObjectMapper mapper) throws ClassNotFoundException, IOException {
    Class clazz = Class.forName(className);
    JavaType javaType = TypeFactory.defaultInstance().constructType(clazz);
    return mapper.readValue(jsonObject.toString(), javaType);
  }

  /**
   * from a json object
   * @param jsonObject
   * @return <code>QueueItem</code>
   */
  private static QueueItem build(JsonObject jsonObject) {
    QueueItem queueItem1 = new QueueItem();

    queueItem1.tryNumber = jsonObject.getInt(TRY_NUMBER);
    queueItem1.jsonObject = jsonObject.getJsonObject(PAYLOAD);
    queueItem1.className = jsonObject.getString(JAVA_CLASS);

    JsonObject headers = jsonObject.getJsonObject(HEADERS);
    queueItem1.headers = new HashMap<String, String>();
    for (Map.Entry<String, JsonValue> entry : headers.entrySet()) {
      queueItem1.headers.put(entry.getKey(), entry.getValue().toString());
    }

    return queueItem1;
  }

  /**
   * from json string
   * @param jsonString json encoded string
   * @return <code>QueueItem</code>
   */
	public static QueueItem build(String jsonString) {
		return build(Json.createReader(new StringReader(jsonString)).readObject());
	}

  /**
   * from a pojo
   * @param objectMapper jackson used by spring
   * @param headers security headers like sessionId, authToken
   * @param clazz pojo class
   * @param pojo pojo object
   * @param <T> pojo type
   * @return <code>QueueItem</code>
   * @throws IOException from jackson library
   */
	public static <T> QueueItem build(ObjectMapper objectMapper,
                                    Map<String, String> headers,
                                    Class<T> clazz, T pojo) throws IOException {
    return build(Json
        .createObjectBuilder()
        .add(PAYLOAD, Json
            .createReader(new StringReader(objectMapper.writeValueAsString(pojo)))
            .readObject())
        .add(TRY_NUMBER, 0)
        .add(JAVA_CLASS, clazz.getCanonicalName())
        .add(HEADERS, convert(headers))
        .build());
	}

  private static JsonObject convert(Map<String, String> headers) {
    JsonObjectBuilder jHeaderBuilder = Json.createObjectBuilder();
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      jHeaderBuilder.add(entry.getKey(), entry.getValue());
    }
    return jHeaderBuilder.build();
  }
}