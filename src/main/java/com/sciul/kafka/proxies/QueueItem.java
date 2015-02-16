package com.sciul.kafka.proxies;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.json.Json;
import javax.json.JsonObject;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * @author GauravChawla
 */
public class QueueItem {
	private static final String PAYLOAD = "Payload";
	private static final String TRY_NUMBER = "TryNumber";

	private Integer tryNumber = 0;
	private String auth;
	private String session;
	private JsonObject jsonObject;

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

	public String getAuth() {
		return auth;
	}

	public void setAuth(String auth) {
		this.auth = auth;
	}

	public String getSession() {
		return session;
	}

	public void setSession(String session) {
		this.session = session;
	}

	public String toString() {
		return Json.createObjectBuilder()
				.add(PAYLOAD, jsonObject)
				.add(TRY_NUMBER, tryNumber)
				.build()
				.toString();
	}

	public static QueueItem build(String jsonString) {
		JsonObject object = Json.createReader(new StringReader(jsonString)).readObject();
		QueueItem queueItem1 = new QueueItem();
		queueItem1.tryNumber = object.getInt(TRY_NUMBER);
		queueItem1.jsonObject = object.getJsonObject(PAYLOAD);
		return queueItem1;
	}

	public static <T> QueueItem build(ObjectMapper objectMapper, T object) throws IOException {
		StringWriter stringWriter = new StringWriter();
		QueueItem queueItem = new QueueItem();
		objectMapper.writeValue(stringWriter, object);
		String json = stringWriter.toString();


		queueItem.setJsonObject(
				Json.createReader(new StringReader(json))
				.readObject());
		return queueItem;
	}
}