/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package alarm;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.codehaus.jackson.map.ObjectMapper;

public class Event implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final ObjectMapper jsonMapper = new ObjectMapper();
	private Type type;
	private int key;
	private int value;
	
	public Event(Type type, int key, int value) {
		this.type = type;
		this.key = key;
		this.value = value;
	}
	
	public Event(Map<String, Object> jsonObject) {
		this(Type.valueOf(String.valueOf(jsonObject.get("id"))), (Integer) jsonObject.get("key"), (Integer) jsonObject.get("value"));
	}

	public Type getType() {
		return type;
	}

	public int getKey() {
		return key;
	}
	
	public int getValue() {
		return value;
	}

	public byte[] getBytes() {
		byte[] yourBytes = {};
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutput out = new ObjectOutputStream(bos);
			out.writeObject(this);
			yourBytes = bos.toByteArray();
			out.close();
			bos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return yourBytes;
	}
	

	public static Map<String, Object> toMap(Event event) {
		Map<String, Object> jsonObject = new HashMap<String, Object>();

		jsonObject.put("id", event.getType());
		jsonObject.put("key", event.getKey());
		jsonObject.put("value", event.getValue());

		return jsonObject;
	}

	public static String toJson(Event event) {
		Map<String, Object> jsonObject = toMap(event);

		try {
			return jsonMapper.writeValueAsString(jsonObject);
		} catch (Exception e) {
			throw new SamzaException(e);
		}
	}
}
