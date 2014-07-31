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

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Alarm implements StreamTask {
	private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "alarms");
	private Map<Integer, Integer> averages = new HashMap<Integer, Integer>();
	private static final Logger log = LoggerFactory.getLogger(Alarm.class);

	@Override
	public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
		Map<String, Object> jsonObject = (Map<String, Object>) envelope.getMessage();
		Event event = new Event(jsonObject);
		int key = event.getKey();
		int value = event.getValue();
		if (event.getType() == Type.AVERAGE) {
			averages.put(key, value);
		} else {
			if(!averages.containsKey(key)) {
				averages.put(key, value);
			}
			if (sendAlarm(key, value)) {
				collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, "Alarm! " + event.getValue()));
			}
		}
	}

	private boolean sendAlarm(int key, int measurement) {
		int average = averages.get(key);
		return measurement > average + average*0.1;
	}
}
