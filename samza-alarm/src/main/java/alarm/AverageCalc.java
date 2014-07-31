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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AverageCalc implements StreamTask {
	private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka",
			"averages");
	private static final int MAX_SIZE = 10;
	private Map<Integer, List<Integer>> allMesurements = new HashMap<Integer, List<Integer>>();
	private static final Logger log = LoggerFactory.getLogger(AverageCalc.class);

	@Override
	public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
		Map<String, Object> jsonObject = (Map<String, Object>) envelope.getMessage();
		Event event = new Event(jsonObject);

		addMeasurement(event.getKey(), event.getValue());
		int average = calcAverage(event.getKey());

		Event averageEvent = new Event(Type.AVERAGE, event.getKey(), average);
		Map<String, Object> outgoingMap = Event.toMap(averageEvent);

		collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, outgoingMap));
	}

	private void addMeasurement(int key, int value) {
		if (!allMesurements.containsKey(key)) {
			allMesurements.put(key, new ArrayList<Integer>());
		}
		List<Integer> measurements = allMesurements.get(key);
		if (measurements.size() == MAX_SIZE) {
			measurements.remove(0);
		}
		measurements.add(value);
	}

	private int calcAverage(int key) {
		List<Integer> measurements = allMesurements.get(key);
		int average = 0;
		for (int measurement : measurements) {
			average += measurement;
		}
		return average / measurements.size();
	}

}
