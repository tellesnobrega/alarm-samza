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

import java.util.Map;
import java.util.Random;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumptionProducer  implements StreamTask, InitableTask {
	private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "consumptions");
	private static int MAX_KEY = 10;
	private static int MAX_VALUE = 100;
	private static Random random = new Random();
	private static final Logger log = LoggerFactory.getLogger(ConsumptionProducer.class);

	@Override
	public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
		int key = random.nextInt(MAX_KEY);
		int value = random.nextInt(MAX_VALUE);
		
		Event event = new Event(Type.CONSUMPTION, key, value);
		Map<String, Object> outgoingMap = Event.toMap(event);
		collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, outgoingMap));
	}

	@Override
	public void init(Config arg0, TaskContext arg1) throws Exception {
		// TODO Auto-generated method stub
		
	}

}
