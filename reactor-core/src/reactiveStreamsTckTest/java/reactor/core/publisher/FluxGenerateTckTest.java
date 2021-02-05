/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.logging.Level;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

/**
 * @author Stephane Maldini
 */
@Test
public class FluxGenerateTckTest extends PublisherVerification<Long> {

	public FluxGenerateTckTest() {
		super(new TestEnvironment(500), 1000);
	}

	@Override
	public Publisher<Long> createPublisher(long elements) {
		return Flux.<Long, Long>generate(() -> 0L, (cursor, s) -> {
			if(cursor < elements && cursor < elements) {
				s.next(cursor);
			}
			else if(cursor == elements){
				s.complete();
			}

			return cursor + 1;
		})

				.map(data -> data * 10)
				.map( data -> data / 10)
				.log("log-test", Level.FINE);
	}

	@Override
	public Publisher<Long> createFailedPublisher() {
		return Flux.error(new Exception("test"));
	}
}