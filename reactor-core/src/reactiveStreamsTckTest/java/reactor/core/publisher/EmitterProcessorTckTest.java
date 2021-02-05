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

import org.reactivestreams.Processor;
import org.testng.SkipException;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class EmitterProcessorTckTest extends AbstractProcessorVerification {

	@Override
	public Processor<Long, Long> createIdentityProcessor(int bufferSize) {
		FluxProcessor<Long, Long> p = EmitterProcessor.create(bufferSize);
		return FluxProcessor.wrap(p, p.log("EmitterProcessorTckTest", Level.FINE));
	}

	@Override
	public void required_mustRequestFromUpstreamForElementsThatHaveBeenRequestedLongAgo()
			throws Throwable {
		throw new SkipException("WARNING: EmitterProcessor does not emit until all " +
				"subscribers request at least 1");
	}

	@Override
	public void required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError()
			throws Throwable {
		throw new SkipException("WARNING: EmitterProcessor does not emit until all " +
				"subscribers request at least 1");
	}
}
