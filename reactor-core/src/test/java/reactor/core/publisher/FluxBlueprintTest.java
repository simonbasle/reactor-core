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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import static org.assertj.core.api.Assertions.assertThat;

class FluxBlueprintTest {

	@Test
	void blueprintValueAsSeed() {
		final Blueprint<Integer, Integer> blueprint = Blueprint
				.givenFallbackSeed(0)
				.andTemplate(i -> Flux.range(i+1, 10 - i))
				.withSeedUpdatedOnNext(Function.identity())
				.createBlueprint("Int");

		Flux.from(blueprint)
			.seedWith(blueprint, 4)
			.collectList()
			.as(StepVerifier::create)
			.assertNext(l -> assertThat(l).containsExactly(5, 6, 7, 8, 9, 10))
			.verifyComplete();
	}

	@Test
	void blueprintValueAsSeed_noSeeding() {
		final Blueprint<Integer, Integer> blueprint = Blueprint
				.givenFallbackSeed(0)
				.andTemplate(i -> Flux.range(i+1, 10 - i))
				.withSeedUpdatedOnNext(Function.identity())
				.createBlueprint("Int");

		Flux.from(blueprint)
			.collectList()
			.as(StepVerifier::create)
			.assertNext(l -> assertThat(l).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
			.verifyComplete();
	}

	@Test
	void blueprintExtractedSeed() {
		final Blueprint<int[], Integer> blueprint = Blueprint
				.givenFallbackSeed(0)
				.andTemplate(i -> Flux.range(i+1, 10 - i).map(j -> new int[] {99, j }))
				.withSeedUpdatedOnNext(arr -> arr[1])
				.createBlueprint("arrayToInt");

		Flux.from(blueprint)
			.seedWith(blueprint, 4)
			.collectList()
			.as(StepVerifier::create)
			.assertNext(l -> assertThat(l)
					.allMatch(arr -> arr[0] == 99)
					.extracting(arr -> arr[1])
					.containsExactly(5, 6, 7, 8, 9, 10)
			)
			.verifyComplete();
	}

	@Test
	void blueprintExtractedSeed_noSeeding() {
		final Blueprint<int[], Integer> blueprint = Blueprint
				.givenFallbackSeed(0)
				.andTemplate(i -> Flux.range(i+1, 10 - i).map(j -> new int[] {99, j }))
				.withSeedUpdatedOnNext(arr -> arr[1])
				.createBlueprint("arrayToInt");

		Flux.from(blueprint)
			.collectList()
			.as(StepVerifier::create)
			.assertNext(l -> assertThat(l)
					.allMatch(arr -> arr[0] == 99)
					.extracting(arr -> arr[1])
					.containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
			)
			.verifyComplete();
	}

	@Test
	void noSeedExtractionWhenNoSeed() {
		final AtomicInteger extractionCount = new AtomicInteger();
		final TestBlueprint<int[], Integer> testBlueprint = new TestBlueprint<>(Blueprint
				.givenFallbackSeed(0)
				.andTemplate(i -> Flux.range(i+1, 10 - i).map(j -> new int[] {99, j }))
				.withSeedUpdatedOnNext(arr -> {
					extractionCount.incrementAndGet();
					return arr[1];
				})
				.createBlueprint("arrayToInt"));

		Flux.from(testBlueprint)
		    .collectList()
		    .as(StepVerifier::create)
		    .assertNext(l -> assertThat(l)
				    .allMatch(arr -> arr[0] == 99)
				    .extracting(arr -> arr[1])
				    .containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		    )
		    .verifyComplete();

		assertThat(testBlueprint.seedOnNextCount).as("seedOnNextCount").isZero();
		assertThat(extractionCount).as("seedExtraction").hasValue(0);
	}

	@Test
	void noSeedStoreWhenNoSeedExtractor() {
		final AtomicInteger extractionCount = new AtomicInteger();
		final TestBlueprint<int[], Integer> testBlueprint = new TestBlueprint<>(Blueprint
				.givenFallbackSeed(0)
				.andTemplate(i -> Flux.range(i+1, 10 - i).map(j -> new int[] {99, j }))
				.withSeedUpdatedOnNext(arr -> {
					extractionCount.incrementAndGet();
					return arr[1];
				})
				.createBlueprint("arrayToInt"));

		Flux.from(testBlueprint)
		    .collectList()
		    .as(StepVerifier::create)
		    .assertNext(l -> assertThat(l)
				    .allMatch(arr -> arr[0] == 99)
				    .extracting(arr -> arr[1])
				    .containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		    )
		    .verifyComplete();

		assertThat(testBlueprint.seedOnNextCount).as("seedOnNextCount").isZero();
		assertThat(extractionCount).as("seedExtraction").hasValue(0);
	}

	@Test
	void canSeedTheWholeSource() {
		Blueprint<Integer, Flux<Integer>> blueprint = Blueprint
				.givenFallbackSeed(Flux.range(1, 10))
				.andTemplate(Function.identity())
				.withSeedPersistent()
				.createAnonymousBlueprint();

		Flux.from(blueprint)
			.seedWith(blueprint, Flux.range(10, 3))
			.collectList()
			.as(StepVerifier::create)
			.assertNext(l -> assertThat(l).containsExactly(10, 11, 12))
			.verifyComplete();
	}

	@Test
	void blueprintSeedingIsPerOuterSubscription() {
		//FIXME
	}

	@Test
	void blueprintAllowsToRetainSeenValuesInARetryWhenSeeded() {
		AtomicBoolean shouldError = new AtomicBoolean(true);

		final Blueprint<Integer, Integer> blueprint = Blueprint
				.givenFallbackSeed(0)
				.andTemplate(i -> Flux
						.range(i+1, 10 - i)
						.map(v -> {
							if (v == 5 && shouldError.compareAndSet(true, false)) {
								throw new IllegalStateException("Transient expected error");
							}
							return v;
						}))
				.withSeedUpdatedOnNext(Function.identity())
				.createBlueprint("Int");

		TestBlueprint<Integer, Integer> testBlueprint = new TestBlueprint<>(blueprint);

		Flux.from(testBlueprint)
			.retry(1)
			.seedWith(blueprint, 0)
			.as(StepVerifier::create)
			.expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
			.verifyComplete();

		assertThat(testBlueprint.seedOnNextCount).as("seed update count").isEqualTo(10);
	}

	@Test
	void blueprintRestartingFromScratchInARetryWhenNotSeeded() {
		AtomicBoolean shouldError = new AtomicBoolean(true);

		final Blueprint<Integer, Integer> blueprint = Blueprint
				.givenFallbackSeed(0)
				.andTemplate(i -> Flux.range(i+1, 10 - i)
					.doOnNext(v -> {
						if (v == 5 && shouldError.compareAndSet(true, false)) {
							throw new IllegalStateException("Transient expected error");
						}
					})
				)
				.withSeedUpdatedOnNext(Function.identity())
				.createBlueprint("Int");

		Flux.from(blueprint)
			.retry(1)
			.as(StepVerifier::create)
			.expectNext(1, 2, 3, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
			.verifyComplete();
	}

	static class TestBlueprint<T, S> implements Blueprint<T, S> {

		final Blueprint<T, S> delegate;

		int seedOnNextCount;
		int seedOnErrorCount;
		int seedOnCompleteCount;
		int seedOnCancelCount;

		TestBlueprint(Blueprint<T, S> delegate) {
			this.delegate = delegate;
		}

		@Override
		public String id() {
			return delegate.id();
		}

		@Override
		public S fallbackSeed() {
			return delegate.fallbackSeed();
		}

		@Override
		public Context initSeedStorage(Context contextToSeed, S s) {
			return delegate.initSeedStorage(contextToSeed, s);
		}

		@Override
		@Nullable
		public Function<T, S> seedExtractor() {
			return delegate.seedExtractor();
		}

		@Override
		public Function<S, ? extends Publisher<T>> publisherFromSeed() {
			return delegate.publisherFromSeed();
		}

		@Override
		@Nullable
		public S storeSeed(ContextView ctx, @Nullable S newSeed) {
			return delegate.storeSeed(ctx, newSeed);
		}

		@Override
		@Nullable
		public S loadSeed(ContextView ctx) {
			return delegate.loadSeed(ctx);
		}

		@Override
		public boolean updateSeedOnNext(ContextView ctx, T value) {
			if (delegate.updateSeedOnNext(ctx, value)) {
				seedOnNextCount++;
				return true;
			}
			return false;
		}

		@Override
		public boolean updateSeedOnError(ContextView ctx, Throwable error) {
			if (delegate.updateSeedOnError(ctx, error)) {
				seedOnErrorCount++;
				return true;
			}
			return false;
		}

		@Override
		public boolean updateSeedOnComplete(ContextView ctx) {
			if (delegate.updateSeedOnComplete(ctx)) {
				seedOnCompleteCount++;
				return true;
			}
			return false;
		}

		@Override
		public boolean updateSeedOnCancel(ContextView ctx) {
			if (delegate.updateSeedOnCancel(ctx)) {
				seedOnCancelCount++;
				return true;
			}
			return false;
		}
	}

}
