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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import reactor.test.MemoryUtils;
import reactor.test.MemoryUtils.Tracked;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class FluxBootstrapExperimentTest {

	@Test
	@DisplayName("bootstrapping from context storing POJOs")
	void bootstrapSimpleCase() {
		//In this experiment the publisher is one of POJOs. These POJOs are retained
		//as-is for bootstrapping purpose.

		AtomicLong transientErrorCount = new AtomicLong(3);

		final String CONTEXT_BOOTSTRAP_KEY = "CONTEXT_BOOTSTRAP_KEY";

		Flux<Integer> bootstrap = Flux
				.deferContextual(ctx -> {

					//simulate a source that errors after 4 elements...
					final AtomicInteger errorCountdown = new AtomicInteger(4);
					//...but that error is transient / stops after x cycles
					if (transientErrorCount.decrementAndGet() == 0) {
						errorCountdown.set(1000);
					}

					AtomicReference<Integer> bootstrapHolder;
					//the source is also bootstrapping itself from context
					int skipX = 0;
					if (!ctx.hasKey(CONTEXT_BOOTSTRAP_KEY)) {
						bootstrapHolder = new AtomicReference<>(null); //unused null object
						System.out.println("WARNING: No bootstrap key " + CONTEXT_BOOTSTRAP_KEY + " found in Context, will always start the sequence from scratch");
					}
					else {
						bootstrapHolder = ctx.get(CONTEXT_BOOTSTRAP_KEY);
						Integer bootstrapFromCtx = bootstrapHolder.get();
						if (bootstrapFromCtx != null && bootstrapFromCtx > 0) {
							skipX = bootstrapFromCtx;
							System.err.println("bootstrapping by skipping " + skipX);
						}
						else {
							System.err.println("bootstrapping from scratch");
						}
					}

					//we simulate a source that can be started from a later point determined by
					//what's in the context. in reality this could be a query parameter for instance...
					Flux<Integer> source = Flux.range(skipX+1, 10-skipX);

					return source
							.doOnNext(id -> {
								if (errorCountdown.decrementAndGet() < 1) {
									System.err.println("Error triggered in " + id);
									throw new IllegalStateException("Error triggered in " + id);
								}
								if (ctx.hasKey(CONTEXT_BOOTSTRAP_KEY)) {
									bootstrapHolder.set(id);
								}
							})
							.hide();
				});

		//this simulates a retry applied to the bootstrapping Flux
		Flux<String> using = bootstrap
				//we now can retry the bootstrapped Flux
				.retry(4)
				.contextWrite(ctx -> ctx.put(CONTEXT_BOOTSTRAP_KEY, new AtomicReference<Integer>()))
				//let's simulate usage of the off-heap object: release on conversion to more mundane type
				//as well as on discard
				.map(i -> "value" + i)
				.doOnDiscard(Tracked.class, Tracked::safeRelease);

		StepVerifier.create(using.doOnNext(v -> System.out.println("Seen " + v)).collectList())
		            .assertNext(l -> assertThat(l).containsExactly("value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "value10"))
		            .verifyComplete();

		System.out.println("separate subscription");

		StepVerifier.create(bootstrap.contextWrite(Context.of(CONTEXT_BOOTSTRAP_KEY, new AtomicReference<>(8))))
		            .expectNext(9, 10)
		            .verifyComplete();
	}


	@Test
	@DisplayName("bootstrapping from context storing extracted data read from off heap objects")
	void bootstrapOffheapFluxWithExtractedData() {
		//In this experiment the publisher is one of off heap objects.
		//However we don't bootstrap from the whole objects, but rather from an extracted subset of their content.
		//This alleviates the challenge of retaining and releasing off-heap objects while they are stored in the
		//context for bootstrapping purpose.

		MemoryUtils.OffHeapDetector offHeapDetector = new MemoryUtils.OffHeapDetector();
		AtomicLong transientErrorCount = new AtomicLong(3);

		final String CONTEXT_BOOTSTRAP_KEY = "CONTEXT_BOOTSTRAP_KEY";

		Flux<Tracked<String>> bootstrap = Flux
				.deferContextual(ctx -> {
					//simulate a source that errors after 4 elements...
					final AtomicInteger errorCountdown = new AtomicInteger(4);
					//...but that error is transient / stops after x cycles
					if (transientErrorCount.decrementAndGet() == 0) {
						errorCountdown.set(1000);
					}

					AtomicInteger bootstrapHolder;
					//the source is also bootstrapping itself from context
					int skipX = 0;
					if (!ctx.hasKey(CONTEXT_BOOTSTRAP_KEY)) {
						bootstrapHolder = new AtomicInteger(); //unused null object
						System.out.println("WARNING: No bootstrap key " + CONTEXT_BOOTSTRAP_KEY + " found in Context, will always start the sequence from scratch");
					}
					else {
						bootstrapHolder = ctx.get(CONTEXT_BOOTSTRAP_KEY);
						int bootstrapFromCtx = bootstrapHolder.get();
						if (bootstrapFromCtx > 0) {
							skipX = bootstrapFromCtx;
							System.err.println("bootstrapping by skipping " + skipX);
						}
						else {
							System.err.println("bootstrapping from scratch");
						}
					}

					//we simulate a source that can be started from a later point determined by
					//what's in the context. in reality this could be a query parameter for instance...
					Flux<String> source = Flux.range(skipX+1, 10-skipX)
					                          .map(i -> "value"+i);

					return source
							.map(id -> {
								if (errorCountdown.decrementAndGet() < 1) {
									System.err.println("Error triggered in " + id);
									throw new IllegalStateException("Error triggered in " + id);
								}
								Tracked<String> offHeap = offHeapDetector.track(id);

								if (ctx.hasKey(CONTEXT_BOOTSTRAP_KEY)) {
									//simulate extracting a meaningful value from the content of the off heap object
									//that value will be stored in context and reused for future bootstrap.
									//as a consequence, no need to retain/clear the bootstrap value (since it is on-heap)
									int newSkip = Integer.parseInt(offHeap.getContent().replace("value", ""));
									bootstrapHolder.set(newSkip);
								}
								return offHeap;
							})
							.hide();
				});

		//this simulates a retry applied to the bootstrapping Flux
		Flux<String> using = bootstrap
				//we now can retry the bootstrapped Flux
				.retry(4)
				.contextWrite(ctx -> ctx.put(CONTEXT_BOOTSTRAP_KEY, new AtomicInteger(0)))
				//let's simulate usage of the off-heap object: release on conversion to more mundane type
				//as well as on discard
				.map(tracked -> {
					tracked.release();
					return tracked.getContent();
				})
				.doOnDiscard(Tracked.class, Tracked::safeRelease);

		StepVerifier.create(using.doOnNext(v -> System.out.println("Seen " + v)).collectList())
		            .assertNext(l -> assertThat(l).containsExactly("value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "value10"))
		            .verifyComplete();

		System.out.println("separate subscription");

		StepVerifier.create(bootstrap.contextWrite(Context.of(CONTEXT_BOOTSTRAP_KEY, new AtomicInteger(8))).map(Tracked::getContent))
		            .expectNext("value9", "value10")
		            .verifyComplete();
	}

	@Test
	@DisplayName("bootstrapping from context storing off heap objects")
	void bootstrapSupportsOffheap() {
		//In this experiment the publisher is one of off heap objects.
		//Additionally, the challenge is to retain and bootstrap from these off-heap object,
		//which involves _retaining_ the objects while they are retained for bootstrapping purposes.
		//We might want to discourage that particular use case in favor of the intermediate one above.

		MemoryUtils.OffHeapDetector offHeapDetector = new MemoryUtils.OffHeapDetector();
		AtomicLong transientErrorCount = new AtomicLong(3);

		final String CONTEXT_BOOTSTRAP_KEY = "CONTEXT_BOOTSTRAP_KEY";

		Flux<Tracked<String>> bootstrap = Flux
				.deferContextual(ctx -> {
					//simulate a source that errors after 4 elements...
					final AtomicInteger errorCountdown = new AtomicInteger(4);
					//...but that error is transient / stops after x cycles
					if (transientErrorCount.decrementAndGet() == 0) {
						errorCountdown.set(1000);
					}

					//the source is also bootstrapping itself from context
					String skipUntil = null;
					AtomicReference<Tracked<String>> bootstrapFromCtx = ctx.getOrDefault(CONTEXT_BOOTSTRAP_KEY, null);
					Runnable releaseLeftoverBootstrap;
					if (bootstrapFromCtx == null) {
						System.out.println("WARNING: No bootstrap key " + CONTEXT_BOOTSTRAP_KEY + " found in Context, will always start the sequence from scratch");
						releaseLeftoverBootstrap = () -> {};
					}
					else {
						releaseLeftoverBootstrap = () -> Tracked.safeRelease(bootstrapFromCtx.getAndSet(null));
						if (bootstrapFromCtx.get() != null) {
							skipUntil = bootstrapFromCtx.get().getContent();
							System.err.println("bootstrapping from " + skipUntil);
						}
						else {
							System.err.println("bootstrapping from scratch");
						}
					}

					//we simulate a source that can be started from a later point determined by
					//what's in the context. in reality this could be a query parameter for instance...
					Flux<String> source = Flux.range(1, 10)
					                          .map(i -> "value"+i);
					if (skipUntil != null) {
						source = source.skipUntil(skipUntil::equalsIgnoreCase).skip(1);
					}

					return source
							.map(id -> {
								if (errorCountdown.decrementAndGet() < 1) {
									System.err.println("Error triggered in " + id);
									throw new IllegalStateException("Error triggered in " + id);
								}
								Tracked<String> offHeap = offHeapDetector.track(id);

								if (bootstrapFromCtx != null) {
									offHeap.retain();
									Tracked<String> oldLatest = bootstrapFromCtx.getAndSet(offHeap);
									Tracked.safeRelease(oldLatest);
								}
								return offHeap;
							})
							.doOnCancel(releaseLeftoverBootstrap)
							.doOnComplete(releaseLeftoverBootstrap)
							.hide();
				});
				//contextWrite(Context) could have seemed to "work" here because it instantiates a Context once, and that Context value is mutated
				//however, this is misleading since now ALL subscriptions share the same reference
				//so we MUST set the AtomicReference(null) in a way that is unique to each final subscription

		//this simulates a retry applied to the bootstrapping Flux
		Flux<String> using = bootstrap
				//we now can retry the bootstrapped Flux
				.retry(4)
				//bug or limitation: `withRetryContext` is not injected into the `currentContext()` so we cannot use `retryWhen` for that
//				.retryWhen(Retry.max(4).withRetryContext(Context.of(CONTEXT_BOOTSTRAP_KEY, new AtomicReference<>(null))))
				.contextWrite(ctx -> ctx.put(CONTEXT_BOOTSTRAP_KEY, new AtomicReference<Tracked<String>>(null)))
				//let's simulate usage of the off-heap object: release on conversion to more mundane type
				//as well as on discard
				.map(tracked -> {
					tracked.release();
					return tracked.getContent();
				})
				.doOnDiscard(Tracked.class, Tracked::safeRelease);

		StepVerifier.create(using.doOnNext(v -> System.out.println("Seen " + v)).collectList())
		            .assertNext(l -> assertThat(l).containsExactly("value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "value10"))
		            .verifyComplete();

		System.out.println("separate subscription");

		Flux<String> otherUsage = bootstrap
				.contextWrite(Context.of(CONTEXT_BOOTSTRAP_KEY, new AtomicReference<>(new Tracked<>("value8"))))
				.map(tracked -> {
					tracked.release();
					return tracked.getContent();
				})
				.doOnDiscard(Tracked.class, Tracked::safeRelease);


		StepVerifier.create(otherUsage)
		            .expectNext("value9", "value10")
		            .verifyComplete();

		offHeapDetector.assertNoLeaks();
	}

}