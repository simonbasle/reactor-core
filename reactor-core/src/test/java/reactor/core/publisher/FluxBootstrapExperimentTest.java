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

		//this will be used so that errors stop after 3 re-subscriptions
		AtomicLong transientErrorCount = new AtomicLong(3);

		//the deferContextual will search for an AtomicReference with that key in the Context
		final String CONTEXT_BOOTSTRAP_KEY = "CONTEXT_BOOTSTRAP_KEY";

		Flux<Integer> bootstrap = Flux
				.deferContextual(ctx -> {

					//simulate a source that errors after 4 elements...
					final AtomicInteger errorCountdown = new AtomicInteger(4);
					//...but that error is transient / stops after x cycles
					if (transientErrorCount.decrementAndGet() == 0) {
						errorCountdown.set(1000);
					}

					//we'll look for an AtomicReference in the Context
					AtomicReference<Integer> bootstrapHolder;
					//from that, we'll get a business value that allows us to "restart from a known point"
					int skipX = 0;
					//we need to cover the case where no such holder is set up, so the publisher becomes a classic cold publisher
					if (!ctx.hasKey(CONTEXT_BOOTSTRAP_KEY)) {
						//here we create a fake holder so that the rest of the code is unchanged, but even if the holder
						//is updated with a seed, it won't be reused
						bootstrapHolder = new AtomicReference<>(null); //unused null object
						System.out.println("WARNING: No bootstrap key " + CONTEXT_BOOTSTRAP_KEY + " found in Context, will always start the sequence from scratch");
					}
					//here is the meat of the bootstrapping:
					else {
						bootstrapHolder = ctx.get(CONTEXT_BOOTSTRAP_KEY);
						Integer bootstrapFromCtx = bootstrapHolder.get();
						//if there is a meaningful "last known point", reshape the source to take it into account
						if (bootstrapFromCtx != null && bootstrapFromCtx > 0) {
							skipX = bootstrapFromCtx;
							System.err.println("bootstrapping by skipping " + skipX);
						}
						//otherwise, initial subscription, full dataset. here the business value of skipX = 0 is sufficient
						else {
							System.err.println("bootstrapping from scratch");
						}
					}

					//this generates the source from the know point / seed extracted from Context.
					Flux<Integer> source = Flux
							//in reality this could be a parameterized db query for instance...
							.range(skipX+1, 10-skipX)
							//we also trigger the error from the countdown (simulating the transient error)
							.doOnNext(id -> {
								if (errorCountdown.decrementAndGet() < 1) {
									System.err.println("Error triggered in " + id);
									throw new IllegalStateException("Error triggered in " + id);
								}
							});

					//this is the source, but taking into account the fact that we need to update the seed
					return source
							//the meat of it is that when the source emit, we update the seed in the AtomicReference
							.doOnNext(newSeed -> {
								if (ctx.hasKey(CONTEXT_BOOTSTRAP_KEY)) {
									bootstrapHolder.set(newSeed);
								}
							})
							.hide();
				});

		//this simulates a retry applied to the bootstrapping Flux
		Flux<String> using = bootstrap
				//we now can retry the bootstrapped Flux
				.retry(4)
				//but we need to set up the Context so that the mutable seed holder is there
				.contextWrite(ctx -> ctx.put(CONTEXT_BOOTSTRAP_KEY, new AtomicReference<Integer>()))
				//we use the source as any other publisher
				.map(i -> "value" + i);

		StepVerifier.create(using.doOnNext(v -> System.out.println("Seen " + v)).collectList())
		            .assertNext(l -> assertThat(l).containsExactly("value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "value10"))
		            .verifyComplete();

		//we can even subscribe multiple times to the outer sequence, each get their own seed thanks to the lazyness of the contextWrite(Function)
		StepVerifier.create(using.doOnNext(v -> System.out.println("Seen " + v)).collectList())
		            .assertNext(l -> assertThat(l).containsExactly("value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "value10"))
		            .verifyComplete();

		System.out.println("\nseparate usage\n");

		//again, the only constraint is that we prepare that particular sequence's Context
		//careful about contextWrite(ContextView) simplified API, as it will
		Flux<Integer> using2 = bootstrap
				.doOnNext(v -> System.out.println("Different usage of " + v))
				//again, the only constraint is that we prepare that particular sequence's Context
				//careful about contextWrite(ContextView) simplified API: past this point any subscription reuses the same mutable holder
				//and thus will step on each other's toe in terms of bootstrapping
				.contextWrite(Context.of(CONTEXT_BOOTSTRAP_KEY, new AtomicReference<>(7)));

		StepVerifier.create(using2)
		            .expectNext(8, 9, 10)
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