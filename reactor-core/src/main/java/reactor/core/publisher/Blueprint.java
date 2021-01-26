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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * @author Simon Baslé
 */
public interface Blueprint<T, SEED> {

	/**
	 * @return the {@link String} id under which the seed can be found in a {@link ContextView} (a context key)
	 */
	String id();

	/**
	 * The seed to use to generate the sequence when no explicit seeding has been detected in the {@link ContextView}.
	 *
	 * @return the seed to use with {@link #publisherFromSeed()} when no seed is in the context
	 */
	SEED fallbackSeed();

	/**
	 * Initializes the provided {@link Context}, returning a new {@link Context} that
	 * is ready to store seeds under the key {@link #id()}.
	 *
	 * @param contextToSeed the {@link Context} in which to prepare a spot for seeds
	 * @param s the initial seed
	 * @return the new {@link Context} that can be used to store seeds for this {@link Blueprint}
	 */
	Context initSeedStorage(Context contextToSeed, SEED s);

	/**
	 * An optional {@link Function} that can be used to extract a new seed from values that
	 * get emitted via {@link org.reactivestreams.Subscriber#onNext(Object)}.
	 * <p>At runtime, if this function is not null, a seeded publisher MUST notify the {@link Blueprint}
	 * of new values via {@link #updateSeedOnNext(ContextView, Object)}.
	 * <p>This function is directly exposed here as a way to detect that the blueprint supports
	 * seed updates, and to avoid calling {@link #updateSeedOnNext(ContextView, Object) update} methods
	 * altogether if that is not the case.
	 *
	 * @return the {@link Function} that extracts seeds from values, or null if seed updates are not desired
	 */
	@Nullable
	Function<T, SEED> seedExtractor();

	/**
	 * Generate a {@link Publisher} from a given seed. Blueprint-based publishers will generally
	 * convert the publisher to their parent reactive type ({@link Flux}, {@link Mono}), but
	 * it is recommended to directly produce consistent reactive types.
	 *
	 * @return the {@link Function} to generate a {@link Publisher} from a seed
	 */
	Function<SEED, ? extends Publisher<T>> publisherFromSeed();

	/**
	 * Defines how to store the given seed into the given {@link ContextView},
	 * with the assumption that the {@link ContextView} is consistent with preparations
	 * done in {@link #initSeedStorage(Context, Object)}. Use a value of {@code null} to
	 * clear the seed storage.
	 * <p>Implementations SHOULD protect against context not prepared for storage, and
	 * it is acceptable to return {@code null} in that case. Additionally, this method can
	 * also return {@code null} if the seed storage alllows being prepared with an initial
	 * {@code null} seed or has previously been cleared.
	 *
	 * @param ctx the {@link ContextView} through which seed storage can be resolved
	 * @param newSeed the new seed value to store
	 * @return the old seed value, or {@code null} if no seed was previously stored /
	 * the context isn't prepared for seed storage
	 * @see #initSeedStorage(Context, Object)
	 */
	@Nullable
	SEED storeSeed(ContextView ctx, @Nullable SEED newSeed);

	/**
	 * Defines how to read the current seed from the given {@link Context}.
	 * Can return {@code null} if the context hasn't been prepared for seed storage
	 * or doesn't currently contain a seed.
	 *
	 * @param ctx the {@link ContextView} through which seed storage can be resolved
	 * @return the currently stored seed value, or {@code null} if none
	 */
	@Nullable
	SEED loadSeed(ContextView ctx);

	/**
	 * Optional hook to update the stored seed on {@link org.reactivestreams.Subscriber#onNext(Object)}.
	 * Defaults to extracting a new seed from the value and {@link #storeSeed(ContextView, Object) storing it}
	 * if {@link #seedExtractor()} is not null, doing nothing otherwise.
	 *
	 * @param ctx the {@link ContextView} through which seed storage can be resolved
	 * @param value the value from which to extract a new seed
	 * @return true if the stored seed was modified, false otherwise
	 */
	default boolean updateSeedOnNext(ContextView ctx, T value) {
		Function<T, SEED> se = seedExtractor();
		if (se == null) {
			return false;
		}
		SEED newSeed = se.apply(value);
		storeSeed(ctx, newSeed);
		return true;
	}

	/**
	 * Optional hook to update the stored seed on {@link org.reactivestreams.Subscriber#onError(Throwable)}.
	 * Defaults to doing nothing.
	 * <p>Note that clearing the seed (by {@link #storeSeed(ContextView, Object) storing} null) in this
	 * method would reset the sequence eg. in case of {@link Flux#retry() retry}, which could be
	 * at odds with some use cases of seed updating.
	 *
	 * @param ctx the {@link ContextView} through which seed storage can be resolved
	 * @param error the {@link Throwable} that triggered this update
	 * @return true if the stored seed was modified, false otherwise
	 */
	default boolean updateSeedOnError(ContextView ctx, Throwable error) {
		return false;
	}

	/**
	 * Optional hook to update the stored seed on {@link Subscriber#onComplete()}
	 * Defaults to doing nothing.
	 * <p>Note that clearing the seed (by {@link #storeSeed(ContextView, Object) storing} null) in this
	 * method would reset the sequence eg. in case of {@link Flux#repeat() repeat}, which could be
	 * 	 * at odds with some use cases of seed updating.
	 *
	 * @param ctx the {@link ContextView} through which seed storage can be resolved
	 * @return true if the stored seed was modified, false otherwise
	 */
	default boolean updateSeedOnComplete(ContextView ctx) {
		return false;
	}

	/**
	 * Optional hook to update the stored seed on {@link Subscription#cancel()}.
	 * Defaults to clearing the stored seed, unless no {@link #seedExtractor()} has been defined.
	 *
	 * @param ctx the {@link ContextView} through which seed storage can be resolved
	 * @return true if the stored seed was modified, false otherwise
	 */
	default boolean updateSeedOnCancel(ContextView ctx) {
		if (seedExtractor() != null) {
			storeSeed(ctx, null);
			return true;
		}
		return false;
	}

	/**
	 * Start configuring a builder for {@link Blueprint} targeting a specific seed type
	 * by providing a default seed (see {@link Blueprint#fallbackSeed()}).
	 *
	 * @param defaultSeed the seed to be used as fallback (which drives the seed type generic)
	 * @param <S> the seed type for this builder
	 * @return the next builder step
	 */
	static <S> TemplateStep<S> givenFallbackSeed(S defaultSeed) {
		return new AtomicRefBlueprintBuilder<>(defaultSeed, null, null);
	}

	final class AtomicRefBlueprint<T, SEED>
			implements Blueprint<T, SEED> {

		private final String                                 id;
		private final SEED                                   fallbackSeed;
		private final Function<T, SEED>                      seedExtractor;
		private final Function<SEED, ? extends Publisher<T>> publisherFromSeed;

		AtomicRefBlueprint(
				String id,
				SEED fallbackSeed,
				@Nullable Function<T, SEED> extractor,
				Function<SEED, ? extends Publisher<T>> seed) {
			this.id = id;
			this.fallbackSeed = fallbackSeed;
			seedExtractor = extractor;
			publisherFromSeed = seed;
		}

		@Override
		public String id() {
			return this.id;
		}

		@Override
		public SEED fallbackSeed() {
			return this.fallbackSeed;
		}

		@Override
		public Context initSeedStorage(Context contextToSeed, SEED s) {
			//important: if the seeding happens closer to subscription, downstream, it takes priority
			if (contextToSeed.hasKey(id())) {
				return contextToSeed;
			}
			else {
				return contextToSeed.put(id(), new AtomicReference<>(s));
			}
		}

		@Override
		@Nullable
		public Function<T, SEED> seedExtractor() {
			return this.seedExtractor;
		}

		@Override
		public Function<SEED, ? extends Publisher<T>> publisherFromSeed() {
			return this.publisherFromSeed;
		}

		@Override
		@Nullable
		public SEED storeSeed(ContextView ctx, @Nullable SEED newSeed) {
			AtomicReference<SEED> container = ctx.getOrDefault(id(), null);
			if (container != null) {
				return container.getAndSet(newSeed);
			}
			return null;
		}

		@Override
		@Nullable
		public SEED loadSeed(ContextView ctx) {
			AtomicReference<SEED> container = ctx.getOrDefault(id(), null);
			if (container != null) {
				return container.get();
			}
			return null;
		}
	}

	/**
	 * {@link Blueprint} builder step around configuring the publisher-generation {@link Function}.
	 * @param <S> the seed type
	 */
	interface TemplateStep<S> {

		/**
		 * Continue configuring the {@link Blueprint} builder, specifying how to generate a
		 * {@link Publisher} from a seed.
		 *
		 * @param publisherGenerator the {@link Function} to generate a {@link Publisher} from a seed
		 * @param <T> the type of values emitted by the publisher (and from which seeds can be extracted)
		 * @param <P> the publisher type
		 * @return the next builder step
		 */
		<T, P extends Publisher<T>> SeedStep<S, T> andTemplate(Function<S, P> publisherGenerator);
	}

	/**
	 * {@link Blueprint} builder step around configuring the seed update feature.
	 * @param <S> the seed type
	 * @param <T> the value type
	 */
	interface SeedStep<S, T> {

		/**
		 * Activate seed updates in case of {@link Subscriber#onNext(Object)}, with the
		 * provided {@link Function} to extract a seed from an onNext value.
		 *
		 * @param seedExtractor the {@link Function} to extract seeds from onNext values
		 * @return the next builder step
		 * @see Blueprint#updateSeedOnNext(ContextView, Object)
		 */
		IdStep<S, T> withSeedUpdatedOnNext(Function<T, S> seedExtractor);

		/**
		 * Deactivate seed updates.
		 *
		 * @return the next builder step
		 */
		IdStep<S, T> withSeedPersistent();
	}

	/**
	 * Final {@link Blueprint} builder step with the option to set the {@link Blueprint#id()}.
	 * @param <S> the seed type
	 * @param <T> the value type
	 */
	interface IdStep<S, T> {

		/**
		 * Create the {@link Blueprint} with the specified {@link Blueprint#id()},
		 * under which the seed storage will be resolved and accessed from {@link Context}.
		 *
		 * @param id the blueprint id
		 * @return  the created {@link Blueprint}
		 */
		Blueprint<T, S> createBlueprint(String id);

		/**
		 * Create the {@link Blueprint} with no specified {@link Blueprint#id()}.
		 * The {@link Blueprint} should use a fallback value, for instance the identity
		 * toString of its {@link Blueprint#publisherFromSeed() generator Function}.
		 *
		 * @return  the created "anonymous" {@link Blueprint}
		 */
		Blueprint<T, S> createAnonymousBlueprint();
	}

	final class AtomicRefBlueprintBuilder<SEED, T>
			implements TemplateStep<SEED>, SeedStep<SEED, T>, IdStep<SEED, T> {

		@Nullable
		SEED defaultSeed;

		@Nullable
		Function<T, SEED> seedExtractor;

		@Nullable
		Function<SEED, ? extends Publisher<T>> publisherGenerator;

		AtomicRefBlueprintBuilder(
				@Nullable SEED defaultSeed,
				@Nullable Function<T, SEED> seedExtractor,
				@Nullable Function<SEED, ? extends Publisher<T>> publisherGenerator) {
			this.defaultSeed = defaultSeed;
			this.seedExtractor = seedExtractor;
			this.publisherGenerator = publisherGenerator;
		}

		@Override
		public <T1, P1 extends Publisher<T1>> SeedStep<SEED, T1> andTemplate(
				Function<SEED, P1> publisherGenerator) {
			return new AtomicRefBlueprintBuilder<>(this.defaultSeed, null, publisherGenerator);
		}

		@Override
		public IdStep<SEED, T> withSeedUpdatedOnNext(Function<T, SEED> seedExtractor) {
			return new AtomicRefBlueprintBuilder<>(this.defaultSeed, seedExtractor, this.publisherGenerator);
		}

		@Override
		public IdStep<SEED, T> withSeedPersistent() {
			return new AtomicRefBlueprintBuilder<>(this.defaultSeed, null, this.publisherGenerator);
		}



		@Override
		public Blueprint<T, SEED> createAnonymousBlueprint() {
			return createBlueprint(null);
		}

		@Override
		public Blueprint<T, SEED> createBlueprint(@Nullable String id) {
			if (this.defaultSeed == null || this.publisherGenerator == null) {
				throw new IllegalStateException("builder led to null attributes");
			}

			if (id == null) {
				id = this.publisherGenerator.toString();
			}

			return new AtomicRefBlueprint<>(
					id,
					this.defaultSeed,
					this.seedExtractor,
					this.publisherGenerator);
		}
	}
}
