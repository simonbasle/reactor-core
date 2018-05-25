/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Uses a resource that is lazily generated by a {@link Publisher} for each individual{@link Subscriber},
 * while streaming the values from a {@link Publisher} derived from the same resource.
 * Whenever the resulting sequence terminates, the relevant {@link Function} generates
 * a "cleanup" {@link Publisher} that is invoked but doesn't change the content of the
 * main sequence. Instead it just defers the termination (unless it errors, in which case
 * the error suppresses the original termination signal).
 * <p>
 * Note that if the resource supplying {@link Publisher} emits more than one resource, the
 * subsequent resources are dropped ({@link Operators#onNextDropped(Object, Context)}). If
 * the publisher errors AFTER having emitted one resource, the error is also silently dropped
 * ({@link Operators#onErrorDropped(Throwable, Context)}).
 *
 * @param <T> the value type streamed
 * @param <S> the resource type
 */
final class FluxUsingWhen<T, S> extends Flux<T> implements Fuseable, SourceProducer<T> {

	final Publisher<S>                                          resourceSupplier;
	final Function<? super S, ? extends Publisher<? extends T>> resourceClosure;
	final Function<? super S, ? extends Publisher<?>>           asyncComplete;
	final Function<? super S, ? extends Publisher<?>>           asyncError;
	@Nullable
	final Function<? super S, ? extends Publisher<?>>           asyncCancel;

	FluxUsingWhen(Publisher<S> resourceSupplier,
			Function<? super S, ? extends Publisher<? extends T>> resourceClosure,
			Function<? super S, ? extends Publisher<?>> asyncComplete,
			Function<? super S, ? extends Publisher<?>> asyncError,
			@Nullable Function<? super S, ? extends Publisher<?>> asyncCancel) {
		this.resourceSupplier = Objects.requireNonNull(resourceSupplier, "resourceSupplier");
		this.resourceClosure = Objects.requireNonNull(resourceClosure, "resourceClosure");
		this.asyncComplete = Objects.requireNonNull(asyncComplete, "asyncComplete");
		this.asyncError = Objects.requireNonNull(asyncError, "asyncError");
		this.asyncCancel = asyncCancel;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super T> actual) {
		if (resourceSupplier instanceof Callable) {
			try {
				Callable<S> resourceCallable = (Callable<S>) resourceSupplier;
				S resource = resourceCallable.call();
				if (resource == null) {
					Operators.complete(actual);
				}
				else {
					subscribeToResource(resource, actual, resourceClosure,
							asyncComplete, asyncError, asyncCancel);
				}
			}
			catch (Throwable e) {
				Operators.error(actual, e);
			}
			return;
		}

		//trigger the resource creation and delay the subscription to actual
		resourceSupplier.subscribe(new ResourceSubscriber(actual, resourceClosure, asyncComplete, asyncError, asyncCancel, resourceSupplier instanceof Mono));
	}

	@Override
	public Object scanUnsafe(Attr key) {
		return null; //no particular key to be represented, still useful in hooks
	}

	private static <S, T> void subscribeToResource(S resource,
			CoreSubscriber<? super T> actual,
			Function<? super S, ? extends Publisher<? extends T>> resourceClosure,
			Function<? super S, ? extends Publisher<?>> asyncComplete,
			Function<? super S, ? extends Publisher<?>> asyncError,
			@Nullable Function<? super S, ? extends Publisher<?>> asyncCancel) {

		Publisher<? extends T> p;

		try {
			p = Objects.requireNonNull(resourceClosure.apply(resource),
					"The resourceClosure function returned a null value");
		}
		catch (Throwable e) {
			//TODO should a closure#apply error translate to a asyncError? (for now it will)
			p = Flux.error(e);
		}

		if (p instanceof Fuseable) {
			Flux.from(p).subscribe(new UsingWhenFuseableSubscriber<>(actual,
					resource, asyncComplete, asyncError, asyncCancel));
		}
		else if (actual instanceof ConditionalSubscriber) {
			Flux.from(p).subscribe(new UsingWhenConditionalSubscriber<>((ConditionalSubscriber<? super T>) actual,
					resource, asyncComplete, asyncError, asyncCancel));
		}
		else {
			Flux.from(p).subscribe(new UsingWhenSubscriber<>(actual, resource, asyncComplete, asyncError, asyncCancel));
		}
	}

	static class ResourceSubscriber<S, T> implements InnerConsumer<S> {

		final CoreSubscriber<? super T> actual;

		final Function<? super S, ? extends Publisher<? extends T>> resourceClosure;
		final Function<? super S, ? extends Publisher<?>>           asyncComplete;
		final Function<? super S, ? extends Publisher<?>>           asyncError;
		@Nullable
		final Function<? super S, ? extends Publisher<?>>           asyncCancel;
		final boolean                                               isMonoSource;

		Subscription s;
		boolean resourceProvided;

		ResourceSubscriber(CoreSubscriber<? super T> actual,
				Function<? super S, ? extends Publisher<? extends T>> resourceClosure,
				Function<? super S, ? extends Publisher<?>> asyncComplete,
				Function<? super S, ? extends Publisher<?>> asyncError,
				@Nullable Function<? super S, ? extends Publisher<?>> asyncCancel,
				boolean isMonoSource) {
			this.actual = Objects.requireNonNull(actual, "actual");
			this.resourceClosure = Objects.requireNonNull(resourceClosure, "resourceClosure");
			this.asyncComplete = Objects.requireNonNull(asyncComplete, "asyncComplete");
			this.asyncError = Objects.requireNonNull(asyncError, "asyncError");
			this.asyncCancel = asyncCancel;
			this.isMonoSource = isMonoSource;
		}

		@Override
		public void onNext(S resource) {
			if (resourceProvided) {
				Operators.onNextDropped(resource, actual.currentContext());
				return;
			}
			resourceProvided = true;

			FluxUsingWhen.subscribeToResource(resource, actual, resourceClosure,
					asyncComplete, asyncError, asyncCancel);

			if (!isMonoSource) {
				s.cancel();
			}
		}

		@Override
		public void onError(Throwable throwable) {
			if (resourceProvided) {
				Operators.onErrorDropped(throwable, actual.currentContext());
				return;
			}
			//if no resource provided, actual.onSubscribe has not been called
			//let's call it and immediately terminate it with the error
			Operators.error(actual, throwable);
		}

		@Override
		public void onComplete() {
			if (resourceProvided) {
				return;
			}
			//if no resource provided, actual.onSubscribe has not been called
			//let's call it and immediately complete it
			Operators.complete(actual);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			return null; //TODO enrich
		}
	}

	static class UsingWhenSubscriber<T, S> implements InnerOperator<T, T>,
	                                                  QueueSubscription<T> {

		//state that differs in the different variants
		final CoreSubscriber<? super T>                                            actual;
		volatile Subscription                                                      s;
		static final AtomicReferenceFieldUpdater<UsingWhenSubscriber, Subscription>SUBSCRIPTION =
				AtomicReferenceFieldUpdater.newUpdater(UsingWhenSubscriber.class,
						Subscription.class, "s");

		//rest of the state is always the same
		final S                                           resource;
		final Function<? super S, ? extends Publisher<?>> asyncComplete;
		final Function<? super S, ? extends Publisher<?>> asyncError;
		@Nullable
		final Function<? super S, ? extends Publisher<?>> asyncCancel;

		volatile int                                                wip;
		static final AtomicIntegerFieldUpdater<UsingWhenSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(UsingWhenSubscriber.class, "wip");

		UsingWhenSubscriber(CoreSubscriber<? super T> actual,
				S resource,
				Function<? super S, ? extends Publisher<?>> asyncComplete,
				Function<? super S, ? extends Publisher<?>> asyncError,
				@Nullable Function<? super S, ? extends Publisher<?>> asyncCancel) {
			this.actual = actual;
			this.resource = resource;
			this.asyncComplete = asyncComplete;
			this.asyncError = asyncError;
			this.asyncCancel = asyncCancel;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED || key == Attr.CANCELLED) return wip == 1;
			if (key == Attr.PARENT) return s;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void request(long l) {
			if (Operators.validate(l)) {
				s.request(l);
			}
		}

		@Override
		public void cancel() {
			if (Operators.terminate(SUBSCRIPTION, this)) {
				try {
					if (asyncCancel != null) {
						//FIXME better integration of asyncCancel?
						Flux.from(asyncCancel.apply(resource))
						    .subscribe(v -> {},
								error -> Loggers.getLogger(FluxUsingWhen.class).warn("Async resource cleanup failed after cancel", error));
					}
					else {
						//FIXME should there be a default to the "complete" path on cancellation, or NO-OP?
						Flux.from(asyncComplete.apply(resource))
						    .subscribe(v -> {},
								error -> Loggers.getLogger(FluxUsingWhen.class).warn("Async resource cleanup failed after cancel", error));
					}
				}
				catch (Throwable error) {
					Loggers.getLogger(FluxUsingWhen.class).warn("Error generating async resource cleanup during onCancel", error);
				}
			}
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			Publisher<?> p;

			try {
				p = Objects.requireNonNull(asyncError.apply(resource),
						"The asyncError returned a null Publisher");
			}
			catch (Throwable e) {
				Throwable _e = Operators.onOperatorError(e, actual.currentContext());
				_e = Exceptions.addSuppressed(_e, t);
				actual.onError(_e);
				return;
			}

			p.subscribe(new RollbackInner(this, t));
		}

		@Override
		public void onComplete() {
			Publisher<?> p;

			try {
				p = Objects.requireNonNull(asyncComplete.apply(resource),
						"The asyncComplete returned a null Publisher");
			}
			catch (Throwable e) {
				Throwable _e = Operators.onOperatorError(e, actual.currentContext());
				actual.onError(_e);
				return;
			}

			p.subscribe(new CommitInner(this));
		}

		// below methods have changes in the different implementations

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			return NONE; // always reject, upstream turned out to be non-fuseable after all
		}

		@Override
		public void clear() {
			// ignoring fusion methods
		}

		@Override
		public boolean isEmpty() {
			// ignoring fusion methods
			return true;
		}

		@Override
		@Nullable
		public T poll() {
			return null;
		}

		@Override
		public int size() {
			return 0;
		}
	}

	static final class UsingWhenConditionalSubscriber<T, S>
			extends UsingWhenSubscriber<T, S>
			implements ConditionalSubscriber<T> {

		final ConditionalSubscriber<? super T>            actual;

		UsingWhenConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				S resource,
				Function<? super S, ? extends Publisher<?>> asyncComplete,
				Function<? super S, ? extends Publisher<?>> asyncError,
				@Nullable Function<? super S, ? extends Publisher<?>> asyncCancel) {
			super(actual, resource, asyncComplete, asyncError, asyncCancel);
			this.actual = actual;
		}

		@Override
		public boolean tryOnNext(T t) {
			return actual.tryOnNext(t);
		}
	}

	static final class UsingWhenFuseableSubscriber<T, S> implements InnerOperator<T, T>,
	                                                                QueueSubscription<T> {

		//state that differs in the different variants
		int mode;
		final CoreSubscriber<? super T>                                                         actual;
		volatile     QueueSubscription<T>                                                       qs;
		static final AtomicReferenceFieldUpdater<UsingWhenFuseableSubscriber, QueueSubscription>SUBSCRIPTION =
				AtomicReferenceFieldUpdater.newUpdater(UsingWhenFuseableSubscriber.class,
						QueueSubscription.class, "qs");

		//rest of the state is same as UsingAsyncSubscriber
		final S                                           resource;
		final Function<? super S, ? extends Publisher<?>> asyncComplete;
		final Function<? super S, ? extends Publisher<?>> asyncError;
		@Nullable
		final Function<? super S, ? extends Publisher<?>> asyncCancel;
		volatile int                                      wip;

		static final AtomicIntegerFieldUpdater<UsingWhenFuseableSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(UsingWhenFuseableSubscriber.class, "wip");

		UsingWhenFuseableSubscriber(CoreSubscriber<? super T> actual,
				S resource,
				Function<? super S, ? extends Publisher<?>> asyncComplete,
				Function<? super S, ? extends Publisher<?>> asyncError,
				@Nullable Function<? super S, ? extends Publisher<?>> asyncCancel) {
			this.actual = actual;
			this.resource = resource;
			this.asyncComplete = asyncComplete;
			this.asyncError = asyncError;
			this.asyncCancel = asyncCancel;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED || key == Attr.CANCELLED) return wip == 1;
			if (key == Attr.PARENT) return qs;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void request(long l) {
			if (Operators.validate(l)) {
				qs.request(l);
			}
		}

		@Override
		public void cancel() {
			QueueSubscription a = SUBSCRIPTION.get(this);
			if (a != null) {
				a = SUBSCRIPTION.getAndSet(this, null);
				if (a != null) {
					a.cancel();

					try {
						if (asyncCancel != null) {
							//FIXME better integration of asyncCancel?
							Flux.from(asyncCancel.apply(resource))
							    .subscribe(v -> {},
									    error -> Loggers.getLogger(FluxUsingWhen.class).warn("Async resource cleanup failed after cancel", error));
						}
						else {
							//FIXME should there be a default to the "complete" path on cancellation, or NO-OP?
							Flux.from(asyncComplete.apply(resource))
							    .subscribe(v -> {},
									    error -> Loggers.getLogger(FluxUsingWhen.class).warn("Async resource cleanup failed after cancel", error));
						}
					}
					catch (Throwable error) {
						Loggers.getLogger(FluxUsingWhen.class).warn("Error generating async resource cleanup during onCancel", error);
					}
				}
				//else already cancelled
			}
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			Publisher<?> p;

			try {
				p = Objects.requireNonNull(asyncError.apply(resource),
						"The asyncError returned a null Publisher");
			}
			catch (Throwable e) {
				Throwable _e = Operators.onOperatorError(e, actual.currentContext());
				_e = Exceptions.addSuppressed(_e, t);
				actual.onError(_e);
				return;
			}

			p.subscribe(new RollbackInner(this, t));
		}

		@Override
		public void onComplete() {
			Publisher<?> p;

			try {
				p = Objects.requireNonNull(asyncComplete.apply(resource),
						"The asyncComplete returned a null Publisher");
			}
			catch (Throwable e) {
				Throwable _e = Operators.onOperatorError(e, actual.currentContext());
				actual.onError(_e);
				return;
			}

			p.subscribe(new CommitInner(this));
		}

		// below are methods differing from UsingAsyncSubscriber

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.qs, s)) {
				//noinspection unchecked
				this.qs = (QueueSubscription<T>) s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void clear() {
			qs.clear();
		}

		@Override
		public boolean isEmpty() {
			return qs.isEmpty();
		}

		@Override
		public int size() {
			return qs.size();
		}

		@Override
		@Nullable
		public T poll() {
			T v = qs.poll();

			if (v == null && mode == SYNC) {
				if (WIP.compareAndSet(this, 0, 1)) {
					Flux.from(asyncComplete.apply(resource))
					    .subscribe(it -> {},
							    e -> Loggers.getLogger(FluxUsingWhen.class).warn("Async resource cleanup failed after poll", e));
				}
			}
			return v;
		}

		@Override
		public int requestFusion(int requestedMode) {
			int m = qs.requestFusion(requestedMode);
			this.mode = m;
			return m;
		}
	}

	static final class RollbackInner implements InnerConsumer<Object> {

		final InnerOperator        parent;
		final Throwable            rollbackCause;

		RollbackInner(InnerOperator ts, Throwable rollbackCause) {
			this.parent = ts;
			this.rollbackCause = rollbackCause;
		}

		@Override
		public void onSubscribe(Subscription s) {
			Objects.requireNonNull(s, "Subscription cannot be null")
			       .request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Object o) {
			//NO-OP
		}

		@Override
		public void onError(Throwable e) {
			RuntimeException rollbackError = new RuntimeException("Async resource cleanup failed after onError", e);
			parent.actual().onError(Exceptions.addSuppressed(rollbackError, rollbackCause));
		}

		@Override
		public void onComplete() {
			parent.actual().onError(rollbackCause);
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return parent;
			if (key == Attr.ACTUAL) return parent.actual();
			if (key == Attr.ERROR) return rollbackCause;

			return null;
		}
	}

	static final class CommitInner implements InnerConsumer<Object> {

		final InnerOperator parent;

		CommitInner(InnerOperator ts) {
			this.parent = ts;
		}

		@Override
		public void onSubscribe(Subscription s) {
			Objects.requireNonNull(s, "Subscription cannot be null")
			       .request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Object o) {
			//NO-OP
		}

		@Override
		public void onError(Throwable e) {
			Throwable e_ = Operators.onOperatorError(e, parent.actual().currentContext());
			Throwable commitError = new RuntimeException("Async resource cleanup failed after onComplete", e_);
			parent.actual().onError(commitError);
		}

		@Override
		public void onComplete() {
			parent.actual().onComplete();
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return parent;
			if (key == Attr.ACTUAL) return parent.actual();

			return null;
		}
	}
}
