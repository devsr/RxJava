package rx.internal.operators;

import java.util.concurrent.atomic.AtomicLong;

import rx.Observable.Operator;
import rx.Producer;
import rx.Scheduler;
import rx.Subscriber;
import rx.exceptions.MissingBackpressureException;
import rx.functions.Action0;
import rx.internal.operators.BackpressureUtils;
import rx.internal.util.RxRingBuffer;

/**
 * Prevents any upstream or downstream operator from pinning a given scheduler.
 * 
 * This behavior is enforced by forcing reactive-pull requests to be dispatched
 * via the protected scheduler. This relieves a pinned scheduler by starving the
 * offending operator.
 *
 * @param <T>
 *            the value type.
 */
public class OperatorFairScheduling<T> implements Operator<T, T> {

    private final Scheduler scheduler;
    private final long maxEmits;

    public OperatorFairScheduling(Scheduler scheduler) {
        this(scheduler, RxRingBuffer.SIZE);
    }

    public OperatorFairScheduling(Scheduler scheduler, long maxEmits) {
        this.scheduler = scheduler;
        this.maxEmits = maxEmits;
        if (maxEmits <= 0) {
            throw new IllegalArgumentException("maxEmits must be greater than zero: " + maxEmits);
        }
    }

    @Override
    public Subscriber<? super T> call(Subscriber<? super T> child) {
        FairThreadingOnSubscriber<? super T> subscriber = new FairThreadingOnSubscriber<T>(child, scheduler, maxEmits);
        subscriber.init();
        return subscriber;
    }

    private static class FairThreadingOnSubscriber<T> extends Subscriber<T> implements Action0 {

        private final Subscriber<? super T> child;
        private final Scheduler.Worker worker;
        private final long maxEmits;

        private final long emitThreshold;

        private final AtomicLong requested = new AtomicLong();

        private final AtomicLong emitsRemaining = new AtomicLong();

        private volatile boolean requestPending;

        public FairThreadingOnSubscriber(Subscriber<? super T> child, Scheduler scheduler, long maxEmits) {
            super(child);
            this.child = child;
            this.worker = scheduler.createWorker();
            this.maxEmits = maxEmits;
            this.emitThreshold = maxEmits - (maxEmits >> 1);
            request(0); // Wait for upstream requests.
        }

        public void init() {

            child.setProducer(new Producer() {

                @Override
                public void request(long n) {
                    /* Add to undispatched request count. */
                    BackpressureUtils.getAndAddRequest(requested, n);

                    /* Schedule to dispatch the request. */
                    worker.schedule(FairThreadingOnSubscriber.this);
                }
            });

            child.add(worker);
        }

        @Override
        public void onCompleted() {
            if (!child.isUnsubscribed()) {
                child.onCompleted();
            }
        }

        @Override
        public void onError(Throwable e) {
            if (!child.isUnsubscribed()) {
                child.onError(e);
            }
        }

        @Override
        public void onNext(T t) {
            if (!child.isUnsubscribed()) {

                long er = emitsRemaining.decrementAndGet();

                /* Check that backpressure is being honored. */
                if (er < 0) {
                    onError(new MissingBackpressureException());
                    return;
                }

                /*
                 * Compare value read from emitsRemaining with threshold.
                 * Dispatch a request if emitsRemaining is too low.
                 * 
                 * requestPending is a volatile read.
                 */
                if (er < emitThreshold && !requestPending) {
                    requestPending = true;
                    worker.schedule(this);
                }

                child.onNext(t);
            }
        }

        @Override
        public void call() {

            final AtomicLong localRequested = requested;
            final AtomicLong localEmitsRemaining = emitsRemaining;
            final long localMaxEmits = maxEmits;

            /* Loop over the atomic operation until it is successful. */
            for (;;) {
                long er = localEmitsRemaining.get();

                /* Calculate request deficit. */
                long request = localMaxEmits - er;

                /*
                 * Compare calculation with number of undispatched requests, use
                 * lowest.
                 */
                request = Math.min(request, localRequested.get());

                /*
                 * If request is zero, there is nothing to be done. We are
                 * leaving the requestPending flag set on purpose as presumably
                 * there must be a request coming from upstream eventually.
                 */
                if (request == 0) {
                    return;
                }

                /* Add request to emitsRemaining atomically. */
                if (localEmitsRemaining.compareAndSet(er, er + request)) {

                    /* Reduce undispatched request count. */
                    BackpressureUtils.produced(localRequested, request);

                    /*
                     * Ordering is important here, emitsRemaining could already
                     * be draining due to concurrency. The requestPending clear
                     * needs to happen before request(int) is called.
                     */
                    requestPending = false;
                    request(request);
                    return;
                }
            }
        }
    }
}
