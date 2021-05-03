package reactor;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class FluxPrefetchOverheadBenchmark {

	//	"0"
	@Param({"10", "100", "1000", "100000"})
	public int sourceSize;

	@Param({"NONE", "SYNC", "ASYNC"})
	public String sourceType;

	// @Param()
	// computations - false
	// computations - true

	// Add "Batch"
	@Param({"One", "Unbound"})
	public String subscriberType;

	Flux<Integer> publishOnOldFlux;
	Flux<Integer> publishOnFlux;
	Flux<Integer> publishOnWithPrefetchFlux;
	Flux<Integer> prefetchFlux;

	BiFunction<CountDownLatch, Blackhole, Subscriber<Integer>> subscriberSupplier;

	@Setup
	public void setup() {
		Flux<Integer> dataSource = getDataSource(sourceSize, sourceType);

		publishOnOldFlux = dataSource.publishOnOld(Schedulers.immediate());
		publishOnFlux = dataSource.publishOn(Schedulers.immediate());
//		TODO: requestMode
		publishOnWithPrefetchFlux = dataSource.prefetch()
		                                      .publishOn(Schedulers.immediate());
		prefetchFlux = dataSource.prefetch();

		subscriberSupplier = getSubscriberSupplier(subscriberType);
	}

	private Flux<Integer> getDataSource(int sourceSize, String sourceType) {
		Integer[] array = new Integer[sourceSize];
		Arrays.fill(array, 666);

		Flux<Integer> publisher = Flux.fromArray(array);

		switch (sourceType) {
			case "ASYNC":
				return publisher.onBackpressureBuffer();
			case "SYNC":
				return publisher;
			default:
				return publisher.hide();
		}
	}

	private BiFunction<CountDownLatch, Blackhole, Subscriber<Integer>> getSubscriberSupplier(
			String subscriberType) {
		return (CountDownLatch latch, Blackhole bh) -> {
			switch (subscriberType) {
				case ("One"):
					return new BenchmarkSubscriber(bh, latch) {
						@Override
						protected void hookOnSubscribe(Subscription s) {
							request(1);
						}

						@Override
						protected void hookOnNext(Integer v) {
							super.hookOnNext(v);
							request(1);
						}
					};
				case ("Unbound"):
				default:
					return new BenchmarkSubscriber(bh, latch) {
						@Override
						protected void hookOnSubscribe(Subscription s) {
							request(Long.MAX_VALUE);
						}
					};
			}
		};
	}

	@Benchmark
	public void oldPublishOnPerformance(Blackhole bh) throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		Subscriber<Integer> s = subscriberSupplier.apply(latch, bh);
		publishOnOldFlux.subscribe(s);
		latch.await();
	}

	@Benchmark
	public void newPublishOnPerformance(Blackhole bh) throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		Subscriber<Integer> s = subscriberSupplier.apply(latch, bh);
		publishOnFlux.subscribe(s);
		latch.await();
	}

	@Benchmark
	public void newPublishOnWithPrefetchFlux(Blackhole bh) throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		Subscriber<Integer> s = subscriberSupplier.apply(latch, bh);
		publishOnWithPrefetchFlux.subscribe(s);
		latch.await();
	}

	@Benchmark
	public void prefetchFlux(Blackhole bh) throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		Subscriber<Integer> s = subscriberSupplier.apply(latch, bh);
		prefetchFlux.subscribe(s);
		latch.await();
	}

	static class BenchmarkSubscriber extends BaseSubscriber<Integer> {

		final Blackhole      blackhole;
		final CountDownLatch latch;

		BenchmarkSubscriber(Blackhole blackhole, CountDownLatch latch) {
			this.blackhole = blackhole;
			this.latch = latch;
		}

		@Override
		protected void hookOnNext(Integer v) {
			blackhole.consume(v);
		}

		@Override
		protected void hookOnError(Throwable throwable) {
			blackhole.consume(throwable);
			latch.countDown();
		}

		@Override
		protected void hookOnComplete() {
			blackhole.consume(true);
			latch.countDown();
		}
	}
}
