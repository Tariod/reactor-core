package reactor;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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

	@Param({"10", "100", "1000", "100000"})
	public int sourceSize;

	@Param({"NONE", "SYNC", "ASYNC"})
	public String sourceType;

	@Param({"true", "false"})
	public boolean prefetchMode;

	@Param({"One", "Unbound"})
	public String subscriberType;

	Flux<Integer> publishOnOldFlux;
	Flux<Integer> publishOnFlux;
	Flux<Integer> publishOnWithPrefetchFlux;
	Flux<Integer> prefetchFlux;

	Function<Blackhole, Subscriber<Integer>> subscriberSupplier;

	@Setup
	public void setup() {
		Flux<Integer> dataSource = getDataSource(sourceSize, sourceType);

		publishOnOldFlux = dataSource.publishOnOld(Schedulers.immediate());
		publishOnFlux = dataSource.publishOn(Schedulers.immediate());
		publishOnWithPrefetchFlux = dataSource.prefetch(prefetchMode)
		                                      .publishOn(Schedulers.immediate());
		prefetchFlux = dataSource.prefetch(prefetchMode);

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
			case "NONE":
			default:
				return publisher.hide();
		}
	}

	private Function<Blackhole, Subscriber<Integer>> getSubscriberSupplier(String subscriberType) {
		return (Blackhole bh) -> {
			switch (subscriberType) {
				case ("One"):
					return new BenchmarkSubscriber(bh) {
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
					return new BenchmarkSubscriber(bh) {
						@Override
						protected void hookOnSubscribe(Subscription s) {
							request(Long.MAX_VALUE);
						}
					};
			}
		};
	}

	@Benchmark
	public void oldPublishOnPerformance(Blackhole bh) {
		Subscriber<Integer> s = subscriberSupplier.apply(bh);
		publishOnOldFlux.subscribe(s);
	}

	@Benchmark
	public void newPublishOnPerformance(Blackhole bh) {
		Subscriber<Integer> s = subscriberSupplier.apply(bh);
		publishOnFlux.subscribe(s);
	}

	@Benchmark
	public void newPublishOnWithPrefetchFlux(Blackhole bh) {
		Subscriber<Integer> s = subscriberSupplier.apply(bh);
		publishOnWithPrefetchFlux.subscribe(s);
	}

	@Benchmark
	public void prefetchFlux(Blackhole bh) {
		Subscriber<Integer> s = subscriberSupplier.apply(bh);
		prefetchFlux.subscribe(s);
	}

	static class BenchmarkSubscriber extends BaseSubscriber<Integer> {

		final Blackhole blackhole;

		BenchmarkSubscriber(Blackhole blackhole) {
			this.blackhole = blackhole;
		}

		@Override
		protected void hookOnNext(Integer v) {
			blackhole.consume(v);
		}

		@Override
		protected void hookOnError(Throwable throwable) {
			blackhole.consume(throwable);
		}

		@Override
		protected void hookOnComplete() {
			blackhole.consume(true);
		}
	}
}
