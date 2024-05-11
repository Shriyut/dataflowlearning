package org.learning.beam.basic;

import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class WithReadDelay<T> extends PTransform<PCollection<T>, PCollection<T>> {

    static <T> WithReadDelay<T> ofProcessingTime(Duration duration) {
        return new WithReadDelay<>(duration);
    }

    private final Duration delay;

    WithReadDelay(Duration delay) {
        this.delay = delay;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        return input
                .apply(WithKeys.of(""))
                .apply(ParDo.of(new DelayFn<>(delay)))
                .apply(MapElements.into(input.getTypeDescriptor()).via(KV::getValue));
    }

    private static class DelayFn<T> extends DoFn<T, T> {

        @StateId("dummy")
        final StateSpec<ValueState<Void>> state = StateSpecs.value();

        private final Duration delay;

        public DelayFn(Duration delay) {
            this.delay = delay;
        }

        @ProcessElement
        public void process(@Element T elem, OutputReceiver<T> out) {
            try {
                TimeUnit.MILLISECONDS.sleep(delay.getMillis());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            out.output(elem);
        }
    }
}
