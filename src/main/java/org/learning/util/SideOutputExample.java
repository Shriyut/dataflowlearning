package org.learning.util;

//import org.apache.beam.sdk.Pipeline;
//import org.apache.beam.sdk.transforms.DoFn;
//import org.apache.beam.sdk.transforms.ParDo;
//import org.apache.beam.sdk.values.KV;
//import org.apache.beam.sdk.values.PCollection;
//import org.apache.beam.sdk.values.PCollectionTuple;
//import org.apache.beam.sdk.values.TupleTag;
//import org.apache.beam.sdk.values.TupleTagList;
//
//public class SideOutputExample {
//
//    // Define the TupleTag for the side output
//    static final TupleTag<Integer> evenNumbersTag = new TupleTag<Integer>(){};
//    static final TupleTag<KV<Integer, Integer>> mainOutputTag = new TupleTag<KV<Integer, Integer>>(){};
//
//    public static void main(String[] args) {
//        Pipeline pipeline = Pipeline.create();
//
//        // Create a sample PCollection of integers
//        PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
//
//        // Apply the DoFn with side output
//        PCollectionTuple results = input.apply("ProcessIntegers", ParDo.of(new DoFn<Integer, KV<Integer, Integer>>() {
//            @ProcessElement
//            public void processElement(ProcessContext context) {
//                Integer number = context.element();
//                if (number % 2 == 0) {
//                    // Output even numbers to the side output
//                    context.output(evenNumbersTag, number);
//                }
//                // Output KV with the integer and its square to the main output
//                context.output(mainOutputTag, KV.of(number, number * number));
//            }
//        }).withOutputTags(mainOutputTag, TupleTagList.of(evenNumbersTag)));
//
//        // Retrieve the main output and side output
//        PCollection<KV<Integer, Integer>> mainOutput = results.get(mainOutputTag);
//        PCollection<Integer> evenNumbers = results.get(evenNumbersTag);
//
//        // Further processing of mainOutput and evenNumbers...
//
//        pipeline.run().waitUntilFinish();
//    }
//}
