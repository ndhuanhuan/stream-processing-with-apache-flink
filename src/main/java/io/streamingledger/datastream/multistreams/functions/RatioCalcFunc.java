package io.streamingledger.datastream.multistreams.functions;

import io.streamingledger.models.Transaction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * RichCoFlatMapFunction processes two different input streams (Connected Streams).
 * The "Co" stands for "Connected" - it allows processing of two streams with shared state.
 * 
 * Type parameters:
 * - First Transaction: Type of first input stream (debit transactions)
 * - Second Transaction: Type of second input stream (credit transactions)
 * - String: Output type
 * 
 * Usage example:
 * debits.connect(credits).flatMap(new RatioCalcFunc()).print();
 */
public class RatioCalcFunc
        extends RichCoFlatMapFunction<Transaction, Transaction, String> {
    // Shared state accessible by both flatMap1 and flatMap2
    private int totalDebitsCount = 0;
    private int totalCreditsCount = 0;


    /**
     * flatMap1 processes elements from the FIRST connected stream (debit transactions).
     * This method is called automatically when an element arrives from the first stream.
     */
    @Override
    public void flatMap1(Transaction transaction, Collector<String> collector) throws Exception {
        totalDebitsCount += 1;
        double ratio =
                totalDebitsCount * 100.0 / (totalCreditsCount + totalDebitsCount);
        collector.collect(String.format("Total debits ratio so far: %s", ratio));
    }

    /**
     * flatMap2 processes elements from the SECOND connected stream (credit transactions).
     * This method is called automatically when an element arrives from the second stream.
     */
    @Override
    public void flatMap2(Transaction transaction, Collector<String> collector) throws Exception {
        totalCreditsCount += 1;
        double ratio =
                totalCreditsCount * 100.0 / (totalCreditsCount + totalDebitsCount);
        collector.collect(String.format("Total credits ratio so far: %s", ratio));
    }
}