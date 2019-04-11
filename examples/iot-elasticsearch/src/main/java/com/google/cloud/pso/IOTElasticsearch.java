package com.google.cloud.pso;

import com.google.cloud.pso.coders.ErrorMessageCoder;
import com.google.cloud.pso.common.ErrorMessage;
import com.google.cloud.pso.common.ExtractKeyFn;
import com.google.cloud.pso.common.FailSafeValidate;
import com.google.cloud.pso.options.IOTElasticsearchOptions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/*
mvn compile exec:java -Dexec.mainClass=com.google.cloud.pso.IOTElasticsearch \
-Dexec.args="--runner=DataflowRunner \
--project=$PROJECT \
--stagingLocation=$GS_STAGING \
--gcpTempLocation=$GS_TMP \
--inputSubscription=$PUBSUB_SUBSCRIPTION \
--rejectionTopic=$PUBSUB_REJECT_TOPIC \
--addresses=$ELASTIC_CLUSTER \
--index=$ELASTIC_INDEX \
--type=$ELASTIC_TYPE \
--idField=$ELASTIC_ID"
 */
public class IOTElasticsearch {
    /**
     * {@link TupleTag> to tag succesfully validated
     * messages.
     */
    private static final TupleTag<KV<String, String>> VALIDATION_SUCCESS_TAG =
            new TupleTag<KV<String, String>>() {};

    /**
     * {@link TupleTag> to tag failed messages.
     */
    private static final TupleTag<ErrorMessage> FAILURE_TAG = new TupleTag<ErrorMessage>() {};

    public static void main(String[] args) {
        IOTElasticsearchOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(IOTElasticsearchOptions.class);

        run(options);
    }

    public static PipelineResult run(IOTElasticsearchOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        // Set the CustomCoder for the ErrorMessage class.
        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForClass(ErrorMessage.class, ErrorMessageCoder.of());

        String[] addresses =
                Lists.newArrayList(Splitter.on(",").trimResults().split(options.getAddresses()))
                        .toArray(new String[0]);

        ElasticsearchIO.ConnectionConfiguration connection =
                ElasticsearchIO.ConnectionConfiguration.create(
                        addresses, options.getIndex(), options.getType());

        // Run pipeline
        PCollection<String> inputMsgs = pipeline
                .apply("ReadPubsubMessages",
                        PubsubIO.readStrings().fromSubscription(options.getInputSubscription()));

        // Validate messages and tag them with success and failure tags.
        PCollectionTuple validated = inputMsgs
                .apply(
                        "Validate Messages",
                        FailSafeValidate.newBuilder()
                                .withSuccessTag(VALIDATION_SUCCESS_TAG)
                                .withFailureTag(FAILURE_TAG)
                                .withKeyPath(options.getFormattedId())
                                .build());

        validated
                .get(VALIDATION_SUCCESS_TAG)
                .apply("Extract Json from KV", ParDo.of(new ExtractJson()))
                .apply("Write to Elasticsearch Index",
                        ElasticsearchIO.write()
                                .withConnectionConfiguration(connection)
                                .withIdFn(new ExtractKeyFn(options.getFormattedId()))
                                .withUsePartialUpdate(true));


        validated
                .get(FAILURE_TAG)
                .apply(ParDo.of(new DoFn<ErrorMessage, String>() {    // a DoFn as an anonymous inner class instance
                    @ProcessElement
                    public void processElement(@Element ErrorMessage message, OutputReceiver<String> out) {
                        out.output(message.toString());
                    }
                }))
                .apply("Write Failed Messages",
                        PubsubIO.writeStrings().to(options.getRejectionTopic()));

        return pipeline.run();
    }

    static class ExtractJson extends DoFn<KV<String, String>, String> {
        @ProcessElement
        public void processElement(@Element KV<String, String> element, OutputReceiver<String> out) {
            out.output(element.getValue());
        }
    }
}
