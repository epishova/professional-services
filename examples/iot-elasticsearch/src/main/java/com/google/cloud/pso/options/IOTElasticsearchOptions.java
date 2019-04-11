package com.google.cloud.pso.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.*;


public interface IOTElasticsearchOptions extends DataflowPipelineOptions {

    @Description("The Cloud Pub/Sub topic to publish rejected messages to. "
            + "The name should be in the format of "
            + "projects/<project-id>/topics/<topic-name>.")
    @Validation.Required
    String getRejectionTopic();
    void setRejectionTopic(String rejectionTopic);

    @Description("The Cloud Pub/Sub subscription to consume from. "
            + "The name should be in the format of "
            + "projects/<project-id>/subscriptions/<subscription-name>.")
    @Validation.Required
    String getInputSubscription();
    void setInputSubscription(String inputSubscription);

    @Description("Elasticsearch cluster address(es).")
    @Validation.Required
    String getAddresses();
    void setAddresses(String addresses);

    @Description("Name of the Elasticsearch index.")
    @Validation.Required
    String getIndex();
    void setIndex(String index);

    @Description("Name of the Elasticsearch type.")
    @Validation.Required
    String getType();
    void setType(String type);

    @Description(
            "Field in json message which is used as Elasticseaarch document id. "
                    + "This field can be nested using / to identify a path in json message. "
                    + "If no path provided the field is considered as a root node in json message.")
    @Default.String("")
    String getIdField();
    void setIdField(String idField);

    @Description("Returns formatted idField. "
            + "It adds leading / if the user omitted it while specifying idField option.")
    @Hidden
    @Default.InstanceFactory(FormattedIdFactory.class)
    String getFormattedId();
    void setFormattedId(String formattedId);

    /**
     * Returns formatted idField. It adds leading / if the user omitted it while specifying idField option.
     */
    public static class FormattedIdFactory implements DefaultValueFactory<String> {
        @Override
        public String create(PipelineOptions options) {
            String idField = options.as(IOTElasticsearchOptions.class).getIdField();
            if (!idField.isEmpty() && idField.charAt(0) != '/') {
                idField = "/" + idField;
            }
            return idField;
        }
    }
}