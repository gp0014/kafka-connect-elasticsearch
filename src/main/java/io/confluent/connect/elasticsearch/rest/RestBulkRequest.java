package io.confluent.connect.elasticsearch.rest;

import io.confluent.connect.elasticsearch.bulk.BulkRequest;

public class RestBulkRequest implements BulkRequest {

    private final org.elasticsearch.action.bulk.BulkRequest bulk;

    public RestBulkRequest(org.elasticsearch.action.bulk.BulkRequest bulk) {
        this.bulk = bulk;
    }

    public org.elasticsearch.action.bulk.BulkRequest getBulk() {
        return bulk;
    }
}
