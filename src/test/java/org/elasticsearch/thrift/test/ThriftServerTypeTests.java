/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.thrift.test;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.thrift.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.annotations.AfterMethod;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@ElasticsearchIntegrationTest.ClusterScope(transportClientRatio = 0.0, numNodes = 0, scope = ElasticsearchIntegrationTest.Scope.TEST)
public class ThriftServerTypeTests extends ElasticsearchIntegrationTest {

    private Node node;

    private TTransport transport;

    private Rest.Client client;

//    @Before
//    public void beforeTest() throws IOException, TTransportException {
//        transport = new TSocket("localhost", 9500);
//        TProtocol protocol = new TBinaryProtocol(transport);
//        client = new Rest.Client(protocol);
//        transport.open();
//    }

    public void setupThreadPoolServer() throws IOException, TTransportException {
        node = nodeBuilder().settings(settingsBuilder()
                .put("path.data", "target/data")
                .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress())
                .put("gateway.type", "none")
                .put("thrift.server_type", "threadpool")).node();
        transport = new TSocket("localhost", 9500);
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new Rest.Client(protocol);
        transport.open();
    }

    // Currently ES test framework is changed, it will start a test cluster automatically,
    // so we have no chance to start our nodes
    // it means thrift.server_type won't be set, it always null, and get default value threadpool
    public void setupNonblockingServer() throws IOException, TTransportException {
        node = nodeBuilder().settings(settingsBuilder()
                .put("path.data", "target/data")
                .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress())
                .put("gateway.type", "none")
                .put("thrift.frame", "5M")
                .put("thrift.server_type", "nonblocking")).node();

        transport = new TFramedTransport(new TSocket("localhost", 9500));
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new Rest.Client(protocol);
        transport.open();
    }

    @After
    public void afterTest() {
        if (transport != null) transport.close();
        if (node != null) node.close();
    }

    @Test
    public void testThreadPoolServer() throws Exception {
        setupThreadPoolServer();
        RestRequest request = new RestRequest(Method.POST, "/test/type1/1");
        request.setBody(ByteBuffer.wrap(XContentFactory.jsonBuilder().startObject()
                .field("field", "value")
                .endObject().bytes().copyBytesArray().array()));
        RestResponse response = client.execute(request);
        Map<String, Object> map = parseBody(response);
        assertThat(response.getStatus(), equalTo(Status.CREATED));
        assertThat(map.get("_index").toString(), equalTo("test"));
        assertThat(map.get("_type").toString(), equalTo("type1"));
        assertThat(map.get("_id").toString(), equalTo("1"));

        request = new RestRequest(Method.GET, "/test/type1/1");
        response = client.execute(request);
        map = parseBody(response);
        assertThat(response.getStatus(), equalTo(Status.OK));
        assertThat(map.get("_index").toString(), equalTo("test"));
        assertThat(map.get("_type").toString(), equalTo("type1"));
        assertThat(map.get("_id").toString(), equalTo("1"));
        assertThat(map.get("_source"), notNullValue());
        assertThat(map.get("_source"), instanceOf(Map.class));
        assertThat(((Map<String, String>)map.get("_source")).get("field"), is("value"));

        request = new RestRequest(Method.GET, "/_cluster/health");
        response = client.execute(request);
        assertThat(response.getStatus(), equalTo(Status.OK));

        request = new RestRequest(Method.GET, "/bogusindex");
        response = client.execute(request);
        assertThat(response.getStatus(), equalTo(Status.BAD_REQUEST));

    }

    @Test
    public void testNonblockingServer() throws Exception {
        setupNonblockingServer();
        RestRequest request = new RestRequest(Method.POST, "/test/type1/1");
        request.setBody(ByteBuffer.wrap(XContentFactory.jsonBuilder().startObject()
                .field("field", "value")
                .endObject().bytes().copyBytesArray().array()));
        RestResponse response = client.execute(request);
        Map<String, Object> map = parseBody(response);
        assertThat(response.getStatus(), equalTo(Status.CREATED));
        assertThat(map.get("_index").toString(), equalTo("test"));
        assertThat(map.get("_type").toString(), equalTo("type1"));
        assertThat(map.get("_id").toString(), equalTo("1"));

        request = new RestRequest(Method.GET, "/test/type1/1");
        response = client.execute(request);
        map = parseBody(response);
        assertThat(response.getStatus(), equalTo(Status.OK));
        assertThat(map.get("_index").toString(), equalTo("test"));
        assertThat(map.get("_type").toString(), equalTo("type1"));
        assertThat(map.get("_id").toString(), equalTo("1"));
        assertThat(map.get("_source"), notNullValue());
        assertThat(map.get("_source"), instanceOf(Map.class));
        assertThat(((Map<String, String>)map.get("_source")).get("field"), is("value"));

        request = new RestRequest(Method.GET, "/_cluster/health");
        response = client.execute(request);
        assertThat(response.getStatus(), equalTo(Status.OK));

        request = new RestRequest(Method.GET, "/bogusindex");
        response = client.execute(request);
        assertThat(response.getStatus(), equalTo(Status.BAD_REQUEST));

    }

    private Map<String, Object> parseBody(RestResponse response) throws IOException {
        return XContentFactory.xContent(XContentType.JSON).createParser(response.bufferForBody().array(), response.bufferForBody().arrayOffset(), response.bufferForBody().remaining()).map();
    }

}
