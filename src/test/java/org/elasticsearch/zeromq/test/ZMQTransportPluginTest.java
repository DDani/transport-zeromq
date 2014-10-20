package org.elasticsearch.zeromq.test;



import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Date;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.zeromq.ZMQSocket;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeromq.ZMQ;

import uk.co.datumedge.hamcrest.json.SameJSONAs;

public class ZMQTransportPluginTest {

    private static Node node = null;

    private static ZMQ.Context context = null;

    /*
     * ØMQ Socket binding adress, must be coherent with elasticsearch.yml config file
     */
    private static final String address = "tcp://localhost:9800";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // Instantiate an ES server
        node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put("es.config", "elasticsearch.yml")).node();
        
        Thread.sleep(1000L);

        // Instantiate a ZMQ context
        context = ZMQ.context(1);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
      
        try {
            context.close();
            context.term();
        } catch (Exception e2) {
            // ignore
        }
        
        if (node != null) {
            Thread.sleep(1000L);
            node.close();
        }

    }

    /**
     * Simple method to send & receive zeromq message
     * 
     * @param method
     * @param uri
     * @param json
     * @return
     * @throws JSONException 
     */
    private JSONResult sendAndReceive(String method, String uri, String json) throws JSONException {

        ZMQ.Socket socket = context.socket(ZMQ.DEALER);
        socket.connect(address);

        // Handshake
        try {
            Thread.sleep(100);
        } catch (Exception e) {
            Assert.fail("Handshake failed");
        }

        StringBuilder sb = new StringBuilder(method);
        sb.append(ZMQSocket.SEPARATOR).append(uri).append(ZMQSocket.SEPARATOR);

        if (json != null) {
            sb.append(json);
        }

        String result = null;
        try {
            socket.send(sb.toString().getBytes("UTF-8"), 0);

            byte[] response = socket.recv(0);
            result = new String(response, Charset.forName("UTF-8"));

        } catch (UnsupportedEncodingException e) {
            Assert.fail("Exception when sending/receiving message");
        } finally {
            try {
                socket.close();
            } catch (Exception e2) {
                // ignore
            }
        }
        
        int firstSeparator = result.indexOf("|");
        int secondSeparator = result.indexOf("|",  firstSeparator + 1);
        
        
        String statusCode = result.substring(0, firstSeparator);
        String statusMessage = result.substring(firstSeparator + 1, secondSeparator);
        String jsonMessage = result.substring(secondSeparator + 1);
        
        return new JSONResult(new JSONObject(jsonMessage), statusMessage, statusCode);
    }


    @Test
    public void testDeleteMissingIndex() throws JSONException {

        JSONObject expected = new JSONObject("{\"error\":\"IndexMissingException[[test-index-missing] missing]\",\"status\":404}");

        JSONResult response = sendAndReceive("DELETE", "/test-index-missing/", null);
        
        
        Assert.assertEquals("404", response.getStatusCode());
        Assert.assertEquals("NOT_FOUND", response.getStatusMessage());

        assertThat(expected, SameJSONAs.sameJSONObjectAs(response.getJson()));


    }

    @Test
    public void testCreateIndex() throws JSONException {
        JSONResult response = sendAndReceive("DELETE", "/books/", null);
        Assert.assertNotNull(response);
        

        response = sendAndReceive("PUT", "/books/", null);
        
        
        Assert.assertEquals("200", response.getStatusCode());
        Assert.assertEquals("OK", response.getStatusMessage());
        
        JSONObject expected = new JSONObject("{\"acknowledged\":true}");

        assertThat(expected, SameJSONAs.sameJSONObjectAs(response.getJson()));
        
        
    }

    
    @Test
    public void testMapping() throws IOException, JSONException {
        XContentBuilder mapping =
            jsonBuilder().startObject().startObject("book").startObject("properties").startObject("title").field("type", "string")
                .field("analyzer", "french").endObject().startObject("author").field("type", "string").endObject().startObject("year")
                .field("type", "integer").endObject().startObject("publishedDate").field("type", "date").endObject().endObject().endObject()
                .endObject();

        JSONResult response = sendAndReceive("PUT", "/books/book/_mapping", mapping.string());
        
        Assert.assertEquals("200", response.getStatusCode());
        Assert.assertEquals("OK", response.getStatusMessage());
        
        JSONObject expected = new JSONObject("{\"acknowledged\":true}");

        assertThat(expected, SameJSONAs.sameJSONObjectAs(response.getJson()));
        
     

    }
    
    

    @Test
    public void testIndex() throws IOException, JSONException {
        
        XContentBuilder book1 =
            jsonBuilder().startObject().field("title", "Les Misérables").field("author", "Victor Hugo").field("year", "1862")
                .field("publishedDate", new Date()).endObject();
        JSONResult response = sendAndReceive("PUT", "/books/book/1", book1.string());
        Assert.assertEquals("201", response.getStatusCode());
        Assert.assertEquals("CREATED", response.getStatusMessage());
        JSONObject expected = new JSONObject("{\"_index\":\"books\",\"_type\":\"book\",\"_id\":\"1\",\"_version\":1,\"created\":true}");
        assertThat(expected, SameJSONAs.sameJSONObjectAs(response.getJson()));




        XContentBuilder book2 =
            jsonBuilder().startObject().field("title", "Notre-Dame de Paris").field("author", "Victor Hugo").field("year", "1831")
                .field("publishedDate", new Date()).endObject();

        response = sendAndReceive("PUT", "/books/book/2", book2.string());
        Assert.assertEquals("201", response.getStatusCode());
        Assert.assertEquals("CREATED", response.getStatusMessage());
        expected = new JSONObject("{\"created\":true,\"_index\":\"books\",\"_type\":\"book\",\"_id\":\"2\",\"_version\":1}");
        assertThat(expected, SameJSONAs.sameJSONObjectAs(response.getJson()));
        
        
        

        XContentBuilder book3 =
            jsonBuilder().startObject().field("title", "Le Dernier Jour d'un condamné").field("author", "Victor Hugo").field("year", "1829")
                .field("publishedDate", new Date()).endObject();

        response = sendAndReceive("POST", "/books/book", book3.string());
        Assert.assertNotNull("Response should not be null", response.getJson());
        Assert.assertEquals("201", response.getStatusCode());
        Assert.assertEquals("CREATED", response.getStatusMessage());
        
        Assert.assertTrue( response.getJson().has("_id"));
        
      
        
    }

    @Test
    public void testRefresh() throws IOException, JSONException {
        JSONResult response = sendAndReceive("GET", "/_all/_refresh", null);
       
        Assert.assertEquals("200", response.getStatusCode());
        Assert.assertEquals("OK", response.getStatusMessage());
        
        JSONObject expected = new JSONObject("{\"_shards\":{\"total\":10,\"failed\":0,\"successful\":5}}");

        assertThat(expected, SameJSONAs.sameJSONObjectAs(response.getJson()));
        
        
    }
/*
    @Test
    public void testSearch() throws IOException {
        String response = sendAndReceive("GET", "/_all/_search", "{\"query\":{\"match_all\":{}}}");
        Assert.assertTrue(response.contains("\"hits\":{\"total\":3"));

        response =
            sendAndReceive(
                "GET",
                "_search",
                "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"year\":{\"gte\":1820,\"lte\":1832}}}],\"must_not\":[],\"should\":[]}},\"from\":0,\"size\":50,\"sort\":[],\"facets\":{},\"version\":true}:");
        Assert.assertTrue(response.contains("\"hits\":{\"total\":2"));
    }
*/
    @Test
    public void testGet() throws IOException, JSONException {
        
        XContentBuilder book2 =
            jsonBuilder().startObject().field("title", "Notre-Dame de Paris").field("author", "Victor Hugo").field("year", "1831")
                .field("publishedDate", new Date()).endObject();

        JSONResult response = sendAndReceive("PUT", "/books/book/2", book2.string());
        
        response = sendAndReceive("GET", "/books/book/2", null);
        Assert.assertTrue(response.getJson().toString().contains("Notre-Dame de Paris"));
    }
    
    
    public class JSONResult {
        private final JSONObject json;
        
        private final String statusMessage;

        private String statusCode;
        
        public JSONResult(JSONObject json, String statusMessage, String statusCode) {
            this.json = json;
            this.statusMessage = statusMessage;
            this.statusCode = statusCode;
        }
        
        public JSONObject getJson() {
            return json;
        }

        public String getStatusMessage() {
            return statusMessage;
        }

        public String getStatusCode() {
            return statusCode;
        }

        public void setStatusCode(String statusCode) {
            this.statusCode = statusCode;
        }

    }
}
