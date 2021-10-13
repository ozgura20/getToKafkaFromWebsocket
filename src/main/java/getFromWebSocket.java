
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft;
import org.java_websocket.handshake.ServerHandshake;

public class getFromWebSocket extends WebSocketClient {
    public getFromWebSocket(URI serverUri, Draft draft) {
        super(serverUri, draft);
    }

    public getFromWebSocket(URI serverURI) {
        super(serverURI);
    }

    public getFromWebSocket(URI serverUri, Map<String, String> httpHeaders) {
        super(serverUri, httpHeaders);
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        send("{\n" +
                "    \"type\": \"subscribe\",\n" +
                "    \"product_ids\": [\n" +
                "        \"ETH-USD\",\n" +
                "        \"ETH-EUR\"\n" +
                "    ],\n" +
                "    \"channels\": [\n" +
                "        \"level2\",\n" +
                "        \"heartbeat\",\n" +
                "        {\n" +
                "            \"name\": \"ticker\",\n" +
                "            \"product_ids\": [\n" +
                "                \"ETH-BTC\",\n" +
                "                \"ETH-USD\"\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}");
        System.out.println("opened connection");
        // if you plan to refuse connection based on ip or httpfields overload: onWebsocketHandshakeReceivedAsClient
    }

    @Override
    public void onMessage(String message) {
        kafkaProducer.send(new ProducerRecord("sahi", message));
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {

        // The codecodes are documented in class org.java_websocket.framing.CloseFrame
        System.out.println(
                "Connection closed by " + (remote ? "remote peer" : "us") + " Code: " + code + " Reason: "
                        + reason);
        kafkaProducer.close();
    }

    @Override
    public void onError(Exception ex) {
        kafkaProducer.close();
        ex.printStackTrace();
        // if the error is fatal then onClose will be called additionally
    }

    // create object of kafka
    static Properties properties = new Properties();
    static KafkaProducer kafkaProducer;

    public static void main(String[] args) throws URISyntaxException {

        //define kafka connection
        properties.put("bootstrap.servers", "Host:port");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer(properties);


        getFromWebSocket c = new getFromWebSocket(new URI( "wss://ws-feed.exchange.coinbase.com") );
        c.connect();
    }
}
