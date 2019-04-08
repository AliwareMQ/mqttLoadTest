package test;

import com.alibaba.fastjson.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.util.concurrent.Future;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import nl.jk5.mqtt.MqttClient;
import nl.jk5.mqtt.MqttClientConfig;
import nl.jk5.mqtt.MqttClientImpl;
import nl.jk5.mqtt.MqttConnectResult;
import nl.jk5.mqtt.MqttHandler;
import nl.jk5.mqtt.NamedThreadFactory;
import nl.jk5.mqtt.StatService;

public class BroadCastTest {
    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup(32, new NamedThreadFactory("workerio"));
        MqttClientConfig config = new MqttClientConfig();
        config.setCleanSession(true);
        config.setProtocolVersion(MqttVersion.MQTT_3_1_1);
        String brokerUrl = System.getProperty("brokerUrl", "127.0.0.1");
        int port = Integer.parseInt(System.getProperty("port", "1883"));
        String topic = System.getProperty("topic", "BroadCast");
        int num = Integer.parseInt(System.getProperty("num", "1"));
        int delta = Integer.parseInt(System.getProperty("delta", "0"));
        long time = System.currentTimeMillis();
        Map<String, MqttClient> clientMap = new ConcurrentHashMap<>();
        StatService statService = new StatService("RT", 5);
        statService.start();
        for (int i = 0; i < num; i++) {
            config.setUsername("XXXXX");
            config.setPassword("XXXXX");
            config.setTimeoutSeconds(90);
            String clientId = "GID_XXXX@@@" + time + "_" + i;
            config.setClientId(clientId);
            MqttClient mqttClient = new MqttClientImpl(config);
            mqttClient.setEventLoop(eventLoopGroup);
            mqttClient.on(topic, new MqttHandler() {
                @Override
                public void onMessage(String topic, ByteBuf payload) {
                    byte[] data = new byte[payload.readableBytes()];
                    payload.getBytes(0, data);
                    MqttMsgPayload body = JSON.parseObject(data, MqttMsgPayload.class);
                    long current = System.currentTimeMillis() - body.getTime() - delta;
                    statService.setStatDelayTimeMax(current);
                }
            }, MqttQoS.AT_MOST_ONCE);
            Future<MqttConnectResult> future = mqttClient.connect(brokerUrl, port);
            clientMap.put(clientId, mqttClient);
            Thread.sleep(10);
        }

        while (true) {
            Thread.sleep(1000);
        }
    }
}
