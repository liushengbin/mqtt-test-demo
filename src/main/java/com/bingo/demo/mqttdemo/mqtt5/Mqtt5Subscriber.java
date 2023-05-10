package com.bingo.demo.mqttdemo.mqtt5;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.mqttv5.client.*;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;

/**
 * 基于paho mqtt5版本 mqtt消费者
 *
 * @author liushengbin
 * @since 2023-05-10
 */
@Slf4j
public class Mqtt5Subscriber {

    private String serverUri;
    private String clientId;
    private String topic = "";

    private String username;

    private String password;

    private MqttAsyncClient client;

    public Mqtt5Subscriber(String serverUri, String clientId, String topic) {
        try {
            mqttAsyncClient(serverUri, clientId, topic);
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    public MqttAsyncClient mqttAsyncClient(String serverUri, String clientId, String topic) throws MqttException {
        client = new MqttAsyncClient(serverUri, clientId);
        MqttConnectionOptions options = new MqttConnectionOptions();
        if (StringUtils.hasText(username) || StringUtils.hasText(password)) {
            options.setUserName(username);
            options.setPassword(password.getBytes(StandardCharsets.UTF_8));
        }

        client.setCallback(new MqttCallback() {

            @Override
            public void mqttErrorOccurred(MqttException exception) {
                log.error("MQTT Client error occurred", exception);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                System.out.println("messageArrived-》 topic:" + topic + " message:" + message);

            }

            @Override
            public void disconnected(MqttDisconnectResponse disconnectResponse) {
                log.info("MQTT Client disconnected");
            }

            @Override
            public void deliveryComplete(IMqttToken token) {
                log.info("MQTT Client delivery completed");
            }

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                log.info("MQTT Client connect completed");
                subscribe(topic);
            }

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties properties) {
                System.out.println("authPacketArrived-》 reasonCode:" + reasonCode + " message:" + properties.toString());
            }
        });

        IMqttToken token = client.connect(options);
        token.waitForCompletion();

        return client;
    }

    public void subscribe(String topic) {
        if (client == null) {
            return;
        }
        try {
            client.subscribe(topic, 1);
            System.out.println("topic 订阅完成");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args) {
        String serverURI = "tcp://192.168.1.223:1883";
        String clientID = "demo_mqtt5_sub";
        String subTopic = "TEST/001";
        new Mqtt5Subscriber(serverURI, clientID, subTopic);
    }


}
