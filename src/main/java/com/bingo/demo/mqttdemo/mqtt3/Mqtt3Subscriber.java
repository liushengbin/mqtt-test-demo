package com.bingo.demo.mqttdemo.mqtt3;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

/**
 * Mqtt3Subscriber
 *
 * @author liushengbin
 * @since 2023-05-10
 */
@Slf4j
public class Mqtt3Subscriber {

    private MqttClient mqttClient;

    private String serverUri;
    private String clientId;
    private String topic;


    public Mqtt3Subscriber(String serverUri, String clientId, String topic) {
        this.serverUri = serverUri;
        this.clientId = clientId;
        this.topic = topic;
        try {
            connect();
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }

    public void subscribe(String topic) {
        if (mqttClient == null) {
            return;
        }
        try {
            mqttClient.subscribe(topic, 1);
            System.out.println("subscribe topic ");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }


    private void connect() throws MqttException {
        MqttDefaultFilePersistence persistence = new MqttDefaultFilePersistence();
        mqttClient = new MqttClient(serverUri, clientId, persistence);
        mqttClient.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                try {
                    subscribe(topic);
                    System.out.println("connection complete---");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("connection lost---");
                throwable.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
                System.out.println(" " + Thread.currentThread().getName() + " msg:" + mqttMessage.toString());//
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                System.out.println("delivery isComplete:" + iMqttDeliveryToken.isComplete());
            }

        });
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(false);
        connOpts.setMaxInflight(1000);
        connOpts.setKeepAliveInterval(10);
        connOpts.setAutomaticReconnect(true);
        mqttClient.connect(connOpts);


    }


    public static void main(String[] args) {
        String serverURI = "tcp://192.168.1.223:1883";
        String clientID = "demo_mqtt_001";
        String subTopic = "TEST/001";
        new Mqtt3Subscriber(serverURI, clientID, subTopic);
    }
}
