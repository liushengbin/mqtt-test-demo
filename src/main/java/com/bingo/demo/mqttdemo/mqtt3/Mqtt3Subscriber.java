package com.bingo.demo.mqttdemo.mqtt3;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import java.util.concurrent.TimeUnit;

/**
 * 基于paho mqtt3版本 mqtt消费者
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
            System.out.println("topic 订阅完成");
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
                    System.out.println("连接完成并订阅主题成功---");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println("连接丢失---");
                throwable.printStackTrace();
                reConnect();
            }

            @Override
            public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
                System.out.println(" " + Thread.currentThread().getName() + " msg:" + mqttMessage.toString());
//                    TimeUnit.SECONDS.sleep(4);
//                    System.out.println(" " + Thread.currentThread().getName());
//                    while (true) {
//                        // 获取线程名称,默认格式:pool-1-thread-1
//                        System.out.println(" " + Thread.currentThread().getName());
//
//                    }

//                executorService.execute(() -> {
//                    System.out.println(" " + Thread.currentThread().getName() + " msg:" + mqttMessage.toString());
//                    int i = 0;
//                    while (true) {
//                        i++;
//                    }
//                });
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                System.out.println("delivery isComplete:" + iMqttDeliveryToken.isComplete());
            }

        });
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(false);
        // 允许同时发送多少条消息（QOS1未收到PUBACK或QOS2未收到PUBCOMP的消息）
        connOpts.setMaxInflight(1000);
        connOpts.setKeepAliveInterval(10);
        connOpts.setAutomaticReconnect(false);
        mqttClient.connect(connOpts);


    }

    /**
     * 重连
     */
    private void reConnect() {

        // 休息1s
        long sleepTime = 1L;

        //重连N次机制
        int tryConnectNum = 0;
        while (true) {
            try {
                if (!mqttClient.isConnected()) {
                    mqttClient.reconnect();
//                    boolean flg = checkTopic(mqttClient);
//                    if (!flg) {
//                        //重新订阅

//                    }
                }
                System.out.println("客户端重新连接成功,SN:" + mqttClient.getClientId());
                break;
            } catch (Exception e) {
                tryConnectNum++;
                String errorNewMsg = "当前设备连接失败,尝试重连次数：" + tryConnectNum + ",SN:" + mqttClient.getClientId();
                System.err.println(errorNewMsg);
                // 休息1s
                try {
                    TimeUnit.SECONDS.sleep(sleepTime);
                } catch (InterruptedException interruptedException) {
                    System.err.println("延时操作报错" + interruptedException);
                }
            }
        }
    }

    public static void main(String[] args) {
        String serverURI = "tcp://192.168.1.223:1883";
        String clientID = "demo_mqtt_001";
        String subTopic = "TEST/001";
        new Mqtt3Subscriber(serverURI, clientID, subTopic);
    }
}
