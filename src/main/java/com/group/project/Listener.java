package com.group.project;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.Map;

public class Listener implements MessageListener {

    private final  String name;
    private final Map<String,String> receivedMessages;

    public Listener(String name, Map<String, String> receivedMessages) {
        this.name = name;
        this.receivedMessages = receivedMessages;
    }

    @Override
    public void onMessage(Message message) {
        TextMessage message1=(TextMessage)message;
        try {
            System.out.println("Message received is "+((TextMessage) message).getText());
            System.out.println("Listener name "+name);
            receivedMessages.put(message1.getText(),name);
        } catch (JMSException e) {
            e.printStackTrace();
        }

    }
}
