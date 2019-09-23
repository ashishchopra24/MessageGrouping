package com.group.project;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.lang.IllegalStateException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class MessageGroup {

    public static void main(String[] args) throws NamingException, JMSException, InterruptedException {

        InitialContext initialContext=new InitialContext();
        Queue queue=(Queue)initialContext.lookup("queue/inbound");
        Map<String,String> receivedMessages=new ConcurrentHashMap<>();

        try(ActiveMQConnectionFactory cf=new ActiveMQConnectionFactory();
            JMSContext jmsContext=cf.createContext();
            JMSContext jmsContext2=cf.createContext())
        {
            JMSProducer producer=jmsContext.createProducer();
            JMSConsumer consumer1=jmsContext2.createConsumer(queue);
            consumer1.setMessageListener(new Listener("Consumer-1",receivedMessages));
            JMSConsumer consumer2=jmsContext2.createConsumer(queue);
            consumer2.setMessageListener(new Listener("Consumer-2",receivedMessages));

            int count=10;
            TextMessage[] textMessages=new TextMessage[10];
            for(int i=0;i<count;i++)
            {
                textMessages[i]=jmsContext.createTextMessage("Message "+i);
                textMessages[i].setStringProperty("JMSXGroupID","Group-0");
                producer.send(queue,textMessages[i]);
            }
            Thread.sleep(2000);

            for(TextMessage message: textMessages)
            {
                if(!receivedMessages.get(message.getText()).equals("Consumer-1"))
                    throw new IllegalStateException("Group Message"+message.getText()+" has gone to the wrong receiver");
            }

        }
    }
}
