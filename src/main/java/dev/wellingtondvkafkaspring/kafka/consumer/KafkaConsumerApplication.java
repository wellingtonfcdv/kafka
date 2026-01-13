package dev.wellingtondvkafkaspring.kafka.consumer;

import dev.wellingtondvkafkaspring.kafka.dto.OrdemCompra;
import org.apache.kafka.clients.consumer.internals.Acknowledgements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumerApplication {

    private final Logger log = LoggerFactory.getLogger(KafkaConsumerApplication.class);
@KafkaListener(topics = "ordem-compra", groupId = "grupo_ordem_compra")
    public void consumirOrdemCompra(OrdemCompra ordem, Acknowledgment ack){
        try{
            log.info("Ordem de compra recebida: {}",ordem);
            //processamento da mensagem

            //após o processamento com sucesso, realiza o ack manual
            ack.acknowledge();
        }catch (Exception e){
            log.error("Erro ao processar a ordem de compra: {}", e.getMessage());
        }
    }
}
