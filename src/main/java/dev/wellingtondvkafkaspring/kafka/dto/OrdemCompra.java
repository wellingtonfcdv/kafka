package dev.wellingtondvkafkaspring.kafka.dto;

public record OrdemCompra(
        Long id,
        String produto,
        int quantidade,
        Double preco
) {}
