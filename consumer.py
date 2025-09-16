import os
import json
import sys
from confluent_kafka import Consumer, KafkaError

# Carrega as configurações do Kafka a partir de variáveis de ambiente
conf = {
    'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
    'group.id': 'agentes-ia-group-1', # ID do grupo de consumidores
    'auto.offset.reset': 'earliest', # Começa a ler do início do tópico se for um novo consumidor
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ.get('KAFKA_API_KEY'),
    'sasl.password': os.environ.get('KAFKA_API_SECRET')
}

topic = 'tarefas_ia'
consumer = Consumer(conf)

print(f"Ouvindo o tópico '{topic}'...")

try:
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(timeout=1.0) # Espera por 1 segundo

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Fim da partição, não é um erro real
                continue
            else:
                print(msg.error())
                break

        # Mensagem recebida
        print(f"\n--- Nova Tarefa Recebida ---")
        print(f"Chave: {msg.key().decode('utf-8')}")
        
        # Tenta decodificar o valor como JSON
        try:
            value = json.loads(msg.value().decode('utf-8'))
            print("Valor (JSON):")
            print(json.dumps(value, indent=2))
        except json.JSONDecodeError:
            print(f"Valor (raw): {msg.value().decode('utf-8')}")
        print("---------------------------")

except KeyboardInterrupt:
    print("\nConsumidor interrompido.")
finally:
    # Fecha o consumidor
    consumer.close()
