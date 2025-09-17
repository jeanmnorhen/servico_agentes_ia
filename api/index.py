import os
import json
import base64
from io import BytesIO
from PIL import Image
from flask import Flask, request, jsonify
from flask_cors import CORS
from confluent_kafka import Consumer, KafkaError
import google.generativeai as genai

app = Flask(__name__)
CORS(app)

# --- Google Gemini Configuration ---
try:
    gemini_api_key = os.environ.get('GEMINI_API_KEY')
    if gemini_api_key:
        genai.configure(api_key=gemini_api_key)
        print("Google Gemini configurado com sucesso.")
    else:
        print("Variável de ambiente GEMINI_API_KEY não encontrada.")
except Exception as e:
    print(f"Erro ao configurar Gemini: {e}")

# --- Kafka Consumer Configuration ---
def create_kafka_consumer():
    try:
        kafka_conf = {
            'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
            'group.id': 'ai_agents_group_v1',
            'auto.offset.reset': 'earliest',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.environ.get('KAFKA_API_KEY'),
            'sasl.password': os.environ.get('KAFKA_API_SECRET')
        }
        if not kafka_conf['bootstrap.servers']:
            print("Variáveis de ambiente do Kafka não encontradas.")
            return None
        
        consumer = Consumer(kafka_conf)
        consumer.subscribe(['tarefas_ia'])
        print("Consumidor Kafka para 'tarefas_ia' inicializado.")
        return consumer
    except Exception as e:
        print(f"Erro ao inicializar Consumidor Kafka: {e}")
        return None

# --- AI Agent Logic ---
def process_image_analysis(task):
    if not gemini_api_key:
        return {"error": "Gemini API key not configured"}

    try:
        image_b64 = task.get('image_b64')
        if not image_b64:
            return {"error": "No image data in task"}

        image_bytes = base64.b64decode(image_b64)
        img = Image.open(BytesIO(image_bytes))
        
        model = genai.GenerativeModel('gemini-pro-vision')
        prompt = "Identifique o nome completo do produto principal nesta imagem, incluindo marca e volume/peso, se visível. Responda apenas com o nome do produto."
        
        response = model.generate_content([prompt, img])
        
        product_name = response.text.strip()
        print(f"Produto identificado pela IA: {product_name}")
        
        # TODO: Salvar o resultado no Firestore e publicar no tópico `resultados_ia`
        return {"identified_product": product_name}

    except Exception as e:
        print(f"Erro na análise de imagem: {e}")
        return {"error": str(e)}

# --- API Endpoints ---

@app.route('/api/agents/consume', methods=['POST', 'GET'])
def consume_tasks():
    # Security for Cron Job
    # auth_header = request.headers.get('Authorization')
    # cron_secret = os.environ.get('CRON_SECRET')
    # if not cron_secret or auth_header != f'Bearer {cron_secret}':
    #     return jsonify({"error": "Unauthorized"}), 401

    consumer = create_kafka_consumer()
    if not consumer:
        return jsonify({"error": "Kafka consumer could not be created."}), 503

    messages_processed = 0
    results = []
    try:
        msgs = consumer.consume(num_messages=5, timeout=10.0)
        if not msgs:
            return jsonify({"status": "No new messages to process"}), 200

        for msg in msgs:
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                task = json.loads(msg.value().decode('utf-8'))
                print(f"Task recebida: {task}")
                task_type = task.get('task_type')
                
                result = None
                if task_type == 'image_analysis':
                    print(f"Processando tarefa de análise de imagem: {task.get('task_id')}")
                    result = process_image_analysis(task)
                else:
                    print(f"Tipo de tarefa desconhecido: {task_type}")
                    result = {"error": "Unknown task type"}
                
                results.append(result)
                messages_processed += 1

            except json.JSONDecodeError as e:
                print(f"Failed to decode message: {e}")

    except Exception as e:
        print(f"An error occurred during message consumption: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        consumer.close()

    return jsonify({"status": "ok", "messages_processed": messages_processed, "results": results}), 200

@app.route('/api/health', methods=['GET'])
def health_check():
    gemini_status = "ok" if genai.api_key else "error"
    
    kafka_consumer = create_kafka_consumer()
    kafka_status = "error"
    if kafka_consumer:
        kafka_status = "ok"
        try:
            kafka_consumer.close()
        except Exception as e:
            print(f"Error closing Kafka consumer during health check: {e}")

    status = {
        "gemini_api": gemini_status,
        "kafka_consumer": kafka_status
    }
    http_status = 200 if all(s == "ok" for s in status.values()) else 503
    return jsonify(status), http_status

if __name__ == '__main__':
    app.run(debug=True)