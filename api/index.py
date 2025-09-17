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

# --- Variáveis globais para erros de inicialização ---
gemini_init_error = None
kafka_init_error = None

# --- Google Gemini Configuration ---
try:
    gemini_api_key = os.environ.get('GEMINI_API_KEY')
    if gemini_api_key:
        genai.configure(api_key=gemini_api_key)
        print("Google Gemini configurado com sucesso.")
    else:
        gemini_init_error = "Variável de ambiente GEMINI_API_KEY não encontrada."
        print(gemini_init_error)
except Exception as e:
    gemini_init_error = str(e)
    print(f"Erro ao configurar Gemini: {e}")

# --- Kafka Consumer Configuration ---
kafka_consumer_instance = None
if Consumer:
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
        if kafka_conf['bootstrap.servers']:
            kafka_consumer_instance = Consumer(kafka_conf)
            kafka_consumer_instance.subscribe(['tarefas_ia'])
            print("Consumidor Kafka para 'tarefas_ia' inicializado.")
        else:
            kafka_init_error = "Variáveis de ambiente do Kafka não encontradas."
            print(kafka_init_error)
    except Exception as e:
        kafka_init_error = str(e)
        print(f"Erro ao inicializar Consumidor Kafka: {e}")
else:
    kafka_init_error = "Biblioteca confluent_kafka não encontrada."

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

    if not kafka_consumer_instance:
        return jsonify({"error": "Kafka consumer not initialized.", "details": kafka_init_error}), 503

    messages_processed = 0
    results = []
    try:
        msgs = kafka_consumer_instance.consume(num_messages=5, timeout=10.0)
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
                if task_type == 'image_analysis' or task_type == 'analisar_imagem':
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
        # O consumidor não deve ser fechado aqui se for uma instância global
        # kafka_consumer_instance.close() # Removido
        pass

    return jsonify({"status": "ok", "messages_processed": messages_processed, "results": results}), 200

def get_health_status():
    env_vars = {
        "GEMINI_API_KEY": "present" if os.environ.get('GEMINI_API_KEY') else "missing",
        "KAFKA_BOOTSTRAP_SERVER": "present" if os.environ.get('KAFKA_BOOTSTRAP_SERVER') else "missing",
        "KAFKA_API_KEY": "present" if os.environ.get('KAFKA_API_KEY') else "missing",
        "KAFKA_API_SECRET": "present" if os.environ.get('KAFKA_API_SECRET') else "missing"
    }

    status = {
        "environment_variables": env_vars,
        "dependencies": {
            "gemini_api": "ok" if not gemini_init_error else "error",
            "kafka_consumer": "ok" if kafka_consumer_instance and not kafka_init_error else "error"
        },
        "initialization_errors": {
            "gemini": gemini_init_error,
            "kafka": kafka_init_error
        }
    }
    return status

@app.route('/api/health', methods=['GET'])
def health_check():
    status = get_health_status()
    
    all_ok = (
        all(value == "present" for value in status["environment_variables"].values()) and
        status["dependencies"]["gemini_api"] == "ok" and
        status["dependencies"]["kafka_consumer"] == "ok"
    )
    http_status = 200 if all_ok else 503
    
    return jsonify(status), http_status

if __name__ == '__main__':
    app.run(debug=True)
