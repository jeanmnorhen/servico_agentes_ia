import os
import json
import base64
from io import BytesIO
from PIL import Image
from datetime import datetime, timezone # Added
from flask import Flask, request, jsonify
from flask_cors import CORS
from confluent_kafka import Consumer, KafkaError
import google.generativeai as genai

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except ImportError:
    firebase_admin = None

app = Flask(__name__)
CORS(app)

# --- Variáveis globais para erros de inicialização ---
gemini_init_error = None
kafka_init_error = None
firebase_init_error = None # Adicionado para rastrear erros do Firebase

# --- Configuração do Firebase ---
db = None
if firebase_admin:
    try:
        base64_sdk = os.environ.get('FIREBASE_ADMIN_SDK_BASE64')
        if base64_sdk:
            decoded_sdk = base64.b64decode(base64_sdk).decode('utf-8')
            cred_dict = json.loads(decoded_sdk)
            cred = credentials.Certificate(cred_dict)
            if not firebase_admin._apps:
                firebase_admin.initialize_app(cred)
            db = firestore.client()
            print("Firebase inicializado com sucesso.")
        else:
            firebase_init_error = "Variável de ambiente FIREBASE_ADMIN_SDK_BASE64 não encontrada."
            print(firebase_init_error)
    except Exception as e:
        firebase_init_error = str(e)
        print(f"Erro ao inicializar Firebase: {e}")
else:
    firebase_init_error = "Biblioteca firebase_admin não encontrada."

# --- Configuração do Kafka Producer ---
producer = None
if Consumer: # Using Consumer here as a proxy for confluent_kafka being available
    try:
        kafka_conf = {
            'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.environ.get('KAFKA_API_KEY'),
            'sasl.password': os.environ.get('KAFKA_API_SECRET')
        }
        if kafka_conf['bootstrap.servers']:
            producer = Producer(kafka_conf)
            print("Produtor Kafka inicializado com sucesso.")
        else:
            kafka_init_error = "Variáveis de ambiente do Kafka não encontradas para o producer."
            print(kafka_init_error)
    except Exception as e:
        kafka_init_error = str(e)
        print(f"Erro ao inicializar Produtor Kafka: {e}")
else:
    kafka_init_error = "Biblioteca confluent_kafka não encontrada."

def delivery_report(err, msg):
    if err is not None:
        print(f'Falha ao entregar mensagem Kafka: {err}')
    else:
        print(f'Mensagem Kafka entregue em {msg.topic()} [{msg.partition()}]')

def publish_event(topic, event_type, task_id, data, changes=None):
    if not producer:
        print("Produtor Kafka não está inicializado. Evento não publicado.")
        return
    event = {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "task_id": task_id,
        "data": data,
        "source_service": "servico_agentes_ia"
    }
    if changes:
        event["changes"] = changes
    try:
        event_value = json.dumps(event, default=str)
        producer.produce(topic, key=task_id, value=event_value, callback=delivery_report)
        producer.poll(0)
        print(f"Evento '{event_type}' para a tarefa {task_id} publicado no tópico {topic}.")
    except Exception as e:
        print(f"Erro ao publicar evento Kafka: {e}")

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
        
        result_data = {
            "task_id": task.get('task_id'),
            "product_name": product_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source_image_url": task.get('source_image_url'), # Assuming task might have this
            "status": "completed"
        }

        if db:
            try:
                # Salvar no Firestore na coleção 'ai_results'
                doc_ref = db.collection('ai_results').document(task.get('task_id'))
                doc_ref.set(result_data)
                print(f"Resultado da IA salvo no Firestore para task_id: {task.get('task_id')}")
            except Exception as e:
                print(f"Erro ao salvar resultado no Firestore: {e}")
                result_data["firestore_error"] = str(e)
        else:
            print("Firestore não inicializado, não foi possível salvar o resultado.")
            result_data["firestore_error"] = "Firestore not initialized"

        if producer:
            try:
                # Publicar no tópico Kafka 'resultados_ia'
                publish_event('resultados_ia', 'ImageAnalysisResult', task.get('task_id'), result_data)
                print(f"Resultado da IA publicado no Kafka para task_id: {task.get('task_id')}")
            except Exception as e:
                print(f"Erro ao publicar resultado no Kafka: {e}")
                result_data["kafka_publish_error"] = str(e)
        else:
            print("Produtor Kafka não inicializado, não foi possível publicar o resultado.")
            result_data["kafka_publish_error"] = "Kafka Producer not initialized"

        return {"identified_product": product_name, "details": result_data}

    except Exception as e:
        print(f"Erro na análise de imagem: {e}")
        return {"error": str(e)}

# --- API Endpoints ---

@app.route('/api/agents/consume', methods=['POST', 'GET'])
def consume_tasks():
    # Security for Cron Job
    auth_header = request.headers.get('Authorization')
    cron_secret = os.environ.get('CRON_SECRET')
    if not cron_secret or auth_header != f'Bearer {cron_secret}':
        return jsonify({"error": "Unauthorized"}), 401

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
        "KAFKA_API_SECRET": "present" if os.environ.get('KAFKA_API_SECRET') else "missing",
        "FIREBASE_ADMIN_SDK_BASE64": "present" if os.environ.get('FIREBASE_ADMIN_SDK_BASE64') else "missing"
    }

    status = {
        "environment_variables": env_vars,
        "dependencies": {
            "gemini_api": "ok" if not gemini_init_error else "error",
            "kafka_consumer": "ok" if kafka_consumer_instance and not kafka_init_error else "error",
            "kafka_producer": "ok" if producer else "error", # Added
            "firestore": "ok" if db else "error"
        },
        "initialization_errors": {
            "gemini": gemini_init_error,
            "kafka": kafka_init_error,
            "kafka_producer": kafka_init_error, # Added
            "firestore": firebase_init_error
        }
    }
    return status

@app.route('/api/health', methods=['GET'])
def health_check():
    status = get_health_status()
    
    all_ok = (
        all(value == "present" for value in status["environment_variables"].values()) and
        status["dependencies"]["gemini_api"] == "ok" and
        status["dependencies"]["kafka_consumer"] == "ok" and
        status["dependencies"]["kafka_producer"] == "ok" and # Added
        status["dependencies"]["firestore"] == "ok"
    )
    http_status = 200 if all_ok else 503
    
    return jsonify(status), http_status

if __name__ == '__main__':
    app.run(debug=True)
