from flask import Flask, request, jsonify
import pika

app = Flask(__name__)

# Configuração do RabbiMQ
RABBITMAQ_HOST = 'localhost'
QUEUE_NAME = 'data_queue'

def send_to_queue(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMAQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=message)
    connection.close()


@app.route('/send', methods=['POST'])
def send_message():
    try:
        data = request.json()
        if not data or 'message' not in data:
            return jsonify({'error': 'Invalid payload'}), 400
        
        message = data['message']
        send_to_queue(message=message)
        return jsonify({'status': 'Message send to queue'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)