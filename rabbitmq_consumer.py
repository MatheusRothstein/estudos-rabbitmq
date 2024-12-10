import pika
RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'data_queue'

def callback(ch, method, properties, body):
    print(f"[x] Receive {body.decode()}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def consume_from_queue():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    print(f"[*] Waiting for messages in {QUEUE_NAME}. To exit press CTRL + C")
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    channel.start_consuming()

if __name__ == '__main__':
    try:
        consume_from_queue()
    except KeyboardInterrupt:
        print("\nConsumer stopped.")