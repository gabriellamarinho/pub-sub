from PIL import Image, ImageDraw
from confluent_kafka import Consumer, KafkaError, Producer
import json
import os
import logging

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:19091,kafka2:19092,kafka3:19093')
NOTIFICATION_TOPIC = os.environ.get('NOTIFICATION_TOPIC', 'notification')
OUT_FOLDER = '/processed/text/'
NEW = '_text'
IN_FOLDER = "/appdata/static/uploads/"
GROUP_ID = 'text-group'

producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
}
producer = Producer(producer_config)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        logging.error(f'Delivery failed for record {msg.key()}: {err}')
    else:
        logging.info(f'Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def publish_notification(original_filename, operation):
    message = {
        'original_filename': original_filename,
        'operation': operation,
        'body': f"O arquivo {original_filename} recebeu a operação de {operation}."
    }
    producer.produce(NOTIFICATION_TOPIC, key=original_filename.encode('utf-8'), value=json.dumps(message).encode('utf-8'), callback=delivery_report)
    producer.flush()
    logging.info(f"Notificação publicada para {NOTIFICATION_TOPIC}: {message}")

def create_text(path_file, original_filename):
    pathname, filename = os.path.split(path_file)
    output_folder = pathname + OUT_FOLDER
    os.makedirs(output_folder, exist_ok=True)
    try:
        original_image = Image.open(path_file)
        draw = ImageDraw.Draw(original_image)
        name, ext = os.path.splitext(filename)
        draw.text((0, 0), f"{name}", fill='white', size=35)
        output_path = os.path.join(output_folder, f"{name}{NEW}{ext}")
        original_image.save(output_path)
        publish_notification(original_filename, 'adição de texto')
        logging.info(f"Texto adicionado à imagem {original_filename} e salva em {output_path}")
    except FileNotFoundError:
        logging.error(f"Arquivo não encontrado: {path_file}")
    except Exception as e:
        logging.error(f"Erro ao processar {original_filename}: {e}")

def main():
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'client.id': 'text-client',
        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'}
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe(['image'])

    try:
        while True:
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            elif not msg.error():
                try:
                    data = json.loads(msg.value())
                    filename = data.get('new_file')
                    if filename:
                        logging.warning(f"TEXT: Lendo {filename}")
                        create_text(os.path.join(IN_FOLDER, filename), filename)
                        logging.warning(f"TEXT: Finalizado {filename}")
                    else:
                        logging.error(f"Mensagem inválida recebida: {msg.value()}")
                except json.JSONDecodeError:
                    logging.error(f"Erro ao decodificar JSON: {msg.value()}")
                except Exception as e:
                    logging.error(f"Erro ao processar mensagem: {e}")
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info(f'Fim da partição alcançado {msg.topic()}/{msg.partition()}')
            else:
                logging.error(f'Erro no consumidor: {msg.error().str()}')
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()