import smtplib
import ssl
import json
from email.message import EmailMessage
from confluent_kafka import Consumer, KafkaException, KafkaError
import os
import logging

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:19091,kafka2:19092,kafka3:19093')
NOTIFICATION_TOPIC = os.environ.get('NOTIFICATION_TOPIC', 'notification')
GROUP_ID = 'notifier-group'

EMAIL_FROM_FIXO = 'eemploe@gmail.com'
EMAIL_TO_FIXO = 'exemplo2222@gmail.com'
SENHA_FIXA = 'senha@'  

def disparar_email(conteudo):
    logging.info(f"[DEBUG] Disparando e-mail para {EMAIL_TO_FIXO} com conteúdo: {conteudo} de {EMAIL_FROM_FIXO}")
    email = EmailMessage()
    email.set_content(conteudo)
    email['Subject'] = 'Alerta de Processamento de Imagem'
    email['From'] = EMAIL_FROM_FIXO
    email['To'] = EMAIL_TO_FIXO

    contexto_ssl = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto_ssl) as servidor:
            servidor.login(EMAIL_FROM_FIXO, SENHA_FIXA)
            servidor.send_message(email)
        logging.info("[INFO] E-mail enviado com sucesso.")
    except Exception as e:
        logging.error(f"[ERRO] Falha ao enviar o e-mail: {e}")

def main():
    config_kafka = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'client.id': 'notifier-client',
        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'}
    }

    cliente = Consumer(config_kafka)
    cliente.subscribe([NOTIFICATION_TOPIC])

    try:
        while True:
            mensagem = cliente.poll(timeout=1.0)

            if mensagem is None:
                continue

            if mensagem.error():
                if mensagem.error().code() == KafkaError._PARTITION_EOF:
                    logging.info(f"[INFO] Fim da partição: {mensagem.partition()}")
                else:
                    raise KafkaException(mensagem.error())
            else:
                try:
                    conteudo_mensagem = mensagem.value().decode('utf-8')
                    logging.info(f"[DEBUG] Mensagem recebida de {NOTIFICATION_TOPIC}: {conteudo_mensagem}")

                    dados = json.loads(conteudo_mensagem)
                    logging.info(f"[DEBUG] JSON decodificado: {dados}")

                    texto_notificacao = dados.get('body')
                    if texto_notificacao:
                        disparar_email(texto_notificacao)
                    else:
                        logging.warning("[AVISO] Corpo da mensagem de notificação ausente.")

                except json.JSONDecodeError:
                    logging.error("[ERRO] Falha ao decodificar mensagem JSON.")
                except Exception as erro:
                    logging.error(f"[ERRO] Falha ao processar a mensagem: {erro}")
    finally:
        cliente.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()