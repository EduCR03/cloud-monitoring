import paho.mqtt.client as mqtt
import ssl
from datetime import datetime
import os

# ------------------------------------
# Configurações MQTT
# ------------------------------------

# Broker: tenta pegar de variável, senão usa default local
BROKER = os.environ.get("BROKER", "a19mijesri84u2-ats.iot.us-east-1.amazonaws.com")
PORT = int(os.environ.get("PORT", 8883))

TOPICS = ["cloudv2-ping", "cloud2", "cloudv2"]
FILTER_NAME = "soilteste_1"

# ------------------------------------
# Gerenciar certificados
# ------------------------------------

# Caminhos dos arquivos locais
CA_CERT = "amazon_ca.pem"
CLIENT_CERT = "device.pem.crt"
CLIENT_KEY = "private.pem.key"

def preparar_certificados():
    """
    Se as variáveis de ambiente de certificados existirem (Render),
    grava em arquivos temporários. Se não existirem, assume que os
    arquivos locais já estão disponíveis (ambiente de desenvolvimento).
    """
    ca_env = os.environ.get("CA_CERT_CONTENT")
    cert_env = os.environ.get("CLIENT_CERT_CONTENT")
    key_env = os.environ.get("CLIENT_KEY_CONTENT")

    if ca_env and cert_env and key_env:
        with open(CA_CERT, "w") as f:
            f.write(ca_env)
        with open(CLIENT_CERT, "w") as f:
            f.write(cert_env)
        with open(CLIENT_KEY, "w") as f:
            f.write(key_env)
        print("Certificados carregados a partir de variáveis de ambiente.")
    else:
        print("Usando certificados locais (.pem).")

# ------------------------------------
# Logs
# ------------------------------------

LOG_DIR = "logs_mqtt"
os.makedirs(LOG_DIR, exist_ok=True)

def salvar_mensagem(topic, payload):
    # Nome do arquivo: <topic>_YYYY-MM-DD.txt
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    log_path = os.path.join(LOG_DIR, f"{topic}_{data_hoje}.txt")

    # Monta mensagem
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}]\n{payload}\n\n"

    # Salva
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(linha)

    print(f"Mensagem salva em {log_path}")

# ------------------------------------
# Callbacks MQTT
# ------------------------------------

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conectado ao broker!")
        for topic in TOPICS:
            client.subscribe(topic)
            print(f"Assinado no tópico: {topic}")
    else:
        print(f"Falha na conexão. Código: {rc}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        print(f"Recebido em {msg.topic}: {payload}")

        # Para TODOS os tópicos, só salva se contiver soilteste_1
        if FILTER_NAME in payload:
            salvar_mensagem(msg.topic, payload)

    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")

# ------------------------------------
# Inicialização
# ------------------------------------

# Prepara certificados dependendo do ambiente
preparar_certificados()

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.tls_set(
    ca_certs=CA_CERT,
    certfile=CLIENT_CERT,
    keyfile=CLIENT_KEY,
    tls_version=ssl.PROTOCOL_TLSv1_2
)

# Conecta e inicia loop
client.connect(BROKER, PORT)
client.loop_forever()
