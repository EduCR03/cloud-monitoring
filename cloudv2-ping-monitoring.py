import paho.mqtt.client as mqtt
import ssl
from datetime import datetime
import os
import threading   # [NOVO] para agendar envios e timeout de resposta
import random      # [NOVO] para sortear os intervalos
import time        # [NOVO] para pequenas esperas/controlar timeout

# ------------------------------------
# Configurações MQTT
# ------------------------------------

BROKER = os.environ.get("BROKER", "a19mijesri84u2-ats.iot.us-east-1.amazonaws.com")
PORT = int(os.environ.get("PORT", 8883))

TOPICS = ["cloudv2-ping", "cloud2", "cloudv2", "cloudv2-shutdown"]

FILTER_NAMES = ["PioneiraLEM_2", "NovaBahia_6", "Savana_16", "ItacirJunior_5", "soilteste_1", "FazendaUfla_1"] 

# [NOVO] Tópicos da funcionalidade #11$
CMD_TOPIC  = os.environ.get("CMD_TOPIC",  "soilteste_1")    # onde publicaremos "#11$"
INFO_TOPIC = os.environ.get("INFO_TOPIC", "cloudv2-info")   # onde ouviremos a resposta

# [NOVO] Intervalo aleatório entre envios (minutos)
MIN_MINUTES = int(os.environ.get("MIN_MINUTES", 1))
MAX_MINUTES = int(os.environ.get("MAX_MINUTES", 10))

# [NOVO] Timeout para considerar que NÃO houve resposta (segundos)
RESPONSE_TIMEOUT_SEC = int(os.environ.get("RESPONSE_TIMEOUT_SEC", 10))

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

# [NOVO] Arquivo dedicado para respostas do cloudv2-info (sempre salva)
def salvar_resposta_info(payload):
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    log_path = os.path.join(LOG_DIR, f"cloudv2-info_respostas_{data_hoje}.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}]\n{payload}\n\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(linha)
    print(f"Resposta cloudv2-info salva em {log_path}")

# [NOVO] Arquivo que registra cada envio de #11$ e se teve resposta (SIM/NAO)
def salvar_envio_resultado(sim_ou_nao):
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    log_path = os.path.join(LOG_DIR, f"envios_11_{data_hoje}.txt")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}]\n#11$ - {sim_ou_nao}\n\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(linha)
    print(f"Envio registrado em {log_path}: #11$ - {sim_ou_nao}")

# ------------------------------------
# Estado da funcionalidade #11$  [NOVO]
# ------------------------------------

# flag de espera de resposta, timer e trava
_esperando_resposta = False
_timer_resposta = None
_lock = threading.Lock()

def _iniciar_timeout_resposta():
    """[NOVO] inicia o timer de 10s para marcar 'NAO' se não houver resposta."""
    global _timer_resposta
    def on_timeout():
        global _esperando_resposta, _timer_resposta
        with _lock:
            if _esperando_resposta:
                salvar_envio_resultado("NAO")
            _esperando_resposta = False
            _timer_resposta = None
    _timer_resposta = threading.Timer(RESPONSE_TIMEOUT_SEC, on_timeout)
    _timer_resposta.daemon = True
    _timer_resposta.start()

def _publicar_11(client):
    """[NOVO] publica '#11$' e arma a janela de resposta."""
    global _esperando_resposta, _timer_resposta
    try:
        client.publish(CMD_TOPIC, "#11$")
        print(f"#11$ publicado em {CMD_TOPIC}")
        with _lock:
            # se já existia um timer, cancela
            if _timer_resposta is not None:
                try:
                    _timer_resposta.cancel()
                except Exception:
                    pass
                _timer_resposta = None
            _esperando_resposta = True
            _iniciar_timeout_resposta()
    except Exception as e:
        print(f"Erro ao publicar #11$: {e}")

def _agendador_11(client):
    """[NOVO] thread que envia #11$ em intervalos aleatórios entre 1 e 10 minutos."""
    while True:
        _publicar_11(client)
        # escolhe o próximo intervalo APÓS o envio, entre MIN_MINUTES e MAX_MINUTES
        prox_min = random.randint(max(1, MIN_MINUTES), max(1, MAX_MINUTES))
        print(f"Próximo #11$ em ~{prox_min} minuto(s).")
        # dorme em segundos
        time.sleep(prox_min * 60)

# ------------------------------------
# Callbacks MQTT
# ------------------------------------

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conectado ao broker!")
        for topic in TOPICS:
            client.subscribe(topic)
            print(f"Assinado no tópico: {topic}")
        # [NOVO] garantir assinatura do INFO_TOPIC e (opcional) do CMD_TOPIC
        client.subscribe(INFO_TOPIC)
        print(f"Assinado no tópico: {INFO_TOPIC}")
        client.subscribe(CMD_TOPIC)
        print(f"Assinado no tópico: {CMD_TOPIC}")
    else:
        print(f"Falha na conexão. Código: {rc}")

def on_message(client, userdata, msg):
    global _esperando_resposta, _timer_resposta
    try:
        payload = msg.payload.decode("utf-8")
        print(f"Recebido em {msg.topic}: {payload}")

        # Para TODOS os tópicos originais, salva somente se contiver FILTER_NAMES
        if any(name in payload for name in FILTER_NAMES):
            salvar_mensagem(msg.topic, payload)

        # [NOVO] Se chegou no cloudv2-info, sempre salvar no arquivo dedicado
        if msg.topic == INFO_TOPIC:
            salvar_resposta_info(payload)
            # Se estávamos esperando resposta, marca SIM e cancela timeout
            with _lock:
                if _esperando_resposta:
                    salvar_envio_resultado("SIM")
                    _esperando_resposta = False
                    if _timer_resposta is not None:
                        try:
                            _timer_resposta.cancel()
                        except Exception:
                            pass
                        _timer_resposta = None

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

# Conecta
client.connect(BROKER, PORT)

# [NOVO] Inicia thread agendadora do #11$
threading.Thread(target=_agendador_11, args=(client,), daemon=True).start()

# Inicia loop
client.loop_forever()
