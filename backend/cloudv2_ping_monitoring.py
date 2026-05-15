import atexit
import logging
import os
import ssl
import sys
import threading
import time

import paho.mqtt.client as mqtt

from backend.cloudv2_config import FIXED_MONITOR_TOPICS, get_config_file_path, load_runtime_config
from backend.cloudv2_dashboard import generate_dashboard_assets, start_dashboard_server
from backend.cloudv2_paths import LEGACY_WEB_DIRS, resolve_data_dir
from backend.cloudv2_telemetry import TelemetryStore


PID_FILE = ".cloudv2-monitor.pid"
LOG_DIR = "logs_mqtt"

CA_CERT = str(os.environ.get("CA_CERT_PATH", "amazon_ca.pem")).strip() or "amazon_ca.pem"
CLIENT_CERT = str(os.environ.get("CLIENT_CERT_PATH", "device.pem.crt")).strip() or "device.pem.crt"
CLIENT_KEY = str(os.environ.get("CLIENT_KEY_PATH", "private.pem.key")).strip() or "private.pem.key"


def _is_render_environment():
    render_value = str(os.environ.get("RENDER", "")).strip().lower()
    return render_value in ("1", "true", "yes", "on") or bool(os.environ.get("RENDER_SERVICE_ID"))


runtime_config = load_runtime_config()

BROKER = runtime_config["broker"]
MQTT_PORT = runtime_config["port"]
MONITOR_TOPICS = tuple(runtime_config.get("monitor_topics") or FIXED_MONITOR_TOPICS)
DASHBOARD_ENABLED = runtime_config["dashboard_enabled"]
DASHBOARD_PORT = runtime_config["dashboard_port"]
DASHBOARD_REFRESH_SEC = runtime_config["dashboard_refresh_sec"]
DASHBOARD_HOST = str(os.environ.get("DASHBOARD_HOST", "127.0.0.1")).strip() or "127.0.0.1"
DEV_HOT_RELOAD = str(os.environ.get("CLOUDV2_DEV_HOT_RELOAD", "1")).strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
DEV_HOT_RELOAD_POLL_SEC = 1.0
if _is_render_environment() and "DASHBOARD_HOST" not in os.environ:
    DASHBOARD_HOST = "0.0.0.0"

# Forca os topicos monitorados fixos e sem wildcard.
if tuple(MONITOR_TOPICS) != tuple(FIXED_MONITOR_TOPICS):
    MONITOR_TOPICS = tuple(FIXED_MONITOR_TOPICS)


logger = logging.getLogger("cloudv2.monitor")
telemetry = None
dashboard_server = None
mqtt_client = None
mqtt_connected = threading.Event()
restart_requested = threading.Event()
restart_reason = None
dev_reload_token = str(int(time.time() * 1000))
hot_reload_watcher = None


def _dashboard_log_url():
    external_url = str(os.environ.get("RENDER_EXTERNAL_URL", "")).strip()
    if external_url:
        return f"{external_url.rstrip('/')}/index.html"

    if DASHBOARD_HOST == "0.0.0.0":
        return f"http://localhost:{DASHBOARD_PORT}/index.html"
    return f"http://{DASHBOARD_HOST}:{DASHBOARD_PORT}/index.html"


def _configure_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, "cloudv2-monitor.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
        force=True,
    )
    logger.info("Arquivo de configuracao ativo: %s", get_config_file_path())
    logger.info("Topicos monitorados (somente leitura): %s", ", ".join(MONITOR_TOPICS))


def _is_process_running(pid):
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except (OSError, SystemError):
        return False
    return True


def _read_pid_file():
    if not os.path.exists(PID_FILE):
        return None
    try:
        with open(PID_FILE, "r", encoding="utf-8") as file:
            content = file.read().strip()
        if not content:
            return None
        return int(content)
    except (OSError, ValueError):
        return None


def _release_pid_file():
    current_pid = os.getpid()
    pid_in_file = _read_pid_file()
    if pid_in_file != current_pid:
        return
    try:
        os.remove(PID_FILE)
    except OSError:
        pass


def _acquire_pid_file():
    current_pid = os.getpid()
    existing_pid = _read_pid_file()
    if existing_pid and existing_pid != current_pid and _is_process_running(existing_pid):
        logger.error("Ja existe um monitor em execucao (PID %s).", existing_pid)
        return False
    try:
        with open(PID_FILE, "w", encoding="utf-8") as file:
            file.write(str(current_pid))
    except OSError as exc:
        logger.error("Falha ao criar arquivo de lock do monitor: %s", exc)
        return False
    atexit.register(_release_pid_file)
    return True


def preparar_certificados():
    ca_env = os.environ.get("CA_CERT_CONTENT")
    cert_env = os.environ.get("CLIENT_CERT_CONTENT")
    key_env = os.environ.get("CLIENT_KEY_CONTENT")

    if ca_env and cert_env and key_env:
        ca_dir = os.path.dirname(CA_CERT)
        cert_dir = os.path.dirname(CLIENT_CERT)
        key_dir = os.path.dirname(CLIENT_KEY)
        if ca_dir:
            os.makedirs(ca_dir, exist_ok=True)
        if cert_dir:
            os.makedirs(cert_dir, exist_ok=True)
        if key_dir:
            os.makedirs(key_dir, exist_ok=True)
        with open(CA_CERT, "w", encoding="utf-8") as file:
            file.write(ca_env)
        with open(CLIENT_CERT, "w", encoding="utf-8") as file:
            file.write(cert_env)
        with open(CLIENT_KEY, "w", encoding="utf-8") as file:
            file.write(key_env)
        logger.info(
            "Certificados carregados por variaveis de ambiente (ca=%s cert=%s key=%s).",
            CA_CERT,
            CLIENT_CERT,
            CLIENT_KEY,
        )
    else:
        logger.info(
            "Usando certificados locais (ca=%s cert=%s key=%s).",
            CA_CERT,
            CLIENT_CERT,
            CLIENT_KEY,
        )


def _publish_payload_to_dynamic_topic(pivot_topic, payload, *, label="comando"):
    topic = str(pivot_topic or "").strip()
    if not topic:
        return False

    # Regra critica: nunca publicar nos topicos fixos monitorados.
    if topic in FIXED_MONITOR_TOPICS:
        logger.error("Bloqueio de seguranca: tentativa de publicar em topico fixo '%s'.", topic)
        return False

    if mqtt_client is None or not mqtt_connected.is_set():
        logger.warning("MQTT ainda nao conectado para publicar %s em %s.", label, topic)
        return False

    try:
        result = mqtt_client.publish(topic, payload, qos=0, retain=False)
        ok = result.rc == mqtt.MQTT_ERR_SUCCESS
        if ok:
            logger.info("%s enviado no topico dinamico %s com payload %s", label.capitalize(), topic, payload)
        else:
            logger.warning("Falha ao publicar %s em %s (rc=%s)", label, topic, result.rc)
        return ok
    except Exception as exc:
        logger.exception("Erro ao publicar %s em %s: %s", label, topic, exc)
        return False


def _publish_probe_to_dynamic_topic(pivot_topic, payload):
    return _publish_payload_to_dynamic_topic(pivot_topic, payload, label="probe")


def _publish_pivot_config_request_to_dynamic_topic(pivot_topic, payload):
    return _publish_payload_to_dynamic_topic(pivot_topic, payload, label="configuracao idp 03")


def _publish_modem_reset_to_dynamic_topic(pivot_topic, payload):
    topic = str(pivot_topic or "").strip()
    if not topic:
        return False
    if mqtt_client is None or not mqtt_connected.is_set():
        logger.warning("MQTT ainda nao conectado para publicar reset de modem em %s.", topic)
        return False
    try:
        subscribe_result = mqtt_client.subscribe(topic)
        logger.info("Topico dinamico assinado para ACK de reset: %s (resultado=%s)", topic, subscribe_result)
    except Exception as exc:
        logger.exception("Erro ao assinar topico dinamico %s para ACK de reset: %s", topic, exc)
        return False
    return _publish_payload_to_dynamic_topic(topic, payload, label="reset de modem")


def on_connect(client, userdata, flags, rc):
    if rc != 0:
        logger.error("Falha na conexao com broker MQTT. Codigo: %s", rc)
        return

    mqtt_connected.set()
    logger.info("Conectado ao broker MQTT.")
    for topic in MONITOR_TOPICS:
        client.subscribe(topic)
        logger.info("Assinado em topico fixo: %s", topic)


def on_disconnect(client, userdata, rc):
    mqtt_connected.clear()
    logger.warning("Desconectado do broker MQTT (rc=%s).", rc)


def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8", errors="replace")
    except Exception:
        payload = str(msg.payload)

    try:
        if telemetry is not None:
            telemetry.process_message(msg.topic, payload)
    except Exception as exc:
        logger.exception("Erro ao processar mensagem MQTT topic=%s: %s", msg.topic, exc)


def _iter_watch_files():
    root = os.getcwd()
    watch_ext = {".py", ".html", ".css", ".js"}
    skip_dirs = {
        ".git",
        ".vscode",
        "__pycache__",
        LOG_DIR,
    }
    skip_prefixes = set()
    legacy_data_dirs = [os.path.join(legacy_dir, "data") for legacy_dir in LEGACY_WEB_DIRS]
    for data_dir in (resolve_data_dir(), *legacy_data_dirs):
        try:
            rel_data_dir = os.path.relpath(os.path.abspath(data_dir), root)
        except ValueError:
            rel_data_dir = data_dir
        normalized = rel_data_dir.replace("/", os.sep).replace("\\", os.sep).strip()
        if normalized and normalized != ".":
            skip_prefixes.add(normalized)

    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = os.path.relpath(dirpath, root)
        rel_dir_norm = "." if rel_dir == "." else rel_dir.replace("/", os.sep).replace("\\", os.sep)

        filtered_dirs = []
        for item in dirnames:
            if item in skip_dirs:
                continue
            item_rel = os.path.join(rel_dir_norm, item) if rel_dir_norm != "." else item
            if any(item_rel == prefix or item_rel.startswith(prefix + os.sep) for prefix in skip_prefixes):
                continue
            filtered_dirs.append(item)
        dirnames[:] = filtered_dirs

        for filename in filenames:
            ext = os.path.splitext(filename)[1].lower()
            if ext not in watch_ext:
                continue
            yield os.path.join(dirpath, filename)


def _build_watch_snapshot():
    snapshot = {}
    for path in _iter_watch_files():
        try:
            snapshot[path] = os.path.getmtime(path)
        except OSError:
            continue
    return snapshot


def _request_restart(reason):
    global restart_reason
    if restart_requested.is_set():
        return
    restart_reason = str(reason or "mudanca de codigo detectada")
    restart_requested.set()
    logger.warning("Hot reload: %s", restart_reason)


def _hot_reload_loop():
    previous = _build_watch_snapshot()
    while not restart_requested.is_set():
        time.sleep(DEV_HOT_RELOAD_POLL_SEC)
        current = _build_watch_snapshot()
        changed = []

        for path, mtime in current.items():
            old_mtime = previous.get(path)
            if old_mtime is None or mtime != old_mtime:
                changed.append(path)

        for path in previous:
            if path not in current:
                changed.append(path)

        if changed:
            changed_rel = []
            cwd = os.getcwd()
            for path in changed[:3]:
                try:
                    changed_rel.append(os.path.relpath(path, cwd))
                except ValueError:
                    changed_rel.append(path)
            suffix = "..." if len(changed) > 3 else ""
            _request_restart(f"arquivos alterados: {', '.join(changed_rel)}{suffix}")
            return

        previous = current


def main():
    global telemetry
    global dashboard_server
    global mqtt_client
    global hot_reload_watcher

    _configure_logging()

    if not _acquire_pid_file():
        raise SystemExit(1)

    preparar_certificados()

    telemetry = TelemetryStore(runtime_config, log_dir=LOG_DIR)
    telemetry.set_probe_sender(_publish_probe_to_dynamic_topic)
    telemetry.set_modem_reset_sender(_publish_modem_reset_to_dynamic_topic)
    telemetry.set_pivot_config_sender(_publish_pivot_config_request_to_dynamic_topic)
    telemetry.start()

    if DASHBOARD_ENABLED:
        generate_dashboard_assets(DASHBOARD_REFRESH_SEC)
        dashboard_server = start_dashboard_server(
            DASHBOARD_PORT,
            telemetry,
            reload_token_getter=lambda: dev_reload_token,
            host=DASHBOARD_HOST,
        )
        logger.info("Dashboard ativo em %s", _dashboard_log_url())
        if DEV_HOT_RELOAD:
            logger.info("Hot reload DEV ativo (poll %.1fs).", DEV_HOT_RELOAD_POLL_SEC)

    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message

    mqtt_client.tls_set(
        ca_certs=CA_CERT,
        certfile=CLIENT_CERT,
        keyfile=CLIENT_KEY,
        tls_version=ssl.PROTOCOL_TLSv1_2,
    )

    logger.info("Conectando ao broker %s:%s ...", BROKER, MQTT_PORT)
    mqtt_client.connect(BROKER, MQTT_PORT)

    if DEV_HOT_RELOAD:
        hot_reload_watcher = threading.Thread(target=_hot_reload_loop, name="cloudv2-hot-reload", daemon=True)
        hot_reload_watcher.start()

    try:
        mqtt_client.loop_start()
        while not restart_requested.is_set():
            time.sleep(0.3)
    except KeyboardInterrupt:
        logger.info("Encerrando monitor por interrupcao do usuario.")
    finally:
        mqtt_connected.clear()
        try:
            mqtt_client.loop_stop()
        except Exception:
            pass
        try:
            mqtt_client.disconnect()
        except Exception:
            pass
        if telemetry is not None:
            telemetry.stop()
        if dashboard_server is not None:
            try:
                dashboard_server.shutdown()
            except Exception:
                pass
            try:
                dashboard_server.server_close()
            except Exception:
                pass

        if restart_requested.is_set():
            logger.info("Reiniciando monitor para aplicar alteracoes...")
            if restart_reason:
                logger.info("Motivo do reinicio: %s", restart_reason)
            os.execv(sys.executable, [sys.executable] + sys.argv)


if __name__ == "__main__":
    main()
