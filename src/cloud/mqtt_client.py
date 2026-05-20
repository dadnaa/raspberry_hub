"""
mqtt_client.py — Persistent HiveMQ Cloud Connection Layer

Responsibilities:
  - Connect to HiveMQ Cloud using env-var credentials
  - Maintain session with automatic reconnect + exponential backoff
  - Expose publish() and subscribe() that work even during reconnect
  - Never block the caller — all I/O is in a background thread
  - Buffer outgoing messages when offline (bounded queue)

Environment variables required:
  MQTT_HOST        HiveMQ broker hostname
  MQTT_PORT        TLS port (default 8883)
  MQTT_USERNAME    HiveMQ username
  MQTT_PASSWORD    HiveMQ password
  MQTT_PRINTER_ID  Unique printer identifier
"""

import logging
import os
import threading
import time
import queue
from typing import Callable, Optional

import paho.mqtt.client as paho
from paho.mqtt.client import MQTTMessage

from config.settings import (
    MQTT_BACKOFF_MAX_SEC,
    MQTT_BACKOFF_START_SEC,
    MQTT_DEFAULT_CLIENT_ID_SUFFIX,
    MQTT_DEFAULT_PORT,
    MQTT_HOST_ENV,
    MQTT_KEEPALIVE_SEC,
    MQTT_OUTBOX_MAXSIZE,
    MQTT_PASSWORD_ENV,
    MQTT_PORT_ENV,
    MQTT_PRINTER_ID_ENV,
    MQTT_USERNAME_ENV,
)

logger = logging.getLogger(__name__)

_BACKOFF_START = MQTT_BACKOFF_START_SEC
_BACKOFF_MAX = MQTT_BACKOFF_MAX_SEC
_OUTBOX_MAXSIZE = MQTT_OUTBOX_MAXSIZE
_KEEPALIVE_SEC = MQTT_KEEPALIVE_SEC


class MQTTClient:
    """
    Persistent, non-blocking MQTT client for HiveMQ Cloud.

    Usage:
        client = MQTTClient()
        client.set_message_handler(my_handler)
        client.start()
        client.publish("some/topic", '{"hello": "world"}')
        client.stop()

    Message handler signature:
        def handler(topic: str, payload: str) -> None: ...
    """

    def __init__(self) -> None:
        self._host      = os.getenv(MQTT_HOST_ENV)
        self._port      = int(os.environ.get(MQTT_PORT_ENV, str(MQTT_DEFAULT_PORT)))
        self._username  = os.getenv(MQTT_USERNAME_ENV)
        self._password  = os.getenv(MQTT_PASSWORD_ENV)
        self.printer_id = os.getenv(MQTT_PRINTER_ID_ENV)

        self._client: paho.Client              = self._build_client()
        self._connected                         = threading.Event()
        self._stop_event                        = threading.Event()
        self._outbox: queue.Queue              = queue.Queue(maxsize=_OUTBOX_MAXSIZE)
        self._subscriptions: list[str]         = []
        self._message_handler: Optional[Callable[[str, str], None]] = None
        self._thread:       Optional[threading.Thread] = None
        self._drain_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_message_handler(self, handler: Callable[[str, str], None]) -> None:
        self._message_handler = handler

    def subscribe(self, topic: str, qos: int = 1) -> None:
        if topic not in self._subscriptions:
            self._subscriptions.append(topic)
        if self._connected.is_set():
            self._client.subscribe(topic, qos=qos)
            logger.info(f"[MQTT] Subscribed: {topic}")

    def publish(self, topic: str, payload: str, qos: int = 1, retain: bool = False) -> None:
        if self._connected.is_set():
            self._client.publish(topic, payload, qos=qos, retain=retain)
        else:
            try:
                self._outbox.put_nowait((topic, payload, qos, retain))
                logger.debug(f"[MQTT] Buffered (offline): {topic}")
            except queue.Full:
                try:
                    self._outbox.get_nowait()
                except queue.Empty:
                    pass
                self._outbox.put_nowait((topic, payload, qos, retain))
                logger.warning("[MQTT] Outbox full — oldest message dropped.")

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._connect_loop, name="MQTTConnectLoop", daemon=True
        )
        self._drain_thread = threading.Thread(
            target=self._drain_loop, name="MQTTDrainLoop", daemon=True
        )
        self._thread.start()
        self._drain_thread.start()
        logger.info("[MQTT] Client started.")

    def stop(self) -> None:
        self._stop_event.set()
        self._client.disconnect()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("[MQTT] Client stopped.")

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    # ------------------------------------------------------------------
    # Paho callbacks
    # ------------------------------------------------------------------

    def _build_client(self) -> paho.Client:
        client = paho.Client(
            client_id=f"rasp-arch-{os.environ.get(MQTT_PRINTER_ID_ENV, MQTT_DEFAULT_CLIENT_ID_SUFFIX)}",
            protocol=paho.MQTTv5,
        )
        client.username_pw_set(self._username, self._password)
        client.tls_set()
        client.on_connect    = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message    = self._on_message
        return client

    def _on_connect(self, client, userdata, flags, rc, properties=None) -> None:
        if rc == 0:
            logger.info(f"[MQTT] Connected to {self._host}:{self._port}")
            self._connected.set()
            for topic in self._subscriptions:
                client.subscribe(topic, qos=1)
                logger.info(f"[MQTT] (Re)subscribed: {topic}")
        else:
            logger.error(f"[MQTT] Connection refused — rc={rc}")

    def _on_disconnect(self, client, userdata, rc, properties=None) -> None:
        self._connected.clear()
        if rc == 0:
            logger.info("[MQTT] Clean disconnect.")
        else:
            logger.warning(f"[MQTT] Unexpected disconnect rc={rc} — will reconnect.")

    def _on_message(self, client, userdata, msg: MQTTMessage) -> None:
        topic   = msg.topic
        payload = msg.payload.decode("utf-8", errors="replace")
        logger.debug(f"[MQTT] <- {topic}: {payload}")
        if self._message_handler:
            try:
                self._message_handler(topic, payload)
            except Exception:
                logger.exception(f"[MQTT] Message handler raised on {topic!r}")

    # ------------------------------------------------------------------
    # Reconnect loop
    # ------------------------------------------------------------------

    def _connect_loop(self) -> None:
        backoff = _BACKOFF_START
        while not self._stop_event.is_set():
            try:
                logger.info(f"[MQTT] Connecting to {self._host}:{self._port} ...")
                self._client.connect(self._host, self._port, keepalive=_KEEPALIVE_SEC)
                self._client.loop_start()
                while not self._stop_event.is_set():
                    if self._connected.wait(timeout=2.0):
                        backoff = _BACKOFF_START
                        break
                self._stop_event.wait()
                self._client.loop_stop()
                return
            except Exception as exc:
                logger.error(f"[MQTT] Connect error: {exc}. Retry in {backoff:.0f}s.")
                self._stop_event.wait(timeout=backoff)
                backoff = min(backoff * 2, _BACKOFF_MAX)

    # ------------------------------------------------------------------
    # Drain buffered messages after reconnect
    # ------------------------------------------------------------------

    def _drain_loop(self) -> None:
        while not self._stop_event.is_set():
            if self._connected.wait(timeout=1.0) and not self._outbox.empty():
                drained = 0
                while not self._outbox.empty():
                    try:
                        topic, payload, qos, retain = self._outbox.get_nowait()
                        self._client.publish(topic, payload, qos=qos, retain=retain)
                        drained += 1
                    except queue.Empty:
                        break
                if drained:
                    logger.info(f"[MQTT] Drained {drained} buffered message(s).")
