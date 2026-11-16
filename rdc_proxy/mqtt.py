"""MQTT publishing for Home Assistant ingestion.

Two kinds of entity are published:

- one sensor per decoded telemetry field (state topic <base>/<field>), and
- a few *derived* entities computed from GeneratorState rather than read off
  the wire (generator mode, running, utility power, proxy mode, oil check).

Discovery for the common fields and all derived entities is sent as soon as
the device id is known, so Home Assistant shows a complete device straight
away instead of growing entities as fields happen to arrive.
"""

from datetime import datetime, timedelta
import json
import re
import threading
import time

from rdc_proxy.wire import PARAM_MAP

FIELD_UNITS = {name: units for name, _transform, units in PARAM_MAP.values()}
HA_UNITS = {
    "C": "\u00b0C",
}

FIELD_META = {
    "batteryVoltageV": {"device_class": "voltage", "state_class": "measurement"},
    "controllerTempC": {"device_class": "temperature", "state_class": "measurement"},
    "engineFrequencyHz": {"device_class": "frequency", "state_class": "measurement"},
    "engineFrequencyHz_2": {"device_class": "frequency", "state_class": "measurement"},
    "engineSpeedRpm": {"state_class": "measurement"},
    "engineSpeedRpm_2": {"state_class": "measurement"},
    "generatorVoltageV": {"device_class": "voltage", "state_class": "measurement"},
    "generatorVoltageV_2": {"device_class": "voltage", "state_class": "measurement"},
    "generatorVoltageV_3": {"device_class": "voltage", "state_class": "measurement"},
    "generatorVoltageV_4": {"device_class": "voltage", "state_class": "measurement"},
    "generatorLoadW": {"device_class": "power", "state_class": "measurement"},
    "generatorLoadPercent": {"state_class": "measurement", "icon": "mdi:gauge"},
    "generatorCurrentA": {"device_class": "current", "state_class": "measurement"},
    "generatorPowerFactor": {"device_class": "power_factor", "state_class": "measurement"},
    "generatorApparentPowerVA": {"device_class": "apparent_power", "state_class": "measurement"},
    "generatorReactivePowerVAR": {"device_class": "reactive_power", "state_class": "measurement"},
    "lubeOilTempC": {"device_class": "temperature", "state_class": "measurement"},
    "maintHoursSinceLast": {"device_class": "duration", "state_class": "total_increasing"},
    "timestamp": {
        "device_class": "timestamp",
        "enabled_by_default": False,
        "entity_category": "diagnostic",
    },
    "totalOperationHours": {"device_class": "duration", "state_class": "total_increasing"},
    "totalOperationHours_2": {"device_class": "duration", "state_class": "total_increasing"},
    "totalRuntimeHours": {"device_class": "duration", "state_class": "total_increasing"},
    "utilityFrequencyHz": {"device_class": "frequency", "state_class": "measurement"},
    "utilityVoltageV": {"device_class": "voltage", "state_class": "measurement"},
    "utilityVoltageV_B": {"device_class": "voltage", "state_class": "measurement"},
}

# Fields announced to Home Assistant up front, before their first value. Many
# are only reported while the engine runs, so otherwise they would not exist
# as entities until the first exercise or outage.
UPFRONT_FIELDS = [
    "batteryVoltageV",
    "utilityVoltageV",
    "utilityVoltageV_B",
    "utilityFrequencyHz",
    "controllerTempC",
    "engineSpeedRpm",
    "generatorVoltageV",
    "engineFrequencyHz",
    "generatorLoadW",
    "generatorLoadPercent",
    "generatorCurrentA",
    "totalRuntimeHours",
    "totalOperationHours",
    "maintHoursSinceLast",
]

# The RDC stops reporting these when the engine stops rather than sending a
# final zero. Publish the zero ourselves on entering standby so graphs and
# automations don't hold the last running value forever.
ZERO_ON_STANDBY = {
    "engineSpeedRpm": "0",
    "engineSpeedRpm_2": "0",
    "generatorVoltageV": "0.0",
    "generatorVoltageV_2": "0.0",
    "generatorVoltageV_3": "0.0",
    "generatorVoltageV_4": "0.0",
    "engineFrequencyHz": "0.0",
    "engineFrequencyHz_2": "0.0",
    "generatorLoadW": "0.0",
    "generatorLoadW_2": "0.0",
    "generatorLoadPercent": "0",
    "generatorCurrentA": "0.0",
}

# How often to re-evaluate derived entities when no telemetry is arriving
# (mode falls back to standby on RPM staleness, proxy mode changes on its own).
DERIVED_REFRESH_S = 5

# Entities computed from GeneratorState, keyed by state-topic suffix.
CUSTOM_ENTITIES = {
    "mode": {
        "component": "sensor",
        "name": "Generator Mode",
        "icon": "mdi:generator-stationary",
    },
    "running": {
        "component": "binary_sensor",
        "name": "Generator Running",
        "device_class": "running",
        "payload_on": "ON",
        "payload_off": "OFF",
    },
    "utility_power": {
        "component": "binary_sensor",
        "name": "Utility Power",
        "device_class": "power",
        "payload_on": "ON",
        "payload_off": "OFF",
    },
    "proxy_mode": {
        "component": "sensor",
        "name": "Proxy Mode",
        "icon": "mdi:server-network",
        "entity_category": "diagnostic",
    },
    "oil_check_warn": {
        "component": "binary_sensor",
        "name": "Oil Check Warning",
        "device_class": "problem",
        "entity_category": "diagnostic",
        "payload_on": "ON",
        "payload_off": "OFF",
    },
    "oil_runtime_since_check": {
        "component": "sensor",
        "name": "Oil Runtime Since Check",
        "device_class": "duration",
        "entity_category": "diagnostic",
        "unit_of_measurement": "h",
    },
}

DEVICE_FIELDS = {"modelCode", "serialNumber"}


def _slug(value):
    return re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_")


def _field_slug(field):
    return _slug(re.sub(r"(?<!^)(?=[A-Z])", "_", field))


def _friendly_name(field):
    words = re.sub(r"(?<!^)(?=[A-Z])", " ", field).replace("_", " ")
    return (
        words.title()
        .replace(" Voltage V", " Voltage")
        .replace(" Temp C", " Temp")
        .replace(" Frequency Hz", " Frequency")
        .replace(" Rpm", " RPM")
    )


def _topic(*parts):
    return "/".join(str(p).strip("/") for p in parts if str(p).strip("/"))


def _timestamp_payload(value):
    # RDC timestamp values are .NET ticks holding the controller's local wall
    # clock time. Attach the host's local timezone so Home Assistant does not
    # interpret the wall-clock value as UTC.
    ts = datetime(1, 1, 1) + timedelta(microseconds=int(value) / 10)
    return ts.astimezone().isoformat()


def _field_payload(field, value):
    if field == "timestamp":
        try:
            return _timestamp_payload(value)
        except (OverflowError, TypeError, ValueError):
            return str(value)
    return str(value)


class MqttPublisher:
    def __init__(self, cfg, client_factory=None):
        self.cfg = cfg
        self.base_topic = cfg.get("base_topic", "rdc_proxy")
        self.discovery_prefix = cfg.get("discovery_prefix", "homeassistant")
        self.configured_device_id = _slug(cfg.get("device_id", ""))
        self.device_name = cfg.get("device_name", "Generator")
        self.retain = bool(cfg.get("retain", True))
        self.qos = int(cfg.get("qos", 0))
        self.client = self._make_client(client_factory)
        self._discovered = set()
        self._pending_discovery = set()
        self._serial_number = None
        self._model_code = None
        self._availability_online = False
        self.state = None
        # Derived entities are published from both the telemetry thread and
        # the refresh thread; only values that changed are sent.
        self._derived_lock = threading.Lock()
        self._last_derived = {}
        self._refresh_thread = None

    def _make_client(self, client_factory):
        if client_factory:
            return client_factory()

        import paho.mqtt.client as mqtt

        client_id = self.cfg.get("client_id", "rdc-proxy")
        if hasattr(mqtt, "CallbackAPIVersion"):
            return mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        return mqtt.Client(client_id=client_id)

    def start(self, state):
        self.state = state
        username = self.cfg.get("username")
        password = self.cfg.get("password")
        if username:
            self.client.username_pw_set(username, password or None)
        self.client.on_connect = self._on_connect
        self.client.reconnect_delay_set(min_delay=1, max_delay=60)
        self.client.will_set(
            _topic(self.base_topic, "status"),
            "offline",
            qos=self.qos,
            retain=True,
        )
        self.client.connect_async(
            self.cfg["host"],
            int(self.cfg.get("port", 1883)),
            keepalive=60,
        )
        self.client.loop_start()
        state.add_update_listener(self.publish_update)
        print(
            f"[mqtt] publishing to {self.cfg['host']}:{int(self.cfg.get('port', 1883))}",
            flush=True,
        )
        self._start_derived_refresh()

    def _start_derived_refresh(self):
        if self._refresh_thread is None or not self._refresh_thread.is_alive():
            self._refresh_thread = threading.Thread(target=self._derived_refresh_loop, daemon=True)
            self._refresh_thread.start()

    def _derived_refresh_loop(self):
        while True:
            time.sleep(DERIVED_REFRESH_S)
            try:
                self._publish_derived_states()
            except Exception as e:
                print(f"[mqtt] derived-state refresh failed: {e}", flush=True)

    def _on_connect(self, client, _userdata, _flags, reason_code, _properties=None):
        if reason_code == 0 or str(reason_code) == "Success":
            self.publish_availability(True)
            if self._device_id() is not None:
                self._publish_all_discoveries()
                self._publish_derived_states(force=True)

    def publish_availability(self, online):
        payload = "online" if online else "offline"
        self.client.publish(
            _topic(self.base_topic, "status"),
            payload,
            qos=self.qos,
            retain=True,
        )
        self._availability_online = online

    def publish_update(self, field, value, _timestamp):
        if not self._availability_online:
            self.publish_availability(True)
        had_device_id = self._device_id() is not None
        device_metadata_changed = False
        if field == "serialNumber" and value:
            serial_number = str(value)
            device_metadata_changed = serial_number != self._serial_number
            self._serial_number = serial_number
        if field == "modelCode" and value:
            model_code = str(value)
            device_metadata_changed = model_code != self._model_code
            self._model_code = model_code

        if self._device_id() is not None:
            if not had_device_id:
                self._publish_all_discoveries()
                self._publish_pending_discovery()
            elif device_metadata_changed:
                self._republish_discovery()

        if field not in DEVICE_FIELDS:
            self._publish_discovery(field)

        self.client.publish(
            _topic(self.base_topic, field),
            _field_payload(field, value),
            qos=self.qos,
            retain=self.retain,
        )

        self._publish_derived_states()

    def _device_id(self):
        if self.configured_device_id:
            return self.configured_device_id
        if self._serial_number:
            return _slug(self._serial_number)
        return None

    def _device_identifier(self):
        if self._serial_number:
            return f"kohler_generator_{_slug(self._serial_number)}"
        return f"rdc_proxy_{self._device_id()}"

    def _publish_pending_discovery(self):
        pending = sorted(self._pending_discovery)
        self._pending_discovery.clear()
        for field in pending:
            self._publish_discovery(field)

    def _republish_discovery(self):
        for field in sorted(self._discovered):
            self._publish_discovery(field, force=True)

    def _publish_all_discoveries(self):
        for field in UPFRONT_FIELDS:
            self._publish_discovery(field)
        for custom_field in CUSTOM_ENTITIES:
            self._publish_custom_discovery(custom_field)

    def _should_announce(self, key, force):
        """Shared gate for discovery: needs a device id, and happens once per
        entity unless forced (device metadata changed)."""
        if self._device_id() is None:
            self._pending_discovery.add(key)
            return False
        if key in self._discovered and not force:
            return False
        self._discovered.add(key)
        return True

    def _discovery_payload(self, key, name):
        payload = {
            "name": name,
            "unique_id": f"generator_{self._device_id()}_{_field_slug(key)}",
            "state_topic": _topic(self.base_topic, key),
            "availability_topic": _topic(self.base_topic, "status"),
            "device": {
                "identifiers": [self._device_identifier()],
                "name": self.device_name,
                "manufacturer": "Kohler",
            },
        }
        if self._serial_number:
            payload["device"]["serial_number"] = self._serial_number
        if self._model_code:
            payload["device"]["model"] = self._model_code
        return payload

    def _publish_discovery_config(self, component, key, payload):
        self.client.publish(
            _topic(self.discovery_prefix, component, self._device_id(), key, "config"),
            json.dumps(payload, sort_keys=True),
            qos=self.qos,
            retain=True,
        )

    def _publish_discovery(self, field, force=False):
        if field in CUSTOM_ENTITIES:
            self._publish_custom_discovery(field, force=force)
            return
        if not self._should_announce(field, force):
            return
        payload = self._discovery_payload(field, _friendly_name(field))
        unit = FIELD_UNITS.get(field)
        if unit:
            payload["unit_of_measurement"] = HA_UNITS.get(unit, unit)
        payload.update(FIELD_META.get(field, {}))
        self._publish_discovery_config("sensor", field, payload)

    def _publish_custom_discovery(self, key, force=False):
        if not self._should_announce(key, force):
            return
        info = dict(CUSTOM_ENTITIES[key])
        component = info.pop("component", "sensor")
        payload = self._discovery_payload(key, info.pop("name", _friendly_name(key)))
        payload.update(info)
        self._publish_discovery_config(component, key, payload)

    def _derived_states(self):
        """Current value of every derived entity, as MQTT payload strings."""
        mode = self.state.get_display_mode()
        # Some controllers report utility voltage only now and then. Until a
        # reading has arrived we do not know whether utility power is present,
        # and claiming OFF would look like an outage after every restart.
        # "None" is the payload Home Assistant reads as "unknown".
        util_v = self.state.values.get("utilityVoltageV")
        if util_v is None:
            utility_power = "None"
        else:
            utility_power = "ON" if util_v > 50 else "OFF"
        oil_warn, oil_runtime = self.state.oil_check_status()
        return {
            "mode": mode,
            "running": "ON" if mode in ("exercise", "running") else "OFF",
            "utility_power": utility_power,
            "proxy_mode": str(self.state.proxy_mode),
            "oil_check_warn": "ON" if oil_warn else "OFF",
            "oil_runtime_since_check": str(round(oil_runtime, 2)),
        }

    def _publish_derived_states(self, force=False):
        """Publish derived entities whose value changed (all of them when
        forced, e.g. after a broker reconnect). Needs the device id, because
        the entities don't exist in Home Assistant before discovery."""
        if self.state is None or self._device_id() is None:
            return
        current = self._derived_states()
        with self._derived_lock:
            previous = self._last_derived
            changed = {
                key: value for key, value in current.items()
                if force or previous.get(key) != value
            }
            entered_standby = current["mode"] == "standby" and (
                force or previous.get("mode") != "standby"
            )
            self._last_derived = current
        if entered_standby:
            for field, zero in ZERO_ON_STANDBY.items():
                self.client.publish(_topic(self.base_topic, field), zero, qos=self.qos, retain=self.retain)
        for key, value in changed.items():
            self.client.publish(_topic(self.base_topic, key), value, qos=self.qos, retain=self.retain)


def start_mqtt_publisher(cfg, state):
    mqtt_cfg = cfg.get("mqtt", {})
    if not mqtt_cfg.get("enabled"):
        return None
    publisher = MqttPublisher(mqtt_cfg)
    publisher.start(state)
    return publisher
