from dataclasses import dataclass

@dataclass
class MqttConnectionInfo():
    broker_address: str = ''
    broker_port: int = 0
    client_name: str = ''
    client_keep_alive: int = 0,
    user_name: str = ''
    user_password: str = ''
    

__all__ = ['rmcl_mqtt_domain']