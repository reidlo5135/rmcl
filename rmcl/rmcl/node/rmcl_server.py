import time

from rclpy.node import Node
from rclpy.parameter import Parameter
from rclpy.impl.rcutils_logger import RcutilsLogger

from typing import Final

from ..mqtt.mqtt_client import Client
from ..mqtt.domain import MqttConnectionInfo


PARAM_MQTT_BROKER_ADDRESS: Final = 'mqtt_broker_address'
PARAM_MQTT_BROKER_PORT: Final = 'mqtt_broker_port'
PARAM_MQTT_CLIENT_NAME: Final = 'mqtt_client_name'
PARAM_MQTT_CLIENT_KEEP_ALIVE: Final = 'mqtt_client_keep_alive'
PARAM_MQTT_USER_NAME: Final = 'mqtt_user_name'
PARAM_MQTT_USER_PASSWORD: Final = 'mqtt_user_password'

MQTT_RETRY_INTERVAL: int = 1

RCLPY_NODE_NAME: Final = 'rmcl_server'

class RmclServer(Node):
    
    def __init__(self) -> None:
        super().__init__(RCLPY_NODE_NAME)
        self.__log: RcutilsLogger = self.get_logger()
        self.__declare_parameters()
        
        mqtt_connection_info: MqttConnectionInfo = self.__set_connection_info_by_parameters()
        self.mqtt_client: Client = Client(log=self.get_logger(), mqtt_connection_info=mqtt_connection_info)
        self.is_broker_opened: bool = self.mqtt_client.check_broker_opened()
        
        if self.is_broker_opened:
            self.__log.info(f'MQTT Broker is opened with [{self.mqtt_client.broker_address}:{str(self.mqtt_client.broker_port)}]')
            self.mqtt_client.connect()
            self.mqtt_client.run()
        else:
            retries: int = 0
            while not self.is_broker_opened:
                self.__log.error(f'MQTT Broker is not opened yet.. retrying [{str(retries)}]')
                time.sleep(MQTT_RETRY_INTERVAL)
                retries += 1

    def __declare_parameters(self) -> None:
        self.declare_parameter(name=PARAM_MQTT_BROKER_ADDRESS, value='')
        self.declare_parameter(name=PARAM_MQTT_BROKER_PORT, value=0)
        self.declare_parameter(name=PARAM_MQTT_CLIENT_NAME, value='')
        self.declare_parameter(name=PARAM_MQTT_CLIENT_KEEP_ALIVE, value=0)
        self.declare_parameter(name=PARAM_MQTT_USER_NAME, value='')
        self.declare_parameter(name=PARAM_MQTT_USER_PASSWORD, value='')
    
    def __set_connection_info_by_parameters(self) -> MqttConnectionInfo:
        param_mqtt_broker_address: str = self.get_parameter(PARAM_MQTT_BROKER_ADDRESS).get_parameter_value().string_value
        param_mqtt_broker_port: int = self.get_parameter(PARAM_MQTT_BROKER_PORT).get_parameter_value().integer_value
        param_mqtt_client_name: str = self.get_parameter(PARAM_MQTT_CLIENT_NAME).get_parameter_value().string_value
        param_mqtt_client_keep_alive: int = self.get_parameter(PARAM_MQTT_CLIENT_KEEP_ALIVE).get_parameter_value().integer_value
        param_mqtt_user_name: str = self.get_parameter(PARAM_MQTT_USER_NAME).get_parameter_value().string_value
        param_mqtt_user_password: str = self.get_parameter(PARAM_MQTT_USER_PASSWORD).get_parameter_value().string_value
        
        mqtt_connection_info: MqttConnectionInfo = MqttConnectionInfo()
        mqtt_connection_info.broker_address = param_mqtt_broker_address
        mqtt_connection_info.broker_port = param_mqtt_broker_port
        mqtt_connection_info.client_name = param_mqtt_client_name
        mqtt_connection_info.client_keep_alive = param_mqtt_client_keep_alive
        mqtt_connection_info.user_name = param_mqtt_user_name
        mqtt_connection_info.user_password = param_mqtt_user_password
        
        return mqtt_connection_info


__all__ = ['rmcl_node']