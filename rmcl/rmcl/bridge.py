import time
import rclpy

from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor
from rclpy.exceptions import ROSInterruptException
from rclpy.parameter import Parameter
from rclpy.impl.rcutils_logger import RcutilsLogger

from typing import Final

from .mqtt import mqtt_client
from .mqtt.domain import MqttConnectionInfo

from .ros.publisher import Publisher


PARAM_MQTT_BROKER_ADDRESS: Final = 'broker_address'
PARAM_MQTT_BROKER_PORT: Final = 'broker_port'
PARAM_MQTT_CLIENT_NAME: Final = 'client_name'
PARAM_MQTT_CLIENT_KEEP_ALIVE: Final = 'client_keep_alive'
PARAM_MQTT_USER_NAME: Final = 'user_name'
PARAM_MQTT_USER_PASSWORD: Final = 'user_password'

MQTT_RETRY_INTERVAL: int = 1

RCLPY_NODE_NAME: Final = 'rmcl_server'
DOMAIN_NAME: Final = 'net/wavem/robotics'


class Bridge(Node):

    def __init__(self) -> None:
        super().__init__(RCLPY_NODE_NAME)
        self.__log: RcutilsLogger = self.get_logger()
        self.__declare_parameters()

        mqtt_connection_info: MqttConnectionInfo = self.__set_connection_info_by_parameters()
        self.mqtt_client: mqtt_client.Client = mqtt_client.Client(log=self.__log, mqtt_connection_info=mqtt_connection_info)
        self.is_broker_opened: bool = self.mqtt_client.check_broker_opened()

        if self.is_broker_opened:
            self.__log.info(
                f'MQTT Broker is opened with [{self.mqtt_client.broker_address}:{str(self.mqtt_client.broker_port)}]')
            self.mqtt_client.connect()
            self.mqtt_client.run()
        else:
            retries: int = 0
            while not self.is_broker_opened:
                self.__log.error(f'MQTT Broker is not opened yet.. retrying [{str(retries)}]')
                time.sleep(MQTT_RETRY_INTERVAL)
                retries += 1
        
        publisher: Publisher = Publisher(_node=self, _mqtt_client=self.mqtt_client)
        publisher.wait_for_reception()

        self.__mqtt_ros_register_subscription_topic: str = f'{DOMAIN_NAME}/rt/register/subscription'
        self.__mqtt_ros_register_topics_qos: int = 0

        self.__mqtt_ros_register_service_client_topic: str = f'{DOMAIN_NAME}/rs/register/service/client'
        self.__mqtt_ros_register_service_server_topic: str = f'{DOMAIN_NAME}/rs/register/service/server'
        self.__mqtt_ros_register_services_qos: int = 0

    def __declare_parameters(self) -> None:
        self.declare_parameter(name=PARAM_MQTT_BROKER_ADDRESS, value=Parameter.Type.STRING)
        self.declare_parameter(name=PARAM_MQTT_BROKER_PORT, value=Parameter.Type.INTEGER)
        self.declare_parameter(name=PARAM_MQTT_CLIENT_NAME, value=Parameter.Type.STRING)
        self.declare_parameter(name=PARAM_MQTT_CLIENT_KEEP_ALIVE, value=Parameter.Type.INTEGER)
        self.declare_parameter(name=PARAM_MQTT_USER_NAME, value=Parameter.Type.STRING)
        self.declare_parameter(name=PARAM_MQTT_USER_PASSWORD, value=Parameter.Type.STRING)

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


def main(args=None) -> None:
    rclpy.init(args=args)

    try:
        node: Node = Bridge()
        node_name: str = node.get_name()
        multi_threaded_executor: MultiThreadedExecutor = MultiThreadedExecutor()
        multi_threaded_executor.add_node(node=node)
        multi_threaded_executor.spin()
    except ROSInterruptException as rie:
        node.get_logger().warn(
            f'===== {node_name} terminated with Ctrl-C {rie} =====')
        node.mqtt_client.disconnect()
        node.mqtt_client.loop_stop()

    node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()
