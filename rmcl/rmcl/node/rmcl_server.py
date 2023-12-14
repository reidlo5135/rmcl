import json
import time
import importlib
import paho.mqtt.client as mqtt

from rclpy.node import Node
from rclpy.parameter import Parameter
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.publisher import Publisher
from rclpy.subscription import Subscription
from rclpy.client import Client
from rclpy.service import Service
from rclpy.qos import qos_profile_system_default
from rclpy.impl.rcutils_logger import RcutilsLogger
from rosbridge_library.internal import message_conversion

from typing import Final
from typing import Dict
from typing import Any

from ..mqtt import mqtt_client
from ..mqtt.domain import MqttConnectionInfo


PARAM_MQTT_BROKER_ADDRESS: Final = 'broker_address'
PARAM_MQTT_BROKER_PORT: Final = 'broker_port'
PARAM_MQTT_CLIENT_NAME: Final = 'client_name'
PARAM_MQTT_CLIENT_KEEP_ALIVE: Final = 'client_keep_alive'
PARAM_MQTT_USER_NAME: Final = 'user_name'
PARAM_MQTT_USER_PASSWORD: Final = 'user_password'

MQTT_RETRY_INTERVAL: int = 1

RCLPY_NODE_NAME: Final = 'rmcl_server'
DOMAIN_NAME: Final = 'net/wavem/robotics'

class RmclServer(Node):
    
    def __init__(self) -> None:
        super().__init__(RCLPY_NODE_NAME)
        self.__log: RcutilsLogger = self.get_logger()
        self.__declare_parameters()
        
        mqtt_connection_info: MqttConnectionInfo = self.__set_connection_info_by_parameters()
        self.mqtt_client: mqtt_client.Client = mqtt_client.Client(log=self.__log, mqtt_connection_info=mqtt_connection_info)
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
        
        self.__mqtt_ros_register_publisher_topic: str = f'{DOMAIN_NAME}/rt/register/publisher'
        self.__mqtt_ros_publisher_publish_topic_format: str = f'{DOMAIN_NAME}/rt/publish'
        
        self.__mqtt_ros_publisher_dict: dict = {}
        
        self.__mqtt_ros_register_subscription_topic: str = f'{DOMAIN_NAME}/rt/register/subscription'
        self.__mqtt_ros_register_topics_qos: int = 0
        
        self.__mqtt_ros_register_service_client_topic: str = f'{DOMAIN_NAME}/rs/register/service/client'
        self.__mqtt_ros_register_service_server_topic: str = f'{DOMAIN_NAME}/rs/register/service/server'
        self.__mqtt_ros_register_services_qos: int = 0
        
        self.__register_ros_publisher()

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

    def __rclpy_lookup_messages(self, module_name: str, module_class_name: str) -> Any:
        self.__log.info(f'lookup object module_name : {module_name}')
        self.__log.info(f'lookup object module_class_name : {module_class_name}')

        message_path = importlib.import_module(name=module_name, package=self.get_name())
        message_object: Any = getattr(message_path, module_class_name)

        return message_object
    
    def __register_ros_publisher(self) -> None:
        def __mqtt_register_ros_publisher_subscription_cb(mqtt_client: mqtt.Client, mqtt_user_data: Dict, mqtt_message: mqtt.MQTTMessage) -> None:
            try:
                mqtt_topic: str = mqtt_message.topic
                mqtt_json: Any = json.loads(mqtt_message.payload)
                self.__log.info(f'Register Publisher mqtt_json : {mqtt_json}')
                
                ros_topic: str = mqtt_json['topic']
                if '/' in ros_topic:
                    ros_topic = ros_topic.split('/')[1]
                ros_message_type: str = mqtt_json['message_type']
                ros_qos: int = mqtt_json['qos']
                
                ros_message_type_split: list = ros_message_type.split('.')
                self.__log.info(f'Register Publisher ros_message_type_split : {ros_message_type_split}')
                
                ros_message_obj: Any = self.__rclpy_lookup_messages(module_name=f'{ros_message_type_split[0]}.{ros_message_type_split[1]}', module_class_name=f'{ros_message_type_split[2]}')
                self.__log.info(f'Register Publisher ros_message_obj : {ros_message_obj}')
                
                ros_cb_group: MutuallyExclusiveCallbackGroup = MutuallyExclusiveCallbackGroup()
                ros_created_publisher: Publisher = self.create_publisher(msg_type=ros_message_obj(), topic=ros_topic, qos_profile=qos_profile_system_default, callback_group=ros_cb_group)
                self.__log.info(f'Register Publisher ros_created_publisher : {ros_created_publisher.topic_name}')
                
                ros_publisher_dict: dict = {
                    'message_obj': ros_message_obj,
                    'publisher': ros_created_publisher
                }
                self.__log.info(f'Register Publisher ros_publisher_dict : {ros_publisher_dict}')
                
                self.__mqtt_ros_publisher_dict[ros_topic] = ros_publisher_dict
                self.__log.info(f'Register Publisher self.__mqtt_ros_publisher_dict : {self.__mqtt_ros_publisher_dict}')
                
                ros_create_publisher_mqtt_topic: str = f'{self.__mqtt_ros_publisher_publish_topic_format}/{ros_topic}'
                    
                self.mqtt_client.subscribe(topic=ros_create_publisher_mqtt_topic, qos=self.__mqtt_ros_register_topics_qos)
                self.mqtt_client.client.message_callback_add(sub=ros_create_publisher_mqtt_topic, callback=self.__mqtt_ros_publisher_publish_subscription_cb)
            except KeyError as ke:
                self.__log.error(f'Register Publisher Invalid JSON Key in MQTT {mqtt_topic} subscription callback: {ke}')

            except json.JSONDecodeError as jde:
                self.__log.error(f'Register Publisher Invalid JSON format in MQTT {mqtt_topic} subscription callback: {jde.msg}')

            except Exception as e:
                self.__log.error(f'Register Publisher Exception in MQTT {mqtt_topic} subscription callback: {e}')
                raise
            
        self.mqtt_client.subscribe(topic=self.__mqtt_ros_register_publisher_topic, qos=self.__mqtt_ros_register_topics_qos)
        self.mqtt_client.client.message_callback_add(sub=self.__mqtt_ros_register_publisher_topic, callback=__mqtt_register_ros_publisher_subscription_cb)

    def __ros_publisher_publish(self, data: Any) -> None:
        pass
    
    def __mqtt_ros_publisher_publish_subscription_cb(self, mqtt_client: mqtt.Client, mqtt_user_data: Dict, mqtt_message: mqtt.MQTTMessage) -> None:
        try:
            mqtt_topic: str = mqtt_message.topic
            mqtt_json: Any = json.loads(mqtt_message.payload)
            self.__log.info(f'ROS Publisher Publish mqtt_json : {mqtt_json}')
                
        except KeyError as ke:
            self.__log.error(f'ROS Publisher Publish Invalid JSON Key in MQTT {mqtt_topic} subscription callback: {ke}')

        except json.JSONDecodeError as jde:
            self.__log.error(f'ROS Publisher Publish Invalid JSON format in MQTT {mqtt_topic} subscription callback: {jde.msg}')

        except Exception as e:
            self.__log.error(f'ROS Publisher Publish Exception in MQTT {mqtt_topic} subscription callback: {e}')
            raise
            

__all__ = ['rmcl_node_rmcl_server']