import json
import paho.mqtt.client as mqtt

from rclpy.node import Node
from rclpy.impl.rcutils_logger import RcutilsLogger
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.qos import qos_profile_system_default
from rclpy import publisher
from rosbridge_library.internal import message_conversion

from typing import Dict
from typing import Any

from ..mqtt import mqtt_client

from .utils import lookup_ros_message
from .utils import import_module

from .qos import MQTT_DOMAIN_NAME
from .qos import MQTT_SUBSCRIPTION_QOS


class Publisher():
    
    def __init__(self, _node: Node, _mqtt_client: mqtt_client.Client) -> None:
        self.__node: Node = _node
        self.__mqtt_client: mqtt_client.Client = _mqtt_client
        self.__log: RcutilsLogger = self.__node.get_logger()
        
        self.__mqtt_ros_register_publisher_topic: str = f'{MQTT_DOMAIN_NAME}/rt/register/publisher'
        self.__mqtt_ros_publisher_publish_topic_format: str = f'{MQTT_DOMAIN_NAME}/rt/publish'
        self.__ros_publisher_dict: dict = {}
    
    def wait_for_reception(self) -> None:
        self.__mqtt_client.subscribe(topic=self.__mqtt_ros_register_publisher_topic, qos=MQTT_SUBSCRIPTION_QOS)
        self.__mqtt_client.client.message_callback_add(sub=self.__mqtt_ros_register_publisher_topic, callback=self.__register_ros_publisher)
    
    def __register_ros_publisher(self, mqtt_client: mqtt.Client, mqtt_user_data: Dict, mqtt_message: mqtt.MQTTMessage) -> None:
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
            
            ros_message_module_name: str = f'{ros_message_type_split[0]}.{ros_message_type_split[1]}'
            ros_message_class_name: str = f'{ros_message_type_split[2]}'

            ros_message_package_module: Any = import_module(node=self.__node, ros_message_type_split=ros_message_type_split)
            ros_message_class: Any = getattr(ros_message_package_module, ros_message_class_name)
            self.__log.info(f'Register Publisher ros_message_obj : {ros_message_class}')

            ros_cb_group: MutuallyExclusiveCallbackGroup = MutuallyExclusiveCallbackGroup()
            ros_created_publisher: publisher.Publisher = self.__node.create_publisher(msg_type=ros_message_class, topic=ros_topic, qos_profile=qos_profile_system_default, callback_group=ros_cb_group)
            self.__log.info(f'Register Publisher ros_created_publisher : {ros_created_publisher.topic_name}')
            
            ros_message_obj: Any = lookup_ros_message(node=self.__node, module_name=ros_message_module_name, module_class_name=ros_message_class_name)

            ros_publisher_dict: dict = {
                'message_obj': ros_message_obj,
                'publisher': ros_created_publisher
            }
            self.__log.info(f'Register Publisher ros_publisher_dict : {ros_publisher_dict}')

            self.__ros_publisher_dict[ros_topic] = ros_publisher_dict
            self.__log.info(f'Register Publisher self.__ros_publisher_dict : {self.__ros_publisher_dict}')

            ros_publisher_publish_mqtt_topic: str = f'{self.__mqtt_ros_publisher_publish_topic_format}/{ros_topic}'

            self.__mqtt_client.subscribe(topic=ros_publisher_publish_mqtt_topic, qos=MQTT_SUBSCRIPTION_QOS)
            self.__mqtt_client.client.message_callback_add(sub=ros_publisher_publish_mqtt_topic, callback=self.__ros_publisher_publish)
        except KeyError as ke:
            self.__log.error(f'Register Publisher Invalid JSON Key in MQTT {mqtt_topic} subscription callback: {ke}')
        except json.JSONDecodeError as jde:
            self.__log.error(f'Register Publisher Invalid JSON format in MQTT {mqtt_topic} subscription callback: {jde.msg}')
        except Exception as e:
            self.__log.error(f'Register Publisher Exception in MQTT {mqtt_topic} subscription callback: {e}')
            raise

    def __ros_publisher_publish(self, mqtt_client: mqtt.Client, mqtt_user_data: Dict, mqtt_message: mqtt.MQTTMessage) -> None:
        try:
            mqtt_topic: str = mqtt_message.topic
            mqtt_json: Any = json.loads(mqtt_message.payload)
            self.__log.info(f'ROS Publisher Publish mqtt_json : {mqtt_json}')
            
            ros_publisher_topic_split: list = mqtt_topic.split('/')
            ros_publisher_topic: str = ros_publisher_topic_split[5]
            
            self.__log.info(f'ROS Publisher Publish ros_publisher_topic : {ros_publisher_topic}')
            
            ros_publisher_dict: dict = self.__ros_publisher_dict[ros_publisher_topic]
            self.__log.info(f'ROS Publisher Publish ros_publisher_dict : {ros_publisher_dict}')
            
            ros_message_obj: Any = ros_publisher_dict['message_obj']
            ros_message: Any = message_conversion.populate_instance(mqtt_json, ros_message_obj())
            ros_publisher: publisher.Publisher = ros_publisher_dict['publisher']
            ros_publisher.publish(ros_message)
        except KeyError as ke:
            self.__log.error(f'ROS Publisher Publish Invalid JSON Key in MQTT {mqtt_topic} subscription callback: {ke}')
        except json.JSONDecodeError as jde:
            self.__log.error(f'ROS Publisher Publish Invalid JSON format in MQTT {mqtt_topic} subscription callback: {jde.msg}')
        except Exception as e:
            self.__log.error(f'ROS Publisher Publish Exception in MQTT {mqtt_topic} subscription callback: {e}')
            raise


__all__ = ['rmcl_ros_publisher']