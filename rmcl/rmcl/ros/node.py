import json
import rclpy.node
import paho.mqtt.client as mqtt

from rclpy.impl.rcutils_logger import RcutilsLogger

from typing import Dict
from typing import Any

from ..mqtt import mqtt_client

from .qos import MQTT_DOMAIN_NAME
from .qos import MQTT_SUBSCRIPTION_QOS

class Node():
    
    def __init__(self, _node: rclpy.node.Node, _mqtt_client: mqtt_client.Client) -> None:
        self.__node: rclpy.node.Node = _node
        self.__mqtt_client: mqtt_client.Client = _mqtt_client
        self.__log: RcutilsLogger = self.__node.get_logger()
        
        self.__mqtt_ros_register_subscription_topic: str = f'{MQTT_DOMAIN_NAME}/rt/register/node'
        self.__ros_subscription_dict: dict = {}
    
    def wait_for_reception(self) -> None:
        self.__mqtt_client.subscribe(topic=self.__mqtt_ros_register_subscription_topic, qos=MQTT_SUBSCRIPTION_QOS)
        self.__mqtt_client.client.message_callback_add(sub=self.__mqtt_ros_register_subscription_topic, callback=self.__register_ros_node)
        
    def __register_ros_node(self, mqtt_client: mqtt.Client, mqtt_user_data: Dict, mqtt_message: mqtt.MQTTMessage) -> None:
        try:
            mqtt_topic: str = mqtt_message.topic
            mqtt_json: Any = json.loads(mqtt_message.payload)
            self.__log.info(f'Register Node mqtt_json : {mqtt_json}')
            
            ros_node_name: str = mqtt_json['node_name']
            ros_created_node: rclpy.node.Node = rclpy.create_node(node_name=ros_node_name)
            
            self.__log.info(f'Register Node ros_created_node : {ros_created_node}')
        except KeyError as ke:
            self.__log.error(f'Register Node Invalid JSON Key in MQTT {mqtt_topic} Node callback: {ke}')
        except json.JSONDecodeError as jde:
            self.__log.error(f'Register Node Invalid JSON format in MQTT {mqtt_topic} Node callback: {jde.msg}')
        except Exception as e:
            self.__log.error(f'Register Node Exception in MQTT {mqtt_topic} Node callback: {e}')
            raise

__all__ = ['rmcl_ros_node']