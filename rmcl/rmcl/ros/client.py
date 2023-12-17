import json
import paho.mqtt.client as mqtt

from rclpy.node import Node
from rclpy.impl.rcutils_logger import RcutilsLogger
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy import client
from rclpy.qos import qos_profile_services_default
from rclpy.task import Future

from rosbridge_library.internal import message_conversion
from rosbridge_library.internal.ros_loader import get_service_class

from typing import Dict
from typing import Any
from typing import List

from ..mqtt import mqtt_client

from .utils import lookup_ros_message
from .utils import import_module

from .qos import MQTT_DOMAIN_NAME
from .qos import MQTT_PUBLISHER_QOS
from .qos import MQTT_SUBSCRIPTION_QOS


class Client():
    
    def __init__(self, _node: Node, _mqtt_client: mqtt_client.Client) -> None:
        self.__node: Node = _node
        self.__mqtt_client: mqtt_client.Client = _mqtt_client
        self.__log: RcutilsLogger = self.__node.get_logger()
        
        self.__mqtt_ros_register_client_topic: str = f'{MQTT_DOMAIN_NAME}/rs/register/client'
        self.__mqtt_ros_client_request_topic_format: str = f'{MQTT_DOMAIN_NAME}/rs/request'
        self.__mqtt_ros_client_response_topic_format: str = f'{MQTT_DOMAIN_NAME}/rs/response'
        
        self.__ros_client_dict: dict = {}

    def wait_for_reception(self) -> None:
        self.__mqtt_client.subscribe(topic=self.__mqtt_ros_register_client_topic, qos=MQTT_SUBSCRIPTION_QOS)
        self.__mqtt_client.client.message_callback_add(sub=self.__mqtt_ros_register_client_topic, callback=self.__register_ros_client)

    def __register_ros_client(self, mqtt_client: mqtt.Client, mqtt_user_data: Dict, mqtt_message: mqtt.MQTTMessage) -> None:
        try:
            mqtt_topic: str = mqtt_message.topic
            mqtt_json: Any = json.loads(mqtt_message.payload)
            self.__log.info(f'Register Client mqtt_json : {mqtt_json}')

            ros_service_name: str = mqtt_json['service_name']
            if '/' in ros_service_name:
                ros_service_name = ros_service_name.split('/')[1]

            ros_service_type: str = mqtt_json['service_type']
            ros_qos: int = mqtt_json['qos']

            ros_message_type_split: list = ros_service_type.split('.')
            self.__log.info(f'Register Client ros_message_type_split : {ros_message_type_split}')
            
            ros_message_module_name: str = f'{ros_message_type_split[0]}.{ros_message_type_split[1]}'
            ros_message_class_name: str = f'{ros_message_type_split[2]}'

            ros_message_package_module: Any = import_module(node=self.__node, ros_message_type_split=ros_message_type_split)
            ros_service_class: Any = getattr(ros_message_package_module, ros_message_class_name)
            self.__log.info(f'Register Client ros_message_obj : {ros_service_class}')

            ros_cb_group: MutuallyExclusiveCallbackGroup = MutuallyExclusiveCallbackGroup()
            ros_created_client: client.Client = self.__node.create_client(srv_type=ros_service_class, srv_name=ros_service_name, qos_profile=qos_profile_services_default, callback_group=ros_cb_group)

            ros_message_obj: Any = lookup_ros_message(node=self.__node, module_name=ros_message_module_name, module_class_name=ros_message_class_name)

            ros_client_dict: dict = {
                'message_obj': ros_message_obj,
                'client': ros_created_client
            }
            self.__log.info(f'Register Client ros_client_dict : {ros_client_dict}')

            self.__ros_client_dict[ros_service_name] = ros_client_dict
            self.__log.info(f'Register Client self.__ros_client_dict : {self.__ros_client_dict}')

            ros_client_request_mqtt_topic: str = f'{self.__mqtt_ros_client_request_topic_format}/{ros_service_name}'

            self.__mqtt_client.subscribe(topic=ros_client_request_mqtt_topic, qos=MQTT_SUBSCRIPTION_QOS)
            self.__mqtt_client.client.message_callback_add(sub=ros_client_request_mqtt_topic, callback=self.__ros_client_request)
        except KeyError as ke:
            self.__log.error(f'Register Client Invalid JSON Key in MQTT {mqtt_topic} Client callback: {ke}')
        except json.JSONDecodeError as jde:
            self.__log.error(f'Register Client Invalid JSON format in MQTT {mqtt_topic} Client callback: {jde.msg}')
        except Exception as e:
            self.__log.error(f'Register Client Exception in MQTT {mqtt_topic} Client callback: {e}')
            raise
    
    def __ros_client_request(self, mqtt_client: mqtt.Client, mqtt_user_data: Dict, mqtt_message: mqtt.MQTTMessage) -> None:
        try:
            mqtt_topic: str = mqtt_message.topic
            mqtt_json: Any = json.loads(mqtt_message.payload)
            self.__log.info(f'ROS Client Request mqtt_json : {mqtt_json}')
            
            ros_client_topic_split: list = mqtt_topic.split('/')
            ros_service_name: str = ros_client_topic_split[5]
            
            self.__log.info(f'ROS Client Request ros_service_name : {ros_service_name}')
            
            ros_client_dict: dict = self.__ros_client_dict[ros_service_name]
            self.__log.info(f'ROS Client Request ros_client_dict : {ros_client_dict}')
            
            ros_message_obj: Any = ros_client_dict['message_obj']
            self.__log.info(f'ROS Client Request ros_message_obj : {ros_message_obj}')

            ros_service_names_and_types: dict = dict(self.__node.get_service_names_and_types())
            ros_service_type: str = ros_service_names_and_types['/' + ros_service_name]
            ros_service_type: str = ros_service_type[0]
            
            self.__log.info(f'ROS Client Request service_names_and_types : {ros_service_names_and_types}')
            self.__log.info(f'ROS Client Request service_type : {ros_service_type}')

            ros_message: Any = get_service_class(typestring=ros_service_type)
            self.__log.info(f'ROS Client Request ros_message : {ros_message}')

            ros_request_message: Any = message_conversion.populate_instance(mqtt_json, ros_message.Request())
            self.__log.info(f'ROS Client Request ros_message_request : {ros_request_message}')

            ros_client: client.Client = ros_client_dict['client']

            ros_service_is_ready: bool = ros_client.wait_for_service(timeout_sec=1.0)
            self.__log.info(f'ROS Client Request ros_service_is_ready : {ros_service_is_ready}')
            
            if not ros_service_is_ready:
                self.__log.error(f'ROS Client Request ros_service is not ready... aborting')
                return
            else:
                ros_client_request_future: Future = ros_client.call_async(ros_request_message)
                self.__log.info(f'ROS Client Request ros_client_request_future : {ros_client_request_future}')
                
                ros_client_request_future_done: bool = ros_client_request_future.done()
                self.__log.info(f'ROS Client Request ros_client_request_future_done : {ros_client_request_future_done}')
                
                if ros_client_request_future_done:
                    ros_client_request_future_result: Any = ros_client_request_future.result()
                    self.__log.info(f'ROS Client Request ros_client_request_future_result : {ros_client_request_future_result}')
                    
                    ros_client_request_future_result_json: Any = json.dumps(message_conversion.extract_values(ros_client_request_future_result))
                    mqtt_response_topic: str = f'{self.__mqtt_ros_client_response_topic_format}/{ros_service_name}'
                    self.__mqtt_client.publish(topic=mqtt_response_topic, payload=ros_client_request_future_result_json, qos=MQTT_PUBLISHER_QOS)
                else:
                    self.__log.error(f'ROS Client Request has been failed... aborting')
                    return
        except KeyError as ke:
            self.__log.error(f'ROS Client Request Invalid JSON Key in MQTT {mqtt_topic} subscription callback: {ke}')
        except json.JSONDecodeError as jde:
            self.__log.error(f'ROS Client Request Invalid JSON format in MQTT {mqtt_topic} subscription callback: {jde.msg}')
        except Exception as e:
            self.__log.error(f'ROS Client Request Exception in MQTT {mqtt_topic} subscription callback: {e}')
            raise


__all__ = ['rmcl_ros_client']