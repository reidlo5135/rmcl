import json
import paho.mqtt.client as mqtt

from rclpy.node import Node
from rclpy.impl.rcutils_logger import RcutilsLogger
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.service import Service
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


class Service():

    def __init__(self, _node: Node, _mqtt_client: mqtt_client.Client) -> None:
        self.__node: Node = _node
        self.__mqtt_client: mqtt_client.Client = _mqtt_client
        self.__log: RcutilsLogger = self.__node.get_logger()
        
        self.__mqtt_ros_register_service_topic: str = f'{MQTT_DOMAIN_NAME}/rs/register/service'
        self.__mqtt_ros_service_request_topic_format: str = f'{MQTT_DOMAIN_NAME}/rs/service/request'
        self.__mqtt_ros_service_response_topic_format: str = f'{MQTT_DOMAIN_NAME}/rs/service/response'
        
        self.__ros_service_dict: dict = {}
    

    def wait_for_reception(self) -> None:
        self.__mqtt_client.subscribe(topic=self.__mqtt_ros_register_service_topic, qos=MQTT_SUBSCRIPTION_QOS)
        self.__mqtt_client.client.message_callback_add(sub=self.__mqtt_ros_register_service_topic, callback=self.__register_ros_service)

    
    def __register_ros_service() -> None:
        pass