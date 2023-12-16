import importlib

from rclpy.node import Node
from rclpy.impl.rcutils_logger import RcutilsLogger

from typing import Any


def lookup_ros_message(node: Node, module_name: str, module_class_name: str) -> Any:
    log: RcutilsLogger = node.get_logger()
    
    log.info(f'lookup object module_name : {module_name}')
    log.info(f'lookup object module_class_name : {module_class_name}')
    
    message_path = importlib.import_module(name=module_name, package=node.get_name())
    message_object: Any = getattr(message_path, module_class_name)

    return message_object

def import_module(node: Node, ros_message_type_split: list) -> Any:
    log: RcutilsLogger = node.get_logger()
    
    ros_message_package_moudle: Any = importlib.import_module(f'{ros_message_type_split[0]}.{ros_message_type_split[1]}')
    log.info(f'import_module : {ros_message_package_moudle}')
        
    return ros_message_package_moudle

__all__ = ['rmcl_ros_utils']