import os
from launch import LaunchDescription
from launch_ros.actions import Node
from ament_index_python.packages import get_package_share_directory


def generate_launch_description():
    ld = LaunchDescription()

    package_name: str = 'rmcl'
    executable_name: str = 'rmcl_server'

    rmde_executor_node = Node(
        package=package_name,
        executable=executable_name,
        name=executable_name,
        output='screen',
        parameters=[
            {
                'mqtt_broker_address': 'localhost',
                'mqtt_broker_port': 1883,
                'mqtt_client_name': '',
                'mqtt_client_keep_alive': 60,
                'mqtt_user_name': 'wavem',
                'mqtt_user_password': '1234'
            }
        ]
    )

    ld.add_action(rmde_executor_node)

    return ld
