import os
from launch import LaunchDescription
from launch_ros.actions import Node
from ament_index_python.packages import get_package_share_directory


def generate_launch_description():
    ld = LaunchDescription()

    package_name: str = 'rmcl'
    executable_name: str = 'rmcl_bridge'
    parameter: str = os.path.join(get_package_share_directory(package_name=package_name), 'config', 'mqtt_connection_info.yaml')

    rmde_executor_node = Node(
        package=package_name,
        executable=executable_name,
        name=executable_name,
        output='screen',
        parameters=[parameter]
    )

    ld.add_action(rmde_executor_node)

    return ld
