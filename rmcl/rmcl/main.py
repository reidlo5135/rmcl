import rclpy

from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor
from rclpy.exceptions import ROSInterruptException
from .node.rmcl_server import RmclServer

def main(args=None) -> None:
    rclpy.init(args=args)

    try:
        node: Node = RmclServer()
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
