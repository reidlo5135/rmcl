import socket
import paho.mqtt.client as mqtt

from rclpy.impl.rcutils_logger import RcutilsLogger
from typing import Any

from .domain import MqttConnectionInfo

class Client:

    def __init__(self, log: RcutilsLogger, mqtt_connection_info: MqttConnectionInfo) -> None:
        self.__log: RcutilsLogger = log
            
        self.broker_address: str = mqtt_connection_info.broker_address
        self.broker_port: int = mqtt_connection_info.broker_port
        self.__client_name: str = mqtt_connection_info.client_name
        self.__client_keep_alive: int = mqtt_connection_info.client_keep_alive
        self.__user_name: str = mqtt_connection_info.user_name
        self.__user_password: str = mqtt_connection_info.user_password
        
        self.is_connected: bool = False


    def check_broker_opened(self) -> bool:
        try:
            sock: socket = socket.create_connection(
                address = (self.broker_address, self.broker_port),
                timeout = None
            )
            sock.close()
            return True
        except socket.error:
            pass
        
        return False
        

    def connect(self) -> None:
        try:
            self.client = mqtt.Client(self.__client_name, clean_session = True, userdata = None, transport = 'tcp')
            self.client.username_pw_set(username=self.__user_name, password=self.__user_password)
            
            self.client.on_connect = self.__on_connect
            self.client.on_disconnect = self.__on_disconnect
            self.client.on_message = self.__on_message
            self.client.connect(self.broker_address, self.broker_port, self.__client_keep_alive)
            
            if self.client.is_connected:
                self.__log.info(f'MQTT Client is connected to [{self.broker_address}:{self.broker_port}]')
                self.is_connected = self.client.is_connected
            else:
                self.__log.error('MQTT failed to connect')
                self.is_connected = self.client.is_connected
        except OSError as ose:
            self.__log.error(f'MQTT OSError : {ose}')
        except Exception as e:
            self.__log.error(f'MQTT Error : {e}')
    

    def run(self) -> None:
        if self.is_connected:
            self.__log.info('MQTT Client is running')
            self.client.loop_start()
        else:
            self.__log.error('MQTT Client is not connected to broker')
            return


    def rerun(self) -> None:
        self.client.disconnect()
        self.client.loop_stop()
        self.run()


    def __on_connect(self, client: Any, user_data: Any, flags: Any, rc: Any) -> None:
        
        if rc == 0:
            self.__log.info(f'MQTT connection succeeded result code : [{str(rc)}]')
        else:
            self.__log.error(f'MQTT connection failed result code : [{str(rc)}] ')
            
    
    def __on_disconnect(self, client: Any, user_data: Any, rc: Any) -> None:
        if rc != 0:
            self.__log.error(f'MQTT disconnection result code : [{str(rc)}] ')
            self.rerun()
            

    def __on_message(self, client: Any, user_data: Any, msg: Any) -> None:
        pass
    

    def publish(self, topic: str, payload: Any, qos: int) -> None:
        self.client.publish(topic = topic, payload = payload, qos = qos)


    def subscribe(self, topic: str, qos: int) -> None:
        self.__log.info(f'MQTT granted subscription\n\ttopic : {topic}\n\tqos : {qos}')
        self.client.subscribe(topic = topic, qos = qos)


__all__ = ['mqtt_client']