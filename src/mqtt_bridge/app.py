# -*- coding: utf-8 -*-
from __future__ import absolute_import

import inject
import paho.mqtt.client as mqtt
import rospy
import threading

from .bridge import create_bridge
from .mqtt_client import create_private_path_extractor
from .util import lookup_object

def create_config(mqtt_client, serializer, deserializer, mqtt_private_path):
    if isinstance(serializer, basestring):
        serializer = lookup_object(serializer)
    if isinstance(deserializer, basestring):
        deserializer = lookup_object(deserializer)
    private_path_extractor = create_private_path_extractor(mqtt_private_path)

    def config(binder):
        binder.bind("serializer", serializer)
        binder.bind("deserializer", deserializer)
        binder.bind(mqtt.Client, mqtt_client)
        binder.bind("mqtt_private_path_extractor", private_path_extractor)

    return config


def mqtt_thread(mqtt_client):
    try:
        # load parameters
        params = rospy.get_param("~", {})
        mqtt_params = params.pop("mqtt", {})
        conn_params = mqtt_params.pop("connection")
        mqtt_private_path = mqtt_params.pop("private_path", "")
        bridge_params = params.get("bridge", [])

        # load serializer and deserializer
        serializer = params.get("serializer", "json:dumps")
        deserializer = params.get("deserializer", "json:loads")

        # dependency injection
        config = create_config(mqtt_client, serializer, deserializer, mqtt_private_path)
        inject.configure(config)

        # Configure and connect to MQTT broker
        mqtt_client.on_connect = _on_connect
        mqtt_client.on_disconnect = _on_disconnect
        mqtt_client.connect(**conn_params)
        mqtt_client.sub_topics = []

        # configure bridges
        bridges = []
        for bridge_args in bridge_params:
            bridges.append(create_bridge(**bridge_args))
            if bridge_args["factory"] == "mqtt_bridge.bridge:MqttToRosBridge":
                mqtt_client.sub_topics.append(bridge_args["topic_from"])

        # Start the MQTT client loop
        mqtt_client.loop_forever()

    except Exception as e:
        print("Exception in MQTT thread: {}".format(e))
        rospy.signal_shutdown(e)


def mqtt_bridge_node():
    # init node
    rospy.init_node("mqtt_bridge_node")


    params = rospy.get_param("~", {})
    mqtt_params = params.pop("mqtt", {})
        
    # Create mqtt client
    mqtt_client_factory_name = rospy.get_param(
        "~mqtt_client_factory", ".mqtt_client:default_mqtt_client_factory"
    )
    mqtt_client_factory = lookup_object(mqtt_client_factory_name)
    mqtt_client = mqtt_client_factory(mqtt_params)
        
    # Start the MQTT client loop in a separate thread
    mqtt_thread_instance = threading.Thread(target=mqtt_thread,args=(mqtt_client,))
    mqtt_thread_instance.start()

    # register shutdown callback and spin
    rospy.on_shutdown(mqtt_client.disconnect)
    rospy.on_shutdown(mqtt_client.loop_stop)
    rospy.spin()


def _on_connect(client, userdata, flags, response_code):
    for topic in client.sub_topics:
        client.subscribe(topic)
    rospy.loginfo("MQTT connected")


def _on_disconnect(client, userdata, response_code):
    rospy.loginfo("MQTT disconnected")


__all__ = ["mqtt_bridge_node"]
