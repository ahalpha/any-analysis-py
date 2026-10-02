from enum import StrEnum


class Channel(StrEnum):
    HTTP = "http"
    TCP = "tcp"
    UDP = "udp"
    WEBSOCKET = "websocket"
