import serial
import socket
import threading
import time

class PanasonicPlc:
    __conn = None
    __address = ()
    __cnnType = ''
    devId = 1
    __lock = threading.Lock()

    def __init__(self,connType,args):
        self.__cnnType = connType
        if connType == 'serial':
            self.__conn = serial.Serial(port= args[0],baudrate= args[1],bytesize=args[2] or serial.EIGHTBITS,parity=args[3] or serial.PARITY_NONE,stopbits=args[4] or serial.STOPBITS_ONE)
        elif connType == 'socket':
            self.__conn = socket.socket()
            self.__address = (args[0],args[1])
            
