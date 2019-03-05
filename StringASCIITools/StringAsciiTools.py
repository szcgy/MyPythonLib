import crc
import crc16

def XOR(sourceString = ''):
    """
    产生异或校验码
    """
    strlen = len(sourceString)
    if strlen == 0:
        return ''
    xor = 0x00
    for byte in sourceString.encode(encoding = 'ascii'):
        xor ^= byte
    return '%02X'% xor

def CRC16(sourceString = ''):
    return '%04X'%crc16.crc16xmodem(sourceString.encode(encoding = 'ascii'))

if __name__ == '__main__':
    #print(XOR('123312'))
    print(CRC16('123312'))