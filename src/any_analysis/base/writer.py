from atools.io_tools import IOWriter


class NetWriter(IOWriter):
    def varint(self, value: int) -> None:
        if value < 0:
            value &= 0xFFFFFFFFFFFFFFFF
        while True:
            byte = value & 0x7F
            value >>= 7
            if value:
                self.u8(byte | 0x80)
            else:
                self.u8(byte)
                return
