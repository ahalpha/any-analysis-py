from atools.io_tools import IOReader


class NetReader(IOReader):
    @property
    def varint(self) -> int:
        result = 0
        bit = 0
        while True:
            if self.offset == self.size:
                if bit != 0:
                    raise IOError(f"Unexpected end of input while reading varint, the file may be incomplete.")
                return None
            byte = self.u8
            result |= (byte & 0x7F) << bit
            bit += 7
            if not (byte & 0x80):
                if byte == 0 and bit != 7:
                    raise IOError(f"Abnormal termination while reading varint, the file might not be in protobuf format.")
                return result
