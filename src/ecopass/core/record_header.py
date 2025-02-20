import enum
import mmap
from typing import Tuple

class RecordType(enum.Enum):
    SYSTEM_RECORD = 1
    DELETED_RECORD = 2
    SYSTEM_RESERVED = 3
    NORMAL_USER_DATA = 4
    POINTER_RECORD = 6
    USER_DATA_RECORD = 7
    RESERVED_FOR_FUTURE_USE = 9

class RecordHeader():
    def __init__(self, filename, record_length: list = [], header_size: int = 2, alignment: int = 2, debug=False):
        if header_size not in {2, 4}:
            raise ValueError("Only 2-byte or 4-byte headers are supported")
        
        self.filename = filename
        self.header_size = header_size
        self.record_length = record_length
        # -------------------------------------------------------------------------------------------------------------
        # for now we will display all record since we only discover just two types [system reserved] & [normal user data]
        # __noraml_user_data__ & __system reserved__
        # -------------------------------------------------------------------------------------------------------------
        self.allowed_statuses = [
            RecordType.NORMAL_USER_DATA.value,
            RecordType.SYSTEM_RESERVED.value,
            RecordType.USER_DATA_RECORD.value,
            RecordType.RESERVED_FOR_FUTURE_USE.value,
            RecordType.SYSTEM_RECORD.value,
            RecordType.POINTER_RECORD.value,
        ]
        self.alignment = alignment
        self.file = None        # Will be set in __enter__
        self.mmap_obj = None    # Will be set in __enter__
        self.debug = debug      # Debug mode flag __track_errors & __fix_bugs__


    def __enter__(self):
        """Opens the file and creates a memory map for it."""
        self.file = open(self.filename, 'rb')
        self.mmap_obj = mmap.mmap(self.file.fileno(), length=0, access=mmap.ACCESS_READ)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Closes the memory map and file when exiting the context."""
        self._close()

    def _parse_header(self, header_bytes: bytes) -> Tuple[int, int]:
        if self.header_size == 2:
            byte0, byte1 = header_bytes
            # Extract status (first 4 bits of byte0)
            status = (byte0 >> 4) & 0x0F
            # Extract length (remaining 12 bits)
            length = ((byte0 & 0x0F) << 8) | byte1
        elif self.header_size == 4:
            # Extract status (first 4 bits of byte0)
            byte0, byte1, byte2, byte3 = header_bytes
            status = (byte0 >> 4) & 0x0F
            # Extract length (remaining 12 bits)
            length = ((byte0 & 0x0F) << 24) | (byte1 << 16) | (byte2 << 8) | byte3

        return status, length
    
    #--------------------------------------------
    # Calculate padding
    #--------------------------------------------
    def _parse_padding(self, length: int):
        # --------------------------------------------
        # Example :
        # (2 - (107 + 2 % 2)) % 2
        # --------------------------------------------
        # if length % 2 != 0:
        #     f.seek(1, 1)

        total_header_data = self.header_size + length
        padding_size = (self.alignment - (total_header_data % self.alignment)) % self.alignment
        return padding_size

    def _close(self):
        if self.mmap_obj:
            self.mmap_obj.close()
        if self.file:
            self.file.close()

    def read_records(self):
        try:
            f = self.mmap_obj
            while True:
                # Read the 2-byte or 4-byte header
                pos = f.tell()
                header = f.read(self.header_size)


                if self.debug:
                    print(f"\n[DEBUG] Reading header at position {pos}: bytes={header.hex()}")


                if len(header) < self.header_size:
                    self._close()
                    if self.debug:
                        print("[DEBUG] Reached end of file (incomplete header)")
                    break  # End of file
                
                status, length = self._parse_header(header)

                # if self.debug:
                #     print(f"[DEBUG] Parsed -> status={status}, length={length}")
                #     print(f"[DEBUG] Expected data length: {length} bytes")

                # Read the record data
                data = f.read(length)
                actual_data_length = len(data)
                
                if actual_data_length < length:
                    self._close()
                    if self.debug:
                        print(f"[DEBUG] ERROR: Incomplete data at {pos}")
                        print(f"[DEBUG] Expected {length} bytes, got {actual_data_length}")
                    raise ValueError(f"Incomplete data at position {pos}")
                #--------------------------------------------
                # return each iterator
                #--------------------------------------------
                # if status in self.allowed_statuses and length in self.record_length:
                if status in self.allowed_statuses:
                    yield length, bytes(data)

                padding_size = self._parse_padding(length=length)
                f.seek(padding_size, 1)
        except:
            self._close()
            raise ValueError(f"Incomplete data at position {pos}")
        finally:
            ...