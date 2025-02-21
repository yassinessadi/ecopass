"""
This module contains the RecordHeader class, which is used to read the header of a record from a file sequentially.
It is used to iterate over the records of a file.

The header is 2 or 4 bytes long.
The first byte is the status and the second byte is the length.
The status is the first 4 bits of the first byte.
The length is the remaining 12 bits of the first byte in case of 2 bytes header.
The length is the remaining 28 bits of the first byte in case of 4 bytes header.

The status is used to determine the type of the record.
The length is used to determine the length of the record.

The record is the data after the header.
The record is aligned to the alignment.

The RecordHeader class is used to read the header of a record and return the length and the data.
It is used to iterate over the records of a file.
"""
import enum
import mmap
from typing import Tuple

class RecordType(enum.Enum):
    SYSTEM_RECORD_DUPLICATE = 1  # A system record (duplicate occurrence)
    DELETED_RECORD = 2  # Deleted record (available for reuse)
    SYSTEM_RESERVED = 3  # System record
    NORMAL_USER_DATA = 4  # Normal user data record
    REDUCED_USER_DATA = 5  # Reduced user data record (indexed files only)
    POINTER_RECORD = 6  # Pointer record (indexed files only)
    USER_DATA_RECORD = 7  # User data record referenced by a pointer
    REDUCED_USER_DATA_REFERENCED_BY_POINTER = 8  # Reduced user data record referenced by a pointer
    RESERVED_FOR_FUTURE_USE = 9  # Reserved for future use
    MID_TRANSACTION_USER_RECORD = 10  # Mid-transaction user data record
    MID_TRANSACTION_REDUCED_USER_RECORD = 11  # Mid-transaction reduced user data record
    MID_TRANSACTION_USER_RECORD_REFERENCED = 12  # Mid-transaction user record referenced by a pointer
    MID_TRANSACTION_REDUCED_USER_RECORD_REFERENCED = 13  # Mid-transaction reduced user record referenced by a pointer
    

class RecordHeader():
    """
    RecordHeader is a class that reads the header of a record.
    It is used to read the header of a record and return the length and the data.
    It is used to iterate over the records of a file.
    """
    def __init__(self, filename, record_length: list = [], header_size: int = 2, alignment: int = 2, debug=False):
        """
        Initialize the RecordHeader class.
        """
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
            RecordType.SYSTEM_RECORD_DUPLICATE.value,
            RecordType.POINTER_RECORD.value,
            RecordType.REDUCED_USER_DATA.value,
            RecordType.MID_TRANSACTION_USER_RECORD.value,
            RecordType.MID_TRANSACTION_REDUCED_USER_RECORD.value,
            RecordType.MID_TRANSACTION_USER_RECORD_REFERENCED.value,
            RecordType.MID_TRANSACTION_REDUCED_USER_RECORD_REFERENCED.value,
        ]
        self.alignment = alignment
        self.file = None        # Will be set in __enter__
        self.mmap_obj = None    # Will be set in __enter__
        self.debug = debug      # Debug mode flag __track_errors & __fix_bugs__

    #--------------------------------------------
    # open the file and create a memory map for it
    # when the context is __enter__ the file is opened and the memory map is created
    #--------------------------------------------
    def __enter__(self):
        """Opens the file and creates a memory map for it."""
        self.file = open(self.filename, 'rb')
        self.mmap_obj = mmap.mmap(self.file.fileno(), length=0, access=mmap.ACCESS_READ)
        return self

    #--------------------------------------------
    # close the memory map and file when exiting the context
    #--------------------------------------------
    def __exit__(self, exc_type, exc_value, traceback):
        """
        Closes the memory map and file when exiting the context.
        """
        self._close()

    #--------------------------------------------
    # parse the header
    #--------------------------------------------
    def _parse_header(self, header_bytes: bytes) -> Tuple[int, int]:
        """
        Parse the header bytes into a status and length.
        The header is 2 or 4 bytes long.
        The first byte is the status and the second byte is the length.
        - The status is the first 4 bits of the first byte.
        - The length is the remaining 12 bits of the first byte in case of 2 bytes header.
        - The length is the remaining 28 bits of the first byte in case of 4 bytes header.
        """
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
        """
        Calculate the padding size.
        The padding size is the difference between the total header data and the alignment.
        """ 
        # --------------------------------------------
        # Example :
        # (2 - (107 + 2 % 2)) % 2
        # --------------------------------------------
        # if length % 2 != 0:
        #     f.seek(1, 1)

        total_header_data = self.header_size + length
        padding_size = (self.alignment - (total_header_data % self.alignment)) % self.alignment
        return padding_size
    
    #--------------------------------------------
    # close the file and the memory map
    #--------------------------------------------
    def _close(self):
        """
        Close the file and the memory map.
        """
        if self.mmap_obj:
            self.mmap_obj.close()
        if self.file:
            self.file.close()

    #--------------------------------------------
    # read the records
    #--------------------------------------------
    def read_records(self):
        """
        Read the records from the memory map.
        - Read the header
        - Read the data
        - Calculate the padding
        - Seek to the next record
        - Return the length and the data
        """
        try:
            f = self.mmap_obj
            while True:
                #-------------------------------------------------------------------
                # Read the 2-byte or 4-byte header
                #-------------------------------------------------------------------
                pos = f.tell()
                header = f.read(self.header_size)

                #-------------------------------------------------------------------
                # if the header is not complete, break
                # when the header is not complete, the header is not a valid header
                # reached the end of the file
                #-------------------------------------------------------------------
                if len(header) < self.header_size:
                    if self.debug:
                        print("--------------------------------")
                        print(f"remaining bytes: {f.read(100).hex()}")
                        print(f"header len: {len(header)}, expected header size: {self.header_size}")
                        print(f"\n[DEBUG] Reading header at position {pos}: bytes={header.hex()}")
                        print("[DEBUG] Reached end of file (incomplete header)")
                        print("--------------------------------")
                    break  # End of file

                status, length = self._parse_header(header)

                #-------------------------------------------------------------------
                # if the status is not valid, break
                #-------------------------------------------------------------------
                if status not in self.allowed_statuses:
                    if self.debug:
                        print(f"remaining bytes: {f.read(self.header_size)}")
                        print("--------------------------------")
                        print(f"[DEBUG] Invalid status: {status}")
                        print("--------------------------------")
                
                #-------------------------------------------------------------------------------------
                # used as a example to stop the iteration
                #-------------------------------------------------------------------------------------
                # if header == b'@k' or header == b'@L':
                #     breakSS

                #-------------------------------------------------------------------------------------
                # enable debug mode only for simple test since will print a lot of data in the console
                #-------------------------------------------------------------------------------------
                if status in self.allowed_statuses:
                    if self.debug:
                        print("--------------------------------")
                        print(f"[DEBUG] Parsed -> status={status}, length={length}")
                        print(f"[DEBUG] Record type: {RecordType(status).name}")
                        print(f"[DEBUG] Expected data length: {length} bytes")
                        print("--------------------------------")

                #-------------------------------------------------------------------------------------  
                # Read the record data
                #-------------------------------------------------------------------------------------
                data = f.read(length)
                actual_data_length = len(data)
                
                #-------------------------------------------------------------------------------------
                # if the data is not complete, raise an error
                #-------------------------------------------------------------------------------------
                if actual_data_length < length:
                    self._close()
                    if self.debug:
                        print("\n--------------------------------")
                        print(f"[DEBUG] ERROR: Incomplete data at {pos}")
                        print(f"[DEBUG] Expected {length} bytes, got {actual_data_length}")
                        print("\n--------------------------------")
                    raise ValueError(f"Incomplete data at position {pos}")
                
                #-------------------------------------------------------------------------------------
                # return each iterator
                #-------------------------------------------------------------------------------------
                # if status in self.allowed_statuses and length in self.record_length:
                #-------------------------------------------------------------------------------------
                if status in self.allowed_statuses:
                    yield length, bytes(data)

                #-------------------------------------------------------------------------------------
                # calculate the padding size
                #-------------------------------------------------------------------------------------
                padding_size = self._parse_padding(length=length)
                f.seek(padding_size, 1)
        except:
            #-------------------------------------------------------------------------------------
            # close the file and raise an error
            # stop the iteration
            #-------------------------------------------------------------------------------------
            self._close()
            raise ValueError(f"Incomplete data at position {pos}")
        finally:
            ...
