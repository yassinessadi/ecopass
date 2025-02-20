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
        self.allowed_statuses = [
            RecordType.NORMAL_USER_DATA.value,
            RecordType.SYSTEM_RESERVED.value,
            RecordType.USER_DATA_RECORD.value,
            RecordType.RESERVED_FOR_FUTURE_USE.value,
            RecordType.SYSTEM_RECORD.value,
            RecordType.POINTER_RECORD.value,
        ]
        self.alignment = alignment
        self.file = None
        self.mmap_obj = None
        self.debug = debug
        self._reset_debug_counters()

    def _reset_debug_counters(self):
        """Initialize all debug counters."""
        self.records_processed = 0
        self.records_yielded = 0
        self.records_skipped_invalid_status = 0
        self.records_invalid_length = 0
        self.padding_bytes_skipped = 0
        self.invalid_headers = 0

    def __enter__(self):
        self.file = open(self.filename, 'rb')
        self.mmap_obj = mmap.mmap(self.file.fileno(), length=0, access=mmap.ACCESS_READ)
        if self.debug:
            print(f"[DEBUG] Opened file {self.filename}, size: {self.mmap_obj.size()} bytes")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.mmap_obj:
            self.mmap_obj.close()
        if self.file:
            self.file.close()
        if self.debug:
            print("[DEBUG] Closed file and memory map")

    def _parse_header(self, header_bytes: bytes) -> Tuple[int, int]:
        try:
            if self.header_size == 2:
                byte0, byte1 = header_bytes
                status = (byte0 >> 4) & 0x0F
                length = ((byte0 & 0x0F) << 8) | byte1
            elif self.header_size == 4:
                byte0, byte1, byte2, byte3 = header_bytes
                status = (byte0 >> 4) & 0x0F
                length = ((byte0 & 0x0F) << 24) | (byte1 << 16) | (byte2 << 8) | byte3
            return status, length
        except Exception as e:
            if self.debug:
                print(f"[DEBUG] Header parsing error: {e}")
            raise

    def _parse_padding(self, length: int):
        total_header_data = self.header_size + length
        padding_size = (self.alignment - (total_header_data % self.alignment)) % self.alignment
        return padding_size

    def get_debug_info(self):
        """Returns a dictionary of debug counters and metrics."""
        return {
            "records_processed": self.records_processed,
            "records_yielded": self.records_yielded,
            "records_skipped_invalid_status": self.records_skipped_invalid_status,
            "records_invalid_length": self.records_invalid_length,
            "padding_bytes_skipped": self.padding_bytes_skipped,
            "invalid_headers": self.invalid_headers,
        }

    def read_records(self):
        try:
            f = self.mmap_obj
            while True:
                pos = f.tell()
                header = f.read(self.header_size)
                self.records_processed += 1

                if self.debug:
                    print(f"\n[DEBUG] Reading header at position {pos}: bytes={header.hex() if header else 'None'}")

                if len(header) < self.header_size:
                    if self.debug:
                        print("[DEBUG] Reached end of file (incomplete header)")
                    break

                try:
                    status, length = self._parse_header(header)
                    try:
                        record_type = RecordType(status)
                        status_name = record_type.name
                    except ValueError:
                        status_name = f"Unknown status {status}"
                except Exception as e:
                    self.invalid_headers += 1
                    if self.debug:
                        print(f"[DEBUG] Invalid header at {pos}: {e}")
                    raise

                if self.debug:
                    print(f"[DEBUG] Parsed -> status={status_name}, length={length}")
                    remaining = f.size() - f.tell()
                    print(f"[DEBUG] Remaining bytes after header: {remaining}")

                data = f.read(length)
                actual_data_length = len(data)
                if actual_data_length < length:
                    self.records_invalid_length += 1
                    if self.debug:
                        print(f"[DEBUG] ERROR: Incomplete data at {f.tell() - actual_data_length}")
                        print(f"[DEBUG] Expected {length} bytes, got {actual_data_length}")
                    raise ValueError(f"Incomplete data at position {pos}")

                if status in self.allowed_statuses:
                    if self.debug:
                        print(f"[DEBUG] Yielding record at {pos}: {status_name}, length {length}")
                    self.records_yielded += 1
                    yield length, bytes(data)
                else:
                    if self.debug:
                        print(f"[DEBUG] Skipping record at {pos}: {status_name} not allowed")
                    self.records_skipped_invalid_status += 1

                padding_size = self._parse_padding(length)
                if self.debug:
                    print(f"[DEBUG] Skipping {padding_size} padding bytes at position {f.tell()}")
                f.seek(padding_size, 1)
                self.padding_bytes_skipped += padding_size

        except Exception as e:
            error_context = {
                "position": pos,
                "header": header.hex() if 'header' in locals() else None,
                "status": status if 'status' in locals() else None,
                "length": length if 'length' in locals() else None,
            }
            if self.debug:
                print(f"[DEBUG] Exception occurred: {e}")
                print(f"[DEBUG] Error context: {error_context}")
            raise ValueError(f"Error at position {pos}: {str(e)}") from e
        finally:
            if self.debug:
                print("[DEBUG] Processing complete. Summary:")
                for k, v in self.get_debug_info().items():
                    print(f"[DEBUG] {k}: {v}")