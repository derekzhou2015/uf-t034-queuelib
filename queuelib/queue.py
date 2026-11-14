from __future__ import annotations

import json
import os
import sqlite3
import struct
import time
from abc import abstractmethod
from collections import deque
from contextlib import suppress
from pathlib import Path
from typing import Any, BinaryIO, Literal, cast


def _clear(queue: Any) -> None:
    """Remove every item from *queue*, popping them one by one if it does not
    implement ``clear()``."""
    clear = getattr(queue, "clear", None)
    if clear is not None:
        clear()
        return
    while queue.pop() is not None:
        pass


class QueueRecoveryError(ValueError):
    """Raised when a persistent queue cannot be recovered from its on-disk
    state.

    This happens when the bookkeeping files are torn or otherwise invalid,
    for example after a power loss halfway through a write. Queuelib never
    discards the affected data on its own: the queue keeps raising this error
    until its path is removed. After that, using the same path again creates
    a new, empty queue.
    """


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write *payload* to *path* as JSON, replacing the file atomically.

    The replacement is atomic, but queuelib deliberately does not call
    ``fsync()`` (see the README): a power loss can still lose the most recent
    write, in which case the previous bookkeeping stays intact instead of a
    torn file.
    """
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(json.dumps(payload))
        f.flush()
    # On Windows a freshly written file can briefly be held by antivirus
    # scans or the indexer, so retry the rename for a short while.
    for attempt in range(5):
        try:
            tmp.replace(path)
            return
        except PermissionError:  # noqa: PERF203
            if attempt == 4:
                raise
            time.sleep(0.01 * (attempt + 1))


class _BaseQueueMeta(type):
    """
    Metaclass to check queue classes against the necessary interface
    """

    def __instancecheck__(cls, instance: Any) -> bool:
        return cls.__subclasscheck__(  # pylint: disable=no-value-for-parameter
            type(instance)
        )

    def __subclasscheck__(cls, subclass: Any) -> bool:
        return (
            hasattr(subclass, "push")
            and callable(subclass.push)
            and hasattr(subclass, "pop")
            and callable(subclass.pop)
            and hasattr(subclass, "peek")
            and callable(subclass.peek)
            and hasattr(subclass, "close")
            and callable(subclass.close)
            and hasattr(subclass, "__len__")
            and callable(subclass.__len__)
        )


class BaseQueue(metaclass=_BaseQueueMeta):
    @abstractmethod
    def push(self, obj: Any) -> None:
        raise NotImplementedError

    @abstractmethod
    def pop(self) -> Any | None:
        raise NotImplementedError

    @abstractmethod
    def peek(self) -> Any | None:
        raise NotImplementedError

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    def close(self) -> None:
        pass


class FifoMemoryQueue:
    """In-memory FIFO queue, API compliant with FifoDiskQueue."""

    def __init__(self) -> None:
        self.q: deque[Any] = deque()

    def push(self, obj: Any) -> None:
        self.q.append(obj)

    def pop(self) -> Any | None:
        return self.q.popleft() if self.q else None

    def peek(self) -> Any | None:
        return self.q[0] if self.q else None

    def clear(self) -> None:
        self.q.clear()

    def close(self) -> None:
        pass

    def __len__(self) -> int:
        return len(self.q)


class LifoMemoryQueue(FifoMemoryQueue):
    """In-memory LIFO queue, API compliant with LifoDiskQueue."""

    def pop(self) -> Any | None:
        return self.q.pop() if self.q else None

    def peek(self) -> Any | None:
        return self.q[-1] if self.q else None


class FifoDiskQueue:
    """Persistent FIFO queue."""

    szhdr_format = ">L"
    szhdr_size = struct.calcsize(szhdr_format)

    def __init__(self, path: str | os.PathLike[str], chunksize: int = 100000) -> None:
        self.path = str(path)
        qdir = Path(path)
        if not qdir.exists():
            qdir.mkdir(parents=True)
            self.info = self._default_info(chunksize)
            self.chunksize = self.info["chunksize"]
            self._saveinfo(self.info)
        else:
            infopath = self._infopath()
            if infopath.exists():
                self.info = self._loadinfo()
            else:
                # No bookkeeping at all (never closed, or a cleanup interrupted
                # by a crash): nothing on disk can be trusted, start empty.
                self._remove_chunk_files()
                self.info = self._default_info(chunksize)
            self.chunksize = self.info["chunksize"]
            if self.info["open"]:
                self._recover()
            self.info["open"] = True
            self._saveinfo(self.info)
        self.headf = self._openchunk(self.info["head"][0], "ab+")
        self.tailf = self._openchunk(self.info["tail"][0])
        os.lseek(self.tailf.fileno(), self.info["tail"][2], os.SEEK_SET)

    def push(self, string: bytes) -> None:
        if not isinstance(string, bytes):
            raise TypeError(f"Unsupported type: {type(string).__name__}")
        hnum, hpos = self.info["head"]
        hpos += 1
        szhdr = struct.pack(self.szhdr_format, len(string))
        os.write(self.headf.fileno(), szhdr + string)
        if hpos == self.chunksize:
            hpos = 0
            hnum += 1
            self.headf.close()
            self.headf = self._openchunk(hnum, "ab+")
        self.info["size"] += 1
        self.info["head"] = [hnum, hpos]

    def _openchunk(self, number: int, mode: Literal["rb", "ab+"] = "rb") -> BinaryIO:
        return Path(self.path, f"q{number:05d}").open(mode)

    def pop(self) -> bytes | None:
        tnum, tcnt, toffset = self.info["tail"]
        if [tnum, tcnt] >= self.info["head"]:
            return None
        tfd = self.tailf.fileno()
        szhdr = os.read(tfd, self.szhdr_size)
        if not szhdr:
            return None
        (size,) = struct.unpack(self.szhdr_format, szhdr)
        data = os.read(tfd, size)
        tcnt += 1
        toffset += self.szhdr_size + size
        if tcnt == self.chunksize and tnum <= self.info["head"][0]:
            tcnt = toffset = 0
            tnum += 1
            self.tailf.close()
            Path(self.tailf.name).unlink()
            self.tailf = self._openchunk(tnum)
        self.info["size"] -= 1
        self.info["tail"] = [tnum, tcnt, toffset]
        return data

    def peek(self) -> bytes | None:
        tnum, tcnt, _ = self.info["tail"]
        if [tnum, tcnt] >= self.info["head"]:
            return None
        tfd = self.tailf.fileno()
        tfd_initial_pos = os.lseek(tfd, 0, os.SEEK_CUR)
        szhdr = os.read(tfd, self.szhdr_size)
        if not szhdr:
            return None
        (size,) = struct.unpack(self.szhdr_format, szhdr)
        data = os.read(tfd, size)
        os.lseek(tfd, tfd_initial_pos, os.SEEK_SET)
        return data

    def clear(self) -> None:
        self.headf.close()
        self.tailf.close()
        self.info = self._default_info(self.chunksize)
        # Bookkeeping first: a crash at any later step reopens as an empty queue.
        self._saveinfo(self.info)
        self._remove_chunk_files()
        self.headf = self._openchunk(0, "ab+")
        self.tailf = self._openchunk(0)

    def close(self) -> None:
        self.headf.close()
        self.tailf.close()
        self.info["open"] = False
        self._saveinfo(self.info)
        if len(self) == 0:
            self._cleanup()

    def __len__(self) -> int:
        return cast("int", self.info["size"])

    def _default_info(self, chunksize: int) -> dict[str, Any]:
        return {
            "chunksize": chunksize,
            "size": 0,
            "tail": [0, 0, 0],
            "head": [0, 0],
            "open": True,
        }

    def _loadinfo(self) -> dict[str, Any]:
        infopath = self._infopath()
        try:
            info = cast(
                "dict[str, Any]", json.loads(infopath.read_text(encoding="utf-8"))
            )
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise QueueRecoveryError(
                f"Torn or invalid queue bookkeeping in {infopath}"
            ) from exc
        valid = (
            isinstance(info, dict)
            and isinstance(info.get("chunksize"), int)
            and info["chunksize"] > 0
            and self._is_nat_pair(info.get("tail"), 3)
            and self._is_nat_pair(info.get("head"), 2)
        )
        if not valid:
            raise QueueRecoveryError(f"Torn or invalid queue bookkeeping in {infopath}")
        info.setdefault("open", False)
        if not isinstance(info["open"], bool):
            raise QueueRecoveryError(f"Torn or invalid queue bookkeeping in {infopath}")
        return info

    @staticmethod
    def _is_nat_pair(value: Any, length: int) -> bool:
        return (
            isinstance(value, list)
            and len(value) == length
            and all(isinstance(x, int) and x >= 0 for x in value)
        )

    def _recover(self) -> None:
        """Reconcile the chunk files with the bookkeeping of the last clean
        close. Every item pushed by the crashed session becomes unreachable."""
        hnum, hpos = self.info["head"]
        tnum, tcnt, toffset = self.info["tail"]

        self._remove_orphan_chunks(hnum)

        if not self._chunk_path(hnum).exists():
            # The whole accounted range is gone; rebuild as an empty queue.
            self._reset_to_empty()
            return

        try:
            end_off = self._frames_end_offset(hnum, hpos)
        except QueueRecoveryError as exc:
            raise QueueRecoveryError(
                f"Chunk {self._chunk_path(hnum)} does not match its bookkeeping"
            ) from exc
        with self._chunk_path(hnum).open("r+b") as f:
            f.truncate(end_off)

        if tnum != hnum and not self._chunk_path(tnum).exists():
            survivors = [n for n in self._chunk_numbers() if tnum <= n <= hnum]
            if not survivors:
                self._reset_to_empty()
                return
            tnum = min(survivors)
            tcnt = toffset = 0

        if tnum == hnum:
            size = hpos - tcnt
        else:
            size = (hnum - tnum) * self.chunksize - tcnt + hpos
        if size < 0:
            raise QueueRecoveryError("Negative queue size computed during recovery")
        self.info["size"] = size
        self.info["tail"] = [tnum, tcnt, toffset]
        self.info["head"] = [hnum, hpos]

    def _frames_end_offset(self, number: int, frames: int) -> int:
        """Return the byte offset right after *frames* complete size-framed
        items in a chunk, or raise if the chunk holds fewer complete items."""
        offset = 0
        with self._chunk_path(number).open("rb") as f:
            for _ in range(frames):
                szhdr = f.read(self.szhdr_size)
                if len(szhdr) < self.szhdr_size:
                    raise QueueRecoveryError("Truncated chunk header")
                (size,) = struct.unpack(self.szhdr_format, szhdr)
                data = f.read(size)
                if len(data) < size:
                    raise QueueRecoveryError("Truncated chunk item")
                offset += self.szhdr_size + size
        return offset

    def _reset_to_empty(self) -> None:
        self._remove_chunk_files()
        self.info["size"] = 0
        self.info["tail"] = [0, 0, 0]
        self.info["head"] = [0, 0]

    def _chunk_path(self, number: int) -> Path:
        return Path(self.path, f"q{number:05d}")

    def _chunk_numbers(self) -> list[int]:
        numbers = []
        for chunk in Path(self.path).glob("q*"):
            suffix = chunk.name[1:]
            if len(suffix) == 5 and suffix.isdigit():
                numbers.append(int(suffix))
        return numbers

    def _remove_chunk_files(self) -> None:
        for number in self._chunk_numbers():
            self._chunk_path(number).unlink()

    def _remove_orphan_chunks(
        self, head_number: int, chunk_numbers: list[int] | None = None
    ) -> None:
        numbers = self._chunk_numbers() if chunk_numbers is None else chunk_numbers
        for number in numbers:
            if number > head_number:
                self._chunk_path(number).unlink()

    def _saveinfo(self, info: dict[str, Any]) -> None:
        _atomic_write_json(self._infopath(), info)

    def _infopath(self) -> Path:
        return Path(self.path, "info.json")

    def _cleanup(self) -> None:
        infopath = self._infopath()
        infopath.with_name(infopath.name + ".tmp").unlink(missing_ok=True)
        infopath.unlink(missing_ok=True)
        self._remove_chunk_files()
        with suppress(OSError):
            Path(self.path).rmdir()


class LifoDiskQueue:
    """Persistent LIFO queue."""

    SIZE_FORMAT = ">L"
    SIZE_SIZE = struct.calcsize(SIZE_FORMAT)
    META_SUFFIX = ".qmeta"

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.size: int
        self.path = str(path)
        qfile = Path(path)
        meta = self._meta_path()
        if not qfile.exists():
            if meta.exists():
                payload = self._load_meta()
                if payload != {"size": 0, "eof": self.SIZE_SIZE, "open": False}:
                    raise QueueRecoveryError(
                        f"Queue data file {qfile} is missing but its bookkeeping remains"
                    )
                meta.unlink()
            self.f = qfile.open("wb+")
            self.f.write(struct.pack(self.SIZE_FORMAT, 0))
            self.f.flush()
            self.size = 0
            self._write_meta(self._meta_payload(True))
            return

        if not meta.exists():
            # Queues written by queuelib <= 1.10 have no sidecar file: import
            # the legacy format after validating every frame it claims to hold.
            self.size = self._import_legacy(qfile)
            self.f = qfile.open("rb+")
            self.f.seek(0, os.SEEK_END)
            self._write_meta(self._meta_payload(True))
            return

        payload = self._load_meta()
        header_size = self._read_header_size(qfile)
        if not payload["open"]:
            if header_size != payload["size"] or qfile.stat().st_size != payload["eof"]:
                raise QueueRecoveryError(
                    f"Queue data file {qfile} does not match its bookkeeping"
                )
            self.f = qfile.open("rb+")
            self.f.seek(0, os.SEEK_END)
            self.size = header_size
        else:
            # The previous session crashed. Drop every byte written after the
            # last clean close, so crashed pushes stay unreachable.
            if qfile.stat().st_size < payload["eof"]:
                # A pop() had already truncated the file before the crash;
                # the bookkeeping cannot tell accounted bytes from stale ones.
                raise QueueRecoveryError(
                    f"Queue data file {qfile} shrank below its bookkeeping"
                )
            self.f = qfile.open("rb+")
            self.f.truncate(payload["eof"])
            self.f.seek(0)
            self.f.write(struct.pack(self.SIZE_FORMAT, payload["size"]))
            self.f.flush()
            self.size = payload["size"]
        self._write_meta(self._meta_payload(True))

    def push(self, string: bytes) -> None:
        if not isinstance(string, bytes):
            raise TypeError(f"Unsupported type: {type(string).__name__}")
        self.f.write(string)
        ssize = struct.pack(self.SIZE_FORMAT, len(string))
        self.f.write(ssize)
        self.size += 1

    def pop(self) -> bytes | None:
        if not self.size:
            return None
        self.f.seek(-self.SIZE_SIZE, os.SEEK_END)
        (size,) = struct.unpack(self.SIZE_FORMAT, self.f.read())
        self.f.seek(-size - self.SIZE_SIZE, os.SEEK_END)
        data = self.f.read(size)
        self.f.seek(-size, os.SEEK_CUR)
        self.f.truncate()
        self.size -= 1
        return data

    def peek(self) -> bytes | None:
        if not self.size:
            return None
        self.f.seek(-self.SIZE_SIZE, os.SEEK_END)
        (size,) = struct.unpack(self.SIZE_FORMAT, self.f.read())
        self.f.seek(-size - self.SIZE_SIZE, os.SEEK_END)
        data = self.f.read(size)
        self.f.seek(0, os.SEEK_END)
        return data

    def clear(self) -> None:
        self._write_meta({"size": 0, "eof": self.SIZE_SIZE, "open": True})
        self.f.seek(0)
        self.f.write(struct.pack(self.SIZE_FORMAT, 0))
        self.f.flush()
        self.f.truncate(self.SIZE_SIZE)
        self.f.flush()
        self.size = 0

    def close(self) -> None:
        self.f.flush()
        if self.size:
            self.f.seek(0)
            self.f.write(struct.pack(self.SIZE_FORMAT, self.size))
            self.f.flush()
            self._write_meta(self._meta_payload(False))
            self.f.close()
        else:
            self._write_meta({"size": 0, "eof": self.SIZE_SIZE, "open": False})
            self.f.close()
            Path(self.path).unlink()
            self._meta_path().unlink()

    def __len__(self) -> int:
        return self.size

    def _meta_payload(self, open_: bool) -> dict[str, Any]:
        # In a consistent session the file ends right after the last accounted
        # frame, so the current end position is exactly the recoverable eof.
        return {
            "size": self.size,
            "eof": self.f.seek(0, os.SEEK_END),
            "open": open_,
        }

    def _import_legacy(self, qfile: Path) -> int:
        """Validate a sidecar-less queuelib <= 1.10 file and return its size.

        The legacy layout is a 4-byte item count followed by every item as
        ``data + 4-byte data-length suffix``, so frames are walked backwards
        from the end of the file.
        """
        with qfile.open("rb+") as f:
            header = f.read(self.SIZE_SIZE)
            if len(header) < self.SIZE_SIZE:
                raise QueueRecoveryError(f"Truncated queue header in {qfile}")
            (qsize,) = struct.unpack(self.SIZE_FORMAT, header)
            f.seek(0, os.SEEK_END)
            position = f.tell()
            for _ in range(qsize):
                if position < self.SIZE_SIZE + self.SIZE_SIZE:
                    raise QueueRecoveryError(f"Truncated queue item header in {qfile}")
                f.seek(position - self.SIZE_SIZE)
                szhdr = f.read(self.SIZE_SIZE)
                (frame_size,) = struct.unpack(self.SIZE_FORMAT, szhdr)
                position -= self.SIZE_SIZE
                if position - frame_size < self.SIZE_SIZE:
                    raise QueueRecoveryError(f"Truncated queue item in {qfile}")
                position -= frame_size
            if position != self.SIZE_SIZE:
                raise QueueRecoveryError(
                    f"Queue data file {qfile} has trailing bytes past its items"
                )
        return cast("int", qsize)

    def _read_header_size(self, qfile: Path) -> int:
        try:
            with qfile.open("rb") as f:
                header = f.read(self.SIZE_SIZE)
                if len(header) < self.SIZE_SIZE:
                    raise QueueRecoveryError(f"Truncated queue header in {qfile}")
                (qsize,) = struct.unpack(self.SIZE_FORMAT, header)
        except struct.error as exc:
            raise QueueRecoveryError(f"Truncated queue header in {qfile}") from exc
        return cast("int", qsize)

    def _load_meta(self) -> dict[str, Any]:
        meta = self._meta_path()
        try:
            payload = cast("dict[str, Any]", json.loads(meta.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise QueueRecoveryError(
                f"Torn or invalid queue bookkeeping in {meta}"
            ) from exc
        valid = (
            isinstance(payload, dict)
            and isinstance(payload.get("size"), int)
            and payload["size"] >= 0
            and isinstance(payload.get("eof"), int)
            and payload["eof"] >= self.SIZE_SIZE
            and isinstance(payload.get("open"), bool)
        )
        if not valid:
            raise QueueRecoveryError(
                f"Torn or invalid queue bookkeeping in {meta}"
            )
        return payload

    def _write_meta(self, payload: dict[str, Any]) -> None:
        _atomic_write_json(self._meta_path(), payload)

    def _meta_path(self) -> Path:
        return Path(self.path + self.META_SUFFIX)


class FifoSQLiteQueue:
    _sql_create = "CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY AUTOINCREMENT, item BLOB)"
    _sql_size = "SELECT COUNT(*) FROM queue"
    _sql_push = "INSERT INTO queue (item) VALUES (?)"
    _sql_pop = "SELECT id, item FROM queue ORDER BY id LIMIT 1"
    _sql_del = "DELETE FROM queue WHERE id = ?"
    _sql_clear = "DELETE FROM queue"

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = Path(path).resolve()
        self._db = sqlite3.Connection(self._path, timeout=60)
        self._db.text_factory = bytes
        with self._db as conn:
            conn.execute(self._sql_create)

    def push(self, item: bytes) -> None:
        if not isinstance(item, bytes):
            raise TypeError(f"Unsupported type: {type(item).__name__}")
        with self._db as conn:
            conn.execute(self._sql_push, (item,))

    def pop(self) -> bytes | None:
        with self._db as conn:
            for id_, item in conn.execute(self._sql_pop):
                conn.execute(self._sql_del, (id_,))
                return cast("bytes", item)
        return None

    def peek(self) -> bytes | None:
        with self._db as conn:
            for _, item in conn.execute(self._sql_pop):
                return cast("bytes", item)
        return None

    def clear(self) -> None:
        with self._db as conn:
            conn.execute(self._sql_clear)

    def close(self) -> None:
        size = len(self)
        self._db.close()
        if not size:
            self._path.unlink()

    def __len__(self) -> int:
        with self._db as conn:
            return cast("int", next(conn.execute(self._sql_size))[0])


class LifoSQLiteQueue(FifoSQLiteQueue):
    _sql_pop = "SELECT id, item FROM queue ORDER BY id DESC LIMIT 1"
