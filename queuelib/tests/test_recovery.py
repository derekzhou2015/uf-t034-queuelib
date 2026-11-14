from __future__ import annotations

import json
import struct
import subprocess
import sys
import time
from pathlib import Path

import pytest

from queuelib import FifoDiskQueue, LifoDiskQueue, PriorityQueue, QueueRecoveryError
from queuelib.tests import QueuelibTestCase


class FifoRecoveryTest(QueuelibTestCase):
    def test_crashed_pushes_never_overtake_recovery_data(self):
        # No clean close ever happened: the on-disk items are all stale.
        q = FifoDiskQueue(self.qpath, chunksize=2)
        q.push(b"old-1")
        q.push(b"old-2")
        del q
        q = FifoDiskQueue(self.qpath, chunksize=2)
        q.push(b"new-1")
        assert q.pop() == b"new-1"  # stale items must not jump the queue
        q.push(b"new-2")
        q.close()  # a clean close commits the recovery-session data
        q = FifoDiskQueue(self.qpath, chunksize=2)
        assert [q.pop(), q.pop()] == [b"new-2", None]
        q.close()

    def test_recovered_snapshot_items_keep_their_order(self):
        q = FifoDiskQueue(self.qpath, chunksize=2)
        q.push(b"a")
        q.push(b"b")
        q.close()
        q = FifoDiskQueue(self.qpath, chunksize=2)
        q.push(b"c")
        q.push(b"d")
        del q  # crash: c, d must become unreachable, a, b survive
        q = FifoDiskQueue(self.qpath, chunksize=2)
        assert len(q) == 2
        assert [q.pop(), q.pop(), q.pop()] == [b"a", b"b", None]
        q.close()

    def test_close_after_draining_stale_data_keeps_new_data(self):
        # The stale item must belong to a committed snapshot, otherwise the
        # never-closed chunks are discarded wholesale on recovery.
        q = FifoDiskQueue(self.qpath)
        q.push(b"old")
        q.close()
        q = FifoDiskQueue(self.qpath)
        q.push(b"stale")
        del q  # crash: "stale" is unreachable, "old" stays in the snapshot
        q = FifoDiskQueue(self.qpath)
        # Drain the committed snapshot until the ledger reaches zero first;
        # recovery-session pushes must not be destroyed by that close().
        assert q.pop() == b"old"
        assert q.pop() is None
        q.push(b"new-1")
        q.push(b"new-2")
        q.close()
        q = FifoDiskQueue(self.qpath)
        assert [q.pop(), q.pop(), q.pop()] == [b"new-1", b"new-2", None]
        q.close()

    def test_repeated_crash_rounds_match_model(self):
        model: list[bytes] = []
        for round_no in range(5):
            q = FifoDiskQueue(self.qpath, chunksize=3)
            item = f"round-{round_no}".encode()
            q.push(item)
            if round_no % 2 == 0:
                # A clean close commits the item; odd rounds crash instead.
                q.close()
                model.append(item)
            else:
                del q
        q = FifoDiskQueue(self.qpath, chunksize=3)
        left = []
        while True:
            item = q.pop()
            if item is None:
                break
            left.append(item)
        assert left == model
        q.close()

    def test_tail_chunk_deleted_before_crash_is_relocated(self):
        q = FifoDiskQueue(self.qpath, chunksize=2)
        for i in range(5):
            q.push(str(i).encode())
        q.pop()
        q.pop()  # q00000 deleted, tail now points at q00001
        q.close()
        q = FifoDiskQueue(self.qpath, chunksize=2)
        q.push(b"stale")
        del q
        q = FifoDiskQueue(self.qpath, chunksize=2)
        assert len(q) == 3
        assert [q.pop(), q.pop(), q.pop(), q.pop()] == [
            b"2",
            b"3",
            b"4",
            None,
        ]
        q.close()

    def test_tail_relocation_ignores_already_deleted_orphans(self):
        # Tail relocation must re-enumerate chunks after orphans past the head
        # are removed, instead of relocating onto one of those deleted files.
        q = FifoDiskQueue(self.qpath, chunksize=2)
        for i in range(4):
            q.push(str(i).encode())
        q.pop()
        q.pop()  # q00000 gone, tail points at q00001
        q.close()
        q = FifoDiskQueue(self.qpath, chunksize=2)
        # Rotate the head forward and leave an orphan chunk behind.
        q.push(b"x")
        q.push(b"y")
        q.push(b"z")  # creates q00003 as an unfinished head chunk
        del q
        q = FifoDiskQueue(self.qpath, chunksize=2)
        assert len(q) == 2
        assert [q.pop(), q.pop(), q.pop()] == [b"2", b"3", None]
        q.close()

    def test_head_chunk_rotated_and_emptied_resets_to_empty(self):
        q = FifoDiskQueue(self.qpath, chunksize=1)
        q.push(b"a")
        q.push(b"b")
        q.pop()
        q.pop()  # every chunk emptied and deleted
        q.close()
        q = FifoDiskQueue(self.qpath, chunksize=1)
        q.push(b"stale")
        del q
        q = FifoDiskQueue(self.qpath, chunksize=1)
        assert len(q) == 0
        q.push(b"fresh")
        assert q.pop() == b"fresh"
        q.close()

    def test_clear_then_crash_reopens_empty_and_usable(self):
        q = FifoDiskQueue(self.qpath)
        q.push(b"a")
        q.clear()
        del q
        q = FifoDiskQueue(self.qpath)
        assert len(q) == 0
        assert q.pop() is None
        q.push(b"b")
        assert q.pop() == b"b"
        q.close()

    def test_torn_info_json_raises_recovery_error(self):
        q = FifoDiskQueue(self.qpath)
        q.push(b"a")
        q.close()
        for broken in ('{"chunksize": 100000, "size": 1,', b"\xff\xfe{"):
            self.qpath.mkdir(exist_ok=True)
            (self.qpath / "info.json").write_bytes(
                broken if isinstance(broken, bytes) else broken.encode()
            )
            with pytest.raises(QueueRecoveryError) as exc_info:
                FifoDiskQueue(self.qpath)
            assert isinstance(exc_info.value, ValueError)
        # The path can be removed and reused for a new queue.
        for chunk in self.qpath.glob("q*"):
            chunk.unlink()
        (self.qpath / "info.json").unlink()
        q = FifoDiskQueue(self.qpath)
        q.push(b"b")
        assert q.pop() == b"b"
        q.close()

    def test_invalid_info_structure_raises_recovery_error(self):
        q = FifoDiskQueue(self.qpath)
        q.push(b"a")
        q.close()
        for broken in [
            {"size": 1},
            {"chunksize": 0, "size": 0, "tail": [0, 0, 0], "head": [0, 0]},
            {"chunksize": 1, "size": 0, "tail": [0], "head": [0, 0]},
            {"chunksize": 1, "size": 0, "tail": [0, 0, 0], "head": [0]},
        ]:
            (self.qpath / "info.json").write_text(json.dumps(broken))
            with pytest.raises(QueueRecoveryError):
                FifoDiskQueue(self.qpath)

    def test_truncated_head_chunk_raises_recovery_error(self):
        q = FifoDiskQueue(self.qpath)
        q.push(b"a")
        q.push(b"b")
        q.close()
        q = FifoDiskQueue(self.qpath)
        q.push(b"stale")
        del q
        # Damage the accounted range: keep only the first frame's size header,
        # so the bookkeeping's two accounted frames can no longer be parsed.
        (self.qpath / "q00000").write_bytes(struct.pack(">L", 1))
        with pytest.raises(QueueRecoveryError):
            FifoDiskQueue(self.qpath)

    def test_orphan_chunks_without_info_are_removed(self):
        q = FifoDiskQueue(self.qpath)
        q.push(b"a")
        del q  # never closed: no info.json, orphan chunk on disk
        q = FifoDiskQueue(self.qpath)
        assert len(q) == 0
        assert q.pop() is None
        q.push(b"b")
        assert q.pop() == b"b"
        q.close()

    def test_unexpected_files_are_never_touched(self):
        q = FifoDiskQueue(self.qpath)
        q.push(b"a")
        q.close()
        unexpected = self.qpath / "unexpected"
        unexpected.touch()
        q = FifoDiskQueue(self.qpath)
        q.push(b"b")
        del q
        q = FifoDiskQueue(self.qpath)
        assert unexpected.exists()
        q.close()
        assert unexpected.exists()

    def test_no_tmp_file_left_blocks_cleanup(self):
        q = FifoDiskQueue(self.qpath)
        q.push(b"a")
        q.pop()
        q.close()
        assert not self.qpath.exists()


class LifoRecoveryTest(QueuelibTestCase):
    def test_crashed_pushes_are_unreachable_round_after_round(self):
        q = LifoDiskQueue(self.qpath)
        q.push(b"a")
        q.close()
        # Each crashed session only pushes: no pop() truncation before the
        # crash, so every round is recoverable from the sidecar checkpoint.
        for round_no in range(4):
            q = LifoDiskQueue(self.qpath)
            q.push(f"stale-{round_no}".encode())
            del q
        q = LifoDiskQueue(self.qpath)
        assert q.pop() == b"a"
        assert q.pop() is None
        q.close()

    def test_recovery_data_survives_close_with_lifo_order(self):
        q = LifoDiskQueue(self.qpath)
        q.push(b"old-1")
        q.push(b"old-2")
        del q
        q = LifoDiskQueue(self.qpath)
        q.push(b"new-1")
        q.push(b"new-2")
        q.close()
        q = LifoDiskQueue(self.qpath)
        assert [q.pop(), q.pop(), q.pop()] == [b"new-2", b"new-1", None]
        q.close()

    def test_pop_to_zero_then_close_keeps_later_data(self):
        q = LifoDiskQueue(self.qpath)
        q.push(b"a")
        q.close()
        q = LifoDiskQueue(self.qpath)
        q.push(b"stale")
        del q
        q = LifoDiskQueue(self.qpath)
        assert q.pop() == b"a"
        assert q.pop() is None
        q.push(b"b")
        q.push(b"c")
        q.close()
        q = LifoDiskQueue(self.qpath)
        assert [q.pop(), q.pop()] == [b"c", b"b"]
        q.close()

    def test_torn_header_self_heals_from_sidecar(self):
        q = LifoDiskQueue(self.qpath)
        q.push(b"a")
        q.close()
        q = LifoDiskQueue(self.qpath)
        q.push(b"stale")
        del q
        # Tear the 4-byte header halfway, as a power loss during close() would.
        with self.qpath.open("r+b") as f:
            f.seek(0)
            f.write(struct.pack(">H", 1))
        q = LifoDiskQueue(self.qpath)
        assert len(q) == 1
        assert q.pop() == b"a"
        q.close()

    def test_broken_sidecar_raises_recovery_error(self):
        q = LifoDiskQueue(self.qpath)
        q.push(b"a")
        q.close()
        meta = Path(str(self.qpath) + ".qmeta")
        for broken in ["{not json", '{"size": -1, "eof": 4, "open": true}', "{}"]:
            meta.write_text(broken)
            with pytest.raises(QueueRecoveryError) as exc_info:
                LifoDiskQueue(self.qpath)
            assert isinstance(exc_info.value, ValueError)

    def test_file_shrunk_below_eof_raises_recovery_error(self):
        q = LifoDiskQueue(self.qpath)
        q.push(b"a")
        q.push(b"b")
        q.close()
        q = LifoDiskQueue(self.qpath)
        q.pop()  # truncates the file, then crash before close()
        del q
        # The sidecar still describes the pre-pop eof, the file is shorter.
        with pytest.raises(QueueRecoveryError):
            LifoDiskQueue(self.qpath)
        # Remove the path and start over.
        self.qpath.unlink()
        Path(str(self.qpath) + ".qmeta").unlink()
        q = LifoDiskQueue(self.qpath)
        q.push(b"c")
        assert q.pop() == b"c"
        q.close()

    def test_legacy_file_is_imported_without_sidecar(self):
        legacy_path = Path(self.qdir) / "legacy"
        # Hand-craft a queuelib <= 1.10 file: 4-byte size header, then each
        # item as data followed by its 4-byte length suffix.
        with legacy_path.open("wb") as f:
            f.write(struct.pack(">L", 2))
            for item in (b"a", b"bb"):
                f.write(item)
                f.write(struct.pack(">L", len(item)))
        q = LifoDiskQueue(legacy_path)
        assert Path(str(legacy_path) + ".qmeta").exists()
        assert len(q) == 2
        assert [q.pop(), q.pop()] == [b"bb", b"a"]
        q.close()

    def test_corrupt_legacy_file_raises_recovery_error(self):
        legacy_path = Path(self.qdir) / "corrupt-legacy"
        with legacy_path.open("wb") as f:
            f.write(struct.pack(">L", 2))  # claims two frames, writes none
        with pytest.raises(QueueRecoveryError):
            LifoDiskQueue(legacy_path)

    def test_clear_then_crash_stays_empty_and_usable(self):
        q = LifoDiskQueue(self.qpath)
        q.push(b"a")
        q.clear()
        del q
        q = LifoDiskQueue(self.qpath)
        assert len(q) == 0
        q.push(b"b")
        assert q.pop() == b"b"
        q.close()

    def test_empty_close_path_reuse_across_rounds(self):
        for _ in range(3):
            q = LifoDiskQueue(self.qpath)
            q.push(b"a")
            assert q.pop() == b"a"
            q.close()
            assert not self.qpath.exists()
            assert not Path(str(self.qpath) + ".qmeta").exists()

    def test_empty_sidecar_without_data_file_rebuilds(self):
        q = LifoDiskQueue(self.qpath)
        q.close()
        Path(str(self.qpath) + ".qmeta").write_text(
            json.dumps({"size": 0, "eof": 4, "open": False})
        )
        q = LifoDiskQueue(self.qpath)
        assert len(q) == 0
        q.push(b"a")
        assert q.pop() == b"a"
        q.close()


class PriorityQueueRecoveryTest(QueuelibTestCase):
    def test_priority_queue_survives_crash_and_propagates_error(self):
        def qfactory(prio: int) -> FifoDiskQueue:
            return FifoDiskQueue(Path(self.qdir, str(prio)))

        pq = PriorityQueue(qfactory)
        pq.push(b"a", 1)
        active = pq.close()
        pq = PriorityQueue(qfactory, active)
        pq.push(b"stale", 1)
        del pq
        pq = PriorityQueue(qfactory, active)
        pq.push(b"b", 1)
        assert [pq.pop(), pq.pop()] == [b"a", b"b"]
        assert not pq.close()

        # A torn sub-queue bookkeeping fails at construction, no half state.
        pq = PriorityQueue(qfactory, active)
        pq.push(b"c", 2)
        del pq
        broken = Path(self.qdir, "2", "info.json")
        broken.write_text("{broken")
        with pytest.raises(QueueRecoveryError):
            PriorityQueue(qfactory, [2])


# A tiny program used by the real kill tests. It pushes one item, signals
# readiness, then sleeps until killed.
_KILLED_CHILD = r"""
import sys
import time
from pathlib import Path

repo, kind, qpath, ready = sys.argv[1], sys.argv[2], sys.argv[3], Path(sys.argv[4])
sys.path.insert(0, repo)
if kind == "fifo":
    from queuelib import FifoDiskQueue as Queue
else:
    from queuelib import LifoDiskQueue as Queue
q = Queue(qpath)
q.push(b"survivor")
ready.touch()
time.sleep(30)
"""


class KilledProcessRecoveryTest(QueuelibTestCase):
    def _run_killed_child(self, kind: str) -> None:
        script = Path(self.tmpdir) / "child.py"
        script.write_text(_KILLED_CHILD)
        ready = Path(self.tmpdir) / "ready"
        repo = str(Path(__file__).resolve().parents[2])
        # Every argument is built by the test itself, no untrusted input.
        proc = subprocess.Popen(  # noqa: S603
            [sys.executable, "-I", str(script), repo, kind, str(self.qpath), str(ready)]
        )
        try:
            for _ in range(100):
                if ready.exists():
                    break
                if proc.poll() is not None:
                    raise AssertionError("child exited before being killed")
                time.sleep(0.1)
            else:
                proc.kill()
                raise AssertionError("child never became ready")
            proc.kill()
            proc.communicate(timeout=30)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.communicate(timeout=30)

    def test_fifo_recovers_from_hard_kill(self):
        q = FifoDiskQueue(self.qpath)
        q.push(b"committed")
        q.close()
        self._run_killed_child("fifo")
        q = FifoDiskQueue(self.qpath)
        assert len(q) == 1
        assert q.pop() == b"committed"
        q.push(b"after")
        assert q.pop() == b"after"
        q.close()

    def test_lifo_recovers_from_hard_kill(self):
        q = LifoDiskQueue(self.qpath)
        q.push(b"committed")
        q.close()
        self._run_killed_child("lifo")
        q = LifoDiskQueue(self.qpath)
        assert len(q) == 1
        assert q.pop() == b"committed"
        q.close()
