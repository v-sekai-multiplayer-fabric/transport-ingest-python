"""The one place this repository touches iceoryx2.

Everything else here holds bytes and a session. Keeping the bus behind one module is what makes
the choice of binding reversible: the rest of the fabric reaches iceoryx2 through a dlsym
dispatch table generated from `iceoryx2.sigs`, because iceoryx2 is Rust and weft links no Rust,
and this repository uses the prebuilt Python wheel instead. RFD 0123 records that departure. If
it is reversed, this file is what changes.

Two facts about the service registration are contracts rather than choices. The payload type is
a byte slice, so nothing here can be opened against `weft/harness/snapshot`, whose type is a
40-byte `weft::Snapshot` -- iceoryx2 rejects a second port whose type name, size or alignment
disagrees, and that rejection is the check that stops one process reading another's layout as
its own. And the bytes published are encoded 100-byte records rather than a C struct, because
the struct carries padding the wire does not.

SPDX-License-Identifier: MIT
"""

from __future__ import annotations

import ctypes
from types import TracebackType

import iceoryx2 as iox

# One publisher's largest single send. A ward is 1800 entities at 100 bytes, which is the most
# a tick can produce, and the sizing comes from that rather than from a datagram: what arrives
# here is already split across datagrams, and what leaves for the interactor is not.
MAX_SEND_BYTES = 180000

# How many samples a subscriber holds before the oldest is overwritten. iceoryx2 defaults this
# to 2, which loses the first of three sends before a reader ever runs -- measured, not assumed.
# Safe overflow stays on, so a lagging reader never stalls the writer, which is RFD 0108's rule.
# The cost is that an overwrite is silent: the header carries a node and publisher id and no
# sequence number, so nothing here can tell a dropped sample from one never sent. RFD 0049 lets
# a state channel drop, which is what this one is.
SUBSCRIBER_BUFFER = 256


class Bus:
    """A publisher and a subscriber on one iceoryx2 node.

    iceoryx2 is shared memory, so both ends are processes on one machine. A transport layer and
    the interactor it feeds cannot be two Fly machines: they share neither /dev/shm nor the
    service registry.
    """

    def __init__(self, *, publish_to: str, subscribe_to: str, max_send: int = MAX_SEND_BYTES) -> None:
        self._node = iox.NodeBuilder.new().create(iox.ServiceType.Ipc)
        self._max_send = max_send

        out = (
            self._node.service_builder(iox.ServiceName.new(publish_to))
            .publish_subscribe(iox.Slice[ctypes.c_uint8])
            .subscriber_max_buffer_size(SUBSCRIBER_BUFFER)
            .open_or_create()
        )
        self._publisher = out.publisher_builder().initial_max_slice_len(max_send).create()

        into = (
            self._node.service_builder(iox.ServiceName.new(subscribe_to))
            .publish_subscribe(iox.Slice[ctypes.c_uint8])
            .subscriber_max_buffer_size(SUBSCRIBER_BUFFER)
            .open_or_create()
        )
        self._subscriber = into.subscriber_builder().create()

    def publish(self, payload: bytes) -> None:
        """Loan a sample, fill it, and send it. The copy here is into shared memory rather than
        into another process, so a subscriber reads these bytes without a second one."""
        if len(payload) > self._max_send:
            raise ValueError(f"payload is {len(payload)} bytes, publisher loans at most {self._max_send}")

        sample = self._publisher.loan_slice_uninit(number_of_elements=len(payload))
        ctypes.memmove(sample.payload_ptr, payload, len(payload))
        sample.assume_init().send()

    def receive(self) -> bytes | None:
        """The next sample, or None when the queue is empty. Never blocks.

        Polling rather than blocking is deliberate. RFD 0096 measured a busy-polled
        shared-memory ring failing to finish 20000 round trips in over 8 minutes on a one-vCPU
        Fly machine, so a caller drives this from its own loop and decides how often.
        """
        sample = self._subscriber.receive()
        if sample is None:
            return None
        return bytes(bytearray(sample.payload()))

    def close(self) -> None:
        self._node = None
        self._publisher = None
        self._subscriber = None

    def __enter__(self) -> Bus:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
