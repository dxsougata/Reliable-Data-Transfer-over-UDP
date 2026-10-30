# AI Use Log

As per course instructions, this document tracks where and how our team consulted AI tools (ChatGPT, Claude, Gemini) during the project. 

Core protocol logic, state transitions, buffer data structures, and socket routines were written and integrated by us. We mainly used AI as a teaching assistant when stuck on networking concepts, for code review/cleanup, debugging tricky timeout bugs, and writing repetitive boilerplate.

---

## 1. Common Components (Group)
* **What we consulted AI for:**
  * Syntax for Python's `struct` library to pack our packet header into binary format (`!BIII` for 1-byte type, 4-byte sequence number, 4-byte payload length, 4-byte CRC32).
  * Quick reference for `zlib.crc32` masking with `0xFFFFFFFF` to ensure 32-bit unsigned integers across platforms.
* **What we did manually:**
  * Defined packet types (`START`, `DATA`, `END`, `ACK`).
  * Designed the payload structure for `START` (chunk count + SHA-256 hash).
  * Implemented serialization/deserialization and error handling in `src/common/packet.py`.

---

## 2. Adrish Paul — Channel Emulator (`src/channel.py`)
* **What I consulted AI for:**
  * **Queue Design & Concurrency:** Asked for suggestions on how to implement the delayed packet queue without busy-waiting or burning CPU cycles. AI suggested pairing `heapq` with `threading.Condition(wait=timeout)` so delayed packets are dispatched cleanly at their scheduled monotonic timestamps.
  * **Debugging:** Looked up UDP loopback socket behavior and ephemeral port handling on Windows localhost.
  * **Boilerplate:** Quick template for `argparse` configuration to support all required impairment flags.
* **What I wrote myself:**
  * Bidirectional endpoint detection and routing logic (learning the sender's port dynamically).
  * The 5 network impairments: packet loss, bitwise corruption (`^ 0xFF`), duplication, random delay, and out-of-order reordering.
  * Statistics tracking and thread synchronization.

---

## 3. Shuvam Sahu — Stop-and-Wait & Go-Back-N (`src/protocols/stop_and_wait.py`, `src/protocols/go_back_n.py`)
* **What I consulted AI for:**
  * **Concept Clarification:** Clarified cumulative ACK handling when sequence numbers wrap around using modular arithmetic (`SEQ_MODULO = 65536`).
  * **Debugging:** Troubleshooting socket timeout exceptions (`socket.timeout`) when multiple consecutive packets get dropped by the channel.
* **What I wrote myself:**
  * The Stop-and-Wait sender/receiver state machine (send, wait for matching ACK, timer expiry, single retransmission).
  * Go-Back-N sender sliding window buffer (`base` and `next_seq_num`).
  * GBN receiver logic to strictly accept in-order packets and discard out-of-order packets while re-sending cumulative ACKs.
  * Integration with the RTO estimator.

---

## 4. Diyatima Majumdar — Selective Repeat (`src/protocols/selective_repeat.py`)
* **What I consulted AI for:**
  * **Design Discussion:** Brainstormed whether to store out-of-order buffered packets in a fixed-size circular list or a dictionary keyed by sequence number (went with dictionary for simpler sliding window lookups).
  * **Test Cases:** Asked AI for edge-case scenarios to test (e.g. lost ACKs for already-buffered packets, duplicate packets falling inside vs. behind the current receive window).
* **What I wrote myself:**
  * Individual ACK tracking for every packet in the sender window.
  * Selective Repeat receiver buffer to store out-of-order packets without discarding them.
  * In-order commit loop: advancing the receiver window and writing consecutive buffered chunks to disk when earlier missing packets finally arrive.

---

## 5. Sougata Kundu — Adaptive RTO & Experiments (`src/rto.py`, `experiments/`)
* **What I consulted AI for:**
  * **Formula Verification:** Checked the standard Jacobson/Karels smoothing equations ($\alpha = 0.125$, $\beta = 0.25$) and confirmed Karn's algorithm rules (ignoring RTT samples from retransmitted packets).
  * **Boilerplate:** Syntax and styling templates for `matplotlib` to plot Goodput vs. Loss Rate and Window Size vs. Transfer Time.
* **What I wrote myself:**
  * `RtoEstimator` class implementing SRTT, RTTVAR, RTO clamping between `MIN_RTO` and `MAX_RTO`, and exponential backoff (`backoff()`).
  * Experiment automation scripts to run transfers across varying loss rates and window sizes, logging performance metrics.
