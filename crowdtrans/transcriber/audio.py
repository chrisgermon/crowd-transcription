"""Audio processing for Karisma DICT format blobs.

Karisma stores dictation audio as compressed blobs in System.Extent.
The blob may be:
  1. Already valid WAV (RIFF header)
  2. GZip compressed
  3. Deflate compressed
  4. Has a small header (2-32 bytes) before the compressed payload
  5. Raw audio (Deepgram can detect many formats natively)

The blob is sliced using ExtentOffset/ExtentLength before decompression.
"""

import gzip
import io
import logging
import zlib
from dataclasses import dataclass

logger = logging.getLogger(__name__)

WAV_MAGIC = b"RIFF"
GZIP_MAGIC = b"\x1f\x8b"


@dataclass
class AudioResult:
    data: bytes
    content_type: str


def _is_wav(data: bytes) -> bool:
    return len(data) >= 4 and data[:4] == WAV_MAGIC


def _is_gzip(data: bytes) -> bool:
    return len(data) >= 2 and data[:2] == GZIP_MAGIC


def _try_gzip(data: bytes) -> bytes | None:
    try:
        return gzip.decompress(data)
    except Exception:
        return None


def _try_deflate(data: bytes) -> bytes | None:
    try:
        result = zlib.decompress(data, -zlib.MAX_WBITS)
        return result if result else None
    except Exception:
        return None


def process_karisma_blob(
    raw_blob: bytes,
    offset: int | None,
    length: int | None,
    dictation_key: int,
) -> AudioResult | None:
    """Process a Karisma audio blob into bytes suitable for Deepgram.

    Applies offset/length slicing, then tries multiple decompression strategies.
    Returns AudioResult with data and content_type, or None on failure.
    """
    try:
        # Apply offset and length
        if offset is not None and length is not None and offset >= 0 and length > 0:
            if offset + length > len(raw_blob):
                logger.warning(
                    "Dictation %d: offset(%d) + length(%d) exceeds blob size(%d), using full blob",
                    dictation_key, offset, length, len(raw_blob),
                )
                segment = raw_blob
            else:
                segment = raw_blob[offset : offset + length]
        else:
            segment = raw_blob

        logger.debug("Dictation %d: processing %d bytes of audio", dictation_key, len(segment))

        # Strategy 1: Already WAV
        if _is_wav(segment):
            logger.debug("Dictation %d: audio is already WAV", dictation_key)
            return AudioResult(data=segment, content_type="audio/wav")

        # Strategy 2: GZip
        if _is_gzip(segment):
            result = _try_gzip(segment)
            if result:
                ct = "audio/wav" if _is_wav(result) else "audio/raw"
                logger.debug(
                    "Dictation %d: GZip decompressed %d -> %d bytes",
                    dictation_key, len(segment), len(result),
                )
                return AudioResult(data=result, content_type=ct)

        # Strategy 3: Deflate
        result = _try_deflate(segment)
        if result:
            ct = "audio/wav" if _is_wav(result) else "audio/raw"
            logger.debug(
                "Dictation %d: Deflate decompressed %d -> %d bytes",
                dictation_key, len(segment), len(result),
            )
            return AudioResult(data=result, content_type=ct)

        # Strategy 4: Skip header bytes + try decompression
        for skip in (2, 4, 8, 16, 32):
            if len(segment) <= skip:
                continue
            trimmed = segment[skip:]

            if _is_gzip(trimmed):
                result = _try_gzip(trimmed)
                if result:
                    ct = "audio/wav" if _is_wav(result) else "audio/raw"
                    logger.debug(
                        "Dictation %d: decompressed after skipping %d header bytes",
                        dictation_key, skip,
                    )
                    return AudioResult(data=result, content_type=ct)

            result = _try_deflate(trimmed)
            if result and len(result) > len(segment):
                ct = "audio/wav" if _is_wav(result) else "audio/raw"
                logger.debug(
                    "Dictation %d: Deflate decompressed after skipping %d header bytes",
                    dictation_key, skip,
                )
                return AudioResult(data=result, content_type=ct)

        # Strategy 5: Send raw — Deepgram handles many formats natively
        logger.info(
            "Dictation %d: could not decompress, sending raw %d bytes (header: %s)",
            dictation_key,
            len(segment),
            segment[:16].hex(),
        )
        return AudioResult(data=segment, content_type="audio/raw")

    except Exception:
        logger.exception("Dictation %d: audio processing failed", dictation_key)
        return None
