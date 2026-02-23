"""Deepgram Nova-3 Medical transcription wrapper."""

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path

from deepgram import DeepgramClient, FileSource, PrerecordedOptions

from crowdtrans.config import settings
from crowdtrans.config_store import get_config_store

logger = logging.getLogger(__name__)


@dataclass
class TranscriptionResult:
    transcript_text: str
    confidence: float
    words_json: str
    paragraphs_json: str
    request_id: str
    processing_duration_ms: int


def _build_options(keyterms: list[str] | None = None) -> PrerecordedOptions:
    store = get_config_store()
    model = store.get_global("deepgram_model") or settings.deepgram_model
    language = store.get_global("deepgram_language") or settings.deepgram_language
    options = PrerecordedOptions(
        model=model,
        language=language,
        smart_format=True,
        punctuate=True,
        paragraphs=True,
        utterances=True,
        numerals=True,
    )
    if keyterms:
        options.keywords = keyterms
    return options


def _parse_response(response) -> TranscriptionResult:
    result = response.results
    channel = result.channels[0]
    alt = channel.alternatives[0]

    words = []
    if alt.words:
        words = [
            {"word": w.word, "start": w.start, "end": w.end, "confidence": w.confidence}
            for w in alt.words
        ]

    paragraphs = []
    if alt.paragraphs and alt.paragraphs.paragraphs:
        paragraphs = [
            {
                "sentences": [
                    {"text": s.text, "start": s.start, "end": s.end}
                    for s in p.sentences
                ]
            }
            for p in alt.paragraphs.paragraphs
        ]

    request_id = response.metadata.request_id if response.metadata else ""

    return TranscriptionResult(
        transcript_text=alt.transcript,
        confidence=alt.confidence,
        words_json=json.dumps(words),
        paragraphs_json=json.dumps(paragraphs),
        request_id=request_id,
        processing_duration_ms=0,  # set by caller
    )


def _get_api_key() -> str:
    store = get_config_store()
    return store.get_global("deepgram_api_key") or settings.deepgram_api_key


def transcribe_file(audio_path: Path, keyterms: list[str] | None = None) -> TranscriptionResult:
    """Transcribe an audio file from disk (Visage .opus files)."""
    client = DeepgramClient(_get_api_key())

    with open(audio_path, "rb") as f:
        buffer_data = f.read()

    payload: FileSource = {"buffer": buffer_data}
    options = _build_options(keyterms)

    logger.info("Sending %s to Deepgram (%d bytes)", audio_path.name, len(buffer_data))
    start = time.monotonic()
    response = client.listen.rest.v("1").transcribe_file(payload, options)
    elapsed_ms = int((time.monotonic() - start) * 1000)

    result = _parse_response(response)
    result.processing_duration_ms = elapsed_ms
    return result


def transcribe_buffer(
    audio_data: bytes,
    content_type: str = "audio/raw",
    keyterms: list[str] | None = None,
    label: str = "blob",
) -> TranscriptionResult:
    """Transcribe audio bytes from memory (Karisma SQL blobs)."""
    client = DeepgramClient(_get_api_key())

    payload: FileSource = {"buffer": audio_data}
    options = _build_options(keyterms)

    logger.info("Sending %s to Deepgram (%d bytes, %s)", label, len(audio_data), content_type)
    start = time.monotonic()
    response = client.listen.rest.v("1").transcribe_file(payload, options)
    elapsed_ms = int((time.monotonic() - start) * 1000)

    result = _parse_response(response)
    result.processing_duration_ms = elapsed_ms
    return result
