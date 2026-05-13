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


def _get_int_setting(key: str, default: int) -> int:
    store = get_config_store()
    raw = store.get_global(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _build_options(keyterms: list[str] | None = None) -> PrerecordedOptions:
    store = get_config_store()
    model = store.get_global("deepgram_model") or settings.deepgram_model
    language = store.get_global("deepgram_language") or settings.deepgram_language
    # Configurable: 1 = no extras (default, no extra billing).
    # 2 surfaces second-best hypotheses for low-confidence words.
    alternatives = max(1, _get_int_setting("deepgram_alternatives", 1))
    kwargs = dict(
        model=model,
        language=language,
        smart_format=True,
        punctuate=True,
        dictation=True,
        paragraphs=True,
        utterances=True,
        numerals=True,
    )
    if alternatives > 1:
        kwargs["alternatives"] = alternatives
    if keyterms:
        # Use the native keyterm parameter — Nova-3's keyterm boost.
        kwargs["keyterm"] = keyterms
    return PrerecordedOptions(**kwargs)


def _word_alternatives(alt_word_lists: list[list]) -> dict[tuple[float, float], list[dict]]:
    """Build a {(start, end) -> [alt_word, ...]} index from alternative hypotheses.

    Only includes alternative words whose timing matches the primary alt's
    word timing (so we can attach them to the right token).
    """
    if len(alt_word_lists) < 2:
        return {}
    index: dict[tuple[float, float], list[dict]] = {}
    primary = alt_word_lists[0]
    primary_keys = {(round(w.start, 2), round(w.end, 2)): w.word.lower() for w in primary}
    for alt_words in alt_word_lists[1:]:
        for w in alt_words:
            key = (round(w.start, 2), round(w.end, 2))
            primary_word = primary_keys.get(key)
            if primary_word is None:
                continue
            if w.word.lower() == primary_word:
                continue  # same word; nothing useful to surface
            index.setdefault(key, []).append({
                "word": w.word,
                "confidence": getattr(w, "confidence", None),
            })
    return index


def _parse_response(response) -> TranscriptionResult:
    result = response.results
    channel = result.channels[0]
    alts = list(channel.alternatives or [])
    if not alts:
        return TranscriptionResult(
            transcript_text="", confidence=0.0,
            words_json="[]", paragraphs_json="[]",
            request_id=response.metadata.request_id if response.metadata else "",
            processing_duration_ms=0,
        )
    alt = alts[0]

    # Index alternative hypotheses by primary word timing so we can attach them
    alt_word_index = _word_alternatives([a.words for a in alts if a.words])

    words = []
    if alt.words:
        for w in alt.words:
            entry = {
                "word": w.word,
                "start": w.start,
                "end": w.end,
                "confidence": w.confidence,
            }
            alts_for_word = alt_word_index.get((round(w.start, 2), round(w.end, 2)))
            if alts_for_word:
                # Deduplicate by word text
                seen = set()
                unique = []
                for a in alts_for_word:
                    key = a["word"].lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    unique.append(a)
                entry["alternatives"] = unique
            words.append(entry)

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
