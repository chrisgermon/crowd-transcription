#!/usr/bin/env python3
"""Compare Deepgram transcriptions against Karisma typed reports.

Produces:
  1. Bulk WER / similarity stats
  2. Breakdown by doctor, modality, facility
  3. Common Deepgram error patterns (misheard terms, verbal punctuation)
"""

import re
import sqlite3
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path

from jiwer import wer as compute_wer, cer as compute_cer
from rapidfuzz import fuzz

DB_PATH = "/opt/crowdtrans/data/crowdtrans.db"

# ---------------------------------------------------------------------------
# Text normalisation
# ---------------------------------------------------------------------------

# Verbal punctuation / commands that radiologists speak but the typist converts
VERBAL_COMMANDS = [
    (r'\bfull stop\b', '.'),
    (r'\bperiod\b', '.'),
    (r'\bcomma\b', ','),
    (r'\bsemicolon\b', ';'),
    (r'\bcolon\b', ':'),
    (r'\bhyphen\b', '-'),
    (r'\bdash\b', '-'),
    (r'\bopen bracket\b', '('),
    (r'\bclose bracket\b', ')'),
    (r'\bopen parenthesis\b', '('),
    (r'\bclose parenthesis\b', ')'),
    (r'\bnew line\b', ' '),
    (r'\bnew paragraph\b', ' '),
    (r'\bparagraph\b', ' '),
    (r'\bnext paragraph\b', ' '),
    (r'\bstop\b', '.'),  # must be after "full stop"
]

# Boilerplate that appears in typed reports but not dictated
REPORT_BOILERPLATE_PATTERNS = [
    r'thank you for referring this patient\.?',
    r'kind regards,?',
    r'electronically signed by:?\s*',
    r'dr\s+\w+\s+\w+\s+\w+\s+\w+\s+consultant radiologist',
    r'patient id number:?\s*\S+',
    r'(?:mbbs|franzcr|frcr|fracr)',
    r'consultant radiologist',
]


def normalise_deepgram(text: str, strip_verbal_punct: bool = True) -> str:
    """Normalise Deepgram transcript for fair comparison."""
    t = text.lower().strip()
    # Remove the <\n\n> markers that appear in some transcripts
    t = re.sub(r'<\\n\\n>', ' ', t)
    if strip_verbal_punct:
        # Remove verbal punctuation commands
        for pattern, _replacement in VERBAL_COMMANDS:
            t = re.sub(pattern, ' ', t, flags=re.IGNORECASE)
    # Strip all punctuation
    t = re.sub(r'[^\w\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def normalise_report(text: str) -> str:
    """Normalise typed report for fair comparison."""
    t = text.lower().strip()
    # Remove boilerplate
    for pattern in REPORT_BOILERPLATE_PATTERNS:
        t = re.sub(pattern, ' ', t, flags=re.IGNORECASE)
    # Remove headings like "CT ABDOMEN AND PELVIS", "Indication:", "Findings:", etc.
    # Actually keep these — the radiologist usually dictates them too
    # Strip all punctuation
    t = re.sub(r'[^\w\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


# ---------------------------------------------------------------------------
# Word-level diff to find substitution patterns
# ---------------------------------------------------------------------------

def word_level_diff(hyp_words: list[str], ref_words: list[str]) -> list[tuple[str, str]]:
    """Return list of (deepgram_word, report_word) substitution pairs using SequenceMatcher."""
    import difflib
    sm = difflib.SequenceMatcher(None, hyp_words, ref_words)
    substitutions = []
    for op, i1, i2, j1, j2 in sm.get_opcodes():
        if op == 'replace':
            # Pair up words 1:1 where possible
            hyp_chunk = hyp_words[i1:i2]
            ref_chunk = ref_words[j1:j2]
            for k in range(min(len(hyp_chunk), len(ref_chunk))):
                substitutions.append((hyp_chunk[k], ref_chunk[k]))
    return substitutions


# ---------------------------------------------------------------------------
# Main analysis
# ---------------------------------------------------------------------------

def run_pass(rows, strip_verbal_punct: bool) -> dict:
    """Run WER analysis on all rows. Returns a results dict."""
    wer_scores = []
    cer_scores = []
    similarity_scores = []
    by_doctor = defaultdict(list)
    by_facility = defaultdict(list)

    for row in rows:
        dg_norm = normalise_deepgram(row['transcript_text'], strip_verbal_punct=strip_verbal_punct)
        rpt_norm = normalise_report(row['final_report_text'])

        if not dg_norm or not rpt_norm:
            continue

        try:
            w = compute_wer(rpt_norm, dg_norm)
            c = compute_cer(rpt_norm, dg_norm)
        except Exception:
            continue

        sim = fuzz.token_sort_ratio(dg_norm, rpt_norm) / 100.0

        wer_scores.append(w)
        cer_scores.append(c)
        similarity_scores.append(sim)

        doctor = row['doctor_family_name'] or 'Unknown'
        facility = row['facility_name'] or 'Unknown'
        by_doctor[doctor].append(w)
        by_facility[facility].append(w)

    n = len(wer_scores)
    sorted_wer = sorted(wer_scores)
    return {
        'n': n,
        'wer_scores': wer_scores,
        'mean_wer': statistics.mean(wer_scores),
        'median_wer': statistics.median(wer_scores),
        'std_wer': statistics.stdev(wer_scores),
        'p10_wer': sorted_wer[n // 10],
        'p90_wer': sorted_wer[9 * n // 10],
        'mean_cer': statistics.mean(cer_scores),
        'mean_sim': statistics.mean(similarity_scores),
        'by_doctor': by_doctor,
        'by_facility': by_facility,
    }


def print_comparison(before: dict, after: dict, rows):
    """Print side-by-side comparison of before/after stripping verbal punctuation."""
    n = before['n']

    def delta(old, new, lower_is_better=True):
        diff = new - old
        pct = diff / old * 100 if old else 0
        arrow = '▼' if (diff < 0 and lower_is_better) or (diff > 0 and not lower_is_better) else '▲'
        color = 'improved' if ((diff < 0 and lower_is_better) or (diff > 0 and not lower_is_better)) else 'worse'
        return f"{diff:+.2%} ({arrow} {abs(pct):.1f}% {color})"

    print("=" * 90)
    print("VERBAL PUNCTUATION STRIPPING — BEFORE vs AFTER COMPARISON")
    print("=" * 90)
    print(f"  Pairs analysed: {n:,}")
    print()
    print(f"  {'Metric':<25s}  {'BEFORE':>12s}  {'AFTER':>12s}  {'CHANGE'}")
    print(f"  {'-' * 25}  {'-' * 12}  {'-' * 12}  {'-' * 35}")
    print(f"  {'Mean WER':<25s}  {before['mean_wer']:>11.2%}  {after['mean_wer']:>11.2%}   {delta(before['mean_wer'], after['mean_wer'])}")
    print(f"  {'Median WER':<25s}  {before['median_wer']:>11.2%}  {after['median_wer']:>11.2%}   {delta(before['median_wer'], after['median_wer'])}")
    print(f"  {'Std Dev WER':<25s}  {before['std_wer']:>11.2%}  {after['std_wer']:>11.2%}   {delta(before['std_wer'], after['std_wer'])}")
    print(f"  {'P10 WER':<25s}  {before['p10_wer']:>11.2%}  {after['p10_wer']:>11.2%}   {delta(before['p10_wer'], after['p10_wer'])}")
    print(f"  {'P90 WER':<25s}  {before['p90_wer']:>11.2%}  {after['p90_wer']:>11.2%}   {delta(before['p90_wer'], after['p90_wer'])}")
    print(f"  {'Mean CER':<25s}  {before['mean_cer']:>11.2%}  {after['mean_cer']:>11.2%}   {delta(before['mean_cer'], after['mean_cer'])}")
    print(f"  {'Mean Token Similarity':<25s}  {before['mean_sim']:>11.2%}  {after['mean_sim']:>11.2%}   {delta(before['mean_sim'], after['mean_sim'], lower_is_better=False)}")

    # WER distribution comparison
    buckets = [(0, 0.1), (0.1, 0.2), (0.2, 0.3), (0.3, 0.5), (0.5, 0.75), (0.75, 1.0), (1.0, float('inf'))]
    print(f"\n  WER Distribution (before -> after):")
    print(f"    {'Bucket':>12s}  {'BEFORE':>14s}  {'AFTER':>14s}  {'SHIFT'}")
    print(f"    {'-' * 12}  {'-' * 14}  {'-' * 14}  {'-' * 20}")
    for lo, hi in buckets:
        b_count = sum(1 for w in before['wer_scores'] if lo <= w < hi)
        a_count = sum(1 for w in after['wer_scores'] if lo <= w < hi)
        label = f"{lo:.0%}-{hi:.0%}" if hi != float('inf') else f"{lo:.0%}+"
        shift = a_count - b_count
        sign = '+' if shift > 0 else ''
        print(f"    {label:>12s}  {b_count:>6,} ({b_count / n:>5.1%})  {a_count:>6,} ({a_count / n:>5.1%})  {sign}{shift:,}")

    # Doctor breakdown comparison
    print(f"\n  {'Doctor':<30s}  {'BEFORE':>9s}  {'AFTER':>9s}  {'IMPROVEMENT'}")
    print(f"  {'-' * 30}  {'-' * 9}  {'-' * 9}  {'-' * 20}")
    doctor_deltas = []
    for doctor in before['by_doctor']:
        if len(before['by_doctor'][doctor]) < 20:
            continue
        b_mean = statistics.mean(before['by_doctor'][doctor])
        a_mean = statistics.mean(after['by_doctor'].get(doctor, before['by_doctor'][doctor]))
        improvement = b_mean - a_mean
        doctor_deltas.append((doctor, b_mean, a_mean, improvement, len(before['by_doctor'][doctor])))

    for doctor, b_mean, a_mean, improvement, count in sorted(doctor_deltas, key=lambda x: -x[3]):
        print(f"  {doctor:<30s}  {b_mean:>8.2%}  {a_mean:>8.2%}  {improvement:>+7.2%} pts  (n={count})")

    # Facility breakdown comparison (top 20 by volume)
    print(f"\n  {'Facility':<50s}  {'BEFORE':>9s}  {'AFTER':>9s}  {'IMPROVEMENT'}")
    print(f"  {'-' * 50}  {'-' * 9}  {'-' * 9}  {'-' * 20}")
    fac_deltas = []
    for fac in before['by_facility']:
        if len(before['by_facility'][fac]) < 20:
            continue
        b_mean = statistics.mean(before['by_facility'][fac])
        a_mean = statistics.mean(after['by_facility'].get(fac, before['by_facility'][fac]))
        improvement = b_mean - a_mean
        fac_deltas.append((fac, b_mean, a_mean, improvement, len(before['by_facility'][fac])))

    for fac, b_mean, a_mean, improvement, count in sorted(fac_deltas, key=lambda x: -x[3])[:25]:
        print(f"  {fac:<50s}  {b_mean:>8.2%}  {a_mean:>8.2%}  {improvement:>+7.2%} pts  (n={count})")

    # Show a few example transformations
    print("\n" + "=" * 90)
    print("EXAMPLE TRANSFORMATIONS (3 random samples)")
    print("=" * 90)
    import random
    random.seed(42)
    samples = random.sample(rows, min(3, len(rows)))
    for row in samples:
        raw = row['transcript_text'][:300]
        stripped = normalise_deepgram(row['transcript_text'], strip_verbal_punct=True)[:300]
        report = normalise_report(row['final_report_text'])[:300]
        print(f"\n  ID={row['id']}  Doctor={row['doctor_family_name']}")
        print(f"  RAW DEEPGRAM:  {raw}")
        print(f"  STRIPPED:      {stripped}")
        print(f"  REPORT:        {report}")


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT
            id, accession_number, patient_family_name,
            modality_code, modality_name,
            doctor_family_name, facility_name,
            dictation_date, confidence,
            transcript_text, final_report_text
        FROM transcriptions
        WHERE site_id = 'karisma'
          AND transcript_text IS NOT NULL AND transcript_text != ''
          AND final_report_text IS NOT NULL AND final_report_text != ''
        ORDER BY id
    """).fetchall()

    conn.close()

    print(f"Loaded {len(rows):,} dictation pairs\n")
    print("Running BEFORE pass (no verbal punctuation stripping)...")
    before = run_pass(rows, strip_verbal_punct=False)
    print("Running AFTER pass (with verbal punctuation stripping)...")
    after = run_pass(rows, strip_verbal_punct=True)
    print()
    print_comparison(before, after, rows)


if __name__ == "__main__":
    main()
