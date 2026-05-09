#!/usr/bin/env python3
"""Layered WER improvement analysis.

Applies normalisations incrementally to measure the impact of each:
  Layer 0: Raw (punctuation stripped only)
  Layer 1: + Verbal punctuation commands stripped
  Layer 2: + US -> AU spelling normalised
  Layer 3: + Report boilerplate/sign-offs removed
  Layer 4: + Ordinal/number normalisation
  Layer 5: + Plural/singular normalisation
"""

import re
import sqlite3
import statistics
from collections import Counter, defaultdict

from jiwer import wer as compute_wer
from rapidfuzz import fuzz

DB_PATH = "/opt/crowdtrans/data/crowdtrans.db"

# ---------------------------------------------------------------------------
# Layer 1: Verbal punctuation
# ---------------------------------------------------------------------------
VERBAL_COMMANDS = [
    (r'\bnext paragraph\b', ''),
    (r'\bnew paragraph\b', ''),
    (r'\bnew line\b', ''),
    (r'\bfull stop\b', ''),
    (r'\bopen parenthesis\b', ''),
    (r'\bclose parenthesis\b', ''),
    (r'\bopen bracket\b', ''),
    (r'\bclose bracket\b', ''),
    (r'\bparagraph\b', ''),
    (r'\bsemicolon\b', ''),
    (r'\bperiod\b', ''),
    (r'\bcomma\b', ''),
    (r'\bcolon\b', ''),
    (r'\bhyphen\b', ''),
    (r'\bdash\b', ''),
    (r'\bstop\b', ''),
]

# ---------------------------------------------------------------------------
# Layer 2: US -> AU spelling map
# ---------------------------------------------------------------------------
US_TO_AU = {
    # -ize -> -ise
    'visualized': 'visualised', 'visualize': 'visualise',
    'localized': 'localised', 'localize': 'localise',
    'mineralized': 'mineralised', 'mineralize': 'mineralise',
    'organized': 'organised', 'organize': 'organise',
    'characterized': 'characterised', 'characterize': 'characterise',
    'recognized': 'recognised', 'recognize': 'recognise',
    'stabilized': 'stabilised', 'stabilize': 'stabilise',
    'cauterized': 'cauterised', 'cauterize': 'cauterise',
    'mobilized': 'mobilised', 'mobilize': 'mobilise',
    'utilized': 'utilised', 'utilize': 'utilise',
    'categorized': 'categorised',
    'generalized': 'generalised',
    'summarized': 'summarised',
    'normalized': 'normalised',
    'maximized': 'maximised',
    'minimized': 'minimised',
    'emphasized': 'emphasised',
    'pressurized': 'pressurised',
    'vascularized': 'vascularised',
    'vaporized': 'vaporised',
    'ionized': 'ionised',
    'anodized': 'anodised',
    'oxidized': 'oxidised',
    'immunized': 'immunised',
    'sensitized': 'sensitised',
    'traumatized': 'traumatised',
    'hypothesized': 'hypothesised',
    'catheterized': 'catheterised',
    'revascularized': 'revascularised',
    'devascularized': 'devascularised',
    'homogenized': 'homogenised',
    'compromized': 'compromised',

    # -or -> -our
    'tumor': 'tumour', 'tumors': 'tumours',
    'color': 'colour', 'colors': 'colours',
    'favor': 'favour',
    'humor': 'humour',
    'labor': 'labour',
    'neighbor': 'neighbour',

    # -er -> -re
    'center': 'centre', 'centers': 'centres',
    'fiber': 'fibre', 'fibers': 'fibres',
    'caliber': 'calibre',
    'liter': 'litre', 'liters': 'litres',
    'meter': 'metre', 'meters': 'metres',
    'millimeter': 'millimetre', 'millimeters': 'millimetres',
    'centimeter': 'centimetre', 'centimeters': 'centimetres',

    # ae/oe diphthongs
    'edema': 'oedema',
    'esophagus': 'oesophagus', 'esophageal': 'oesophageal',
    'estrogen': 'oestrogen',
    'fetal': 'foetal', 'fetus': 'foetus',
    'cecum': 'caecum', 'cecal': 'caecal',
    'anemia': 'anaemia', 'anemic': 'anaemic',
    'leukemia': 'leukaemia',
    'hyperemia': 'hyperaemia',
    'ischemia': 'ischaemia', 'ischemic': 'ischaemic',
    'hemorrhage': 'haemorrhage', 'hemorrhagic': 'haemorrhagic',
    'hematoma': 'haematoma', 'hematomas': 'haematomas',
    'hemoglobin': 'haemoglobin',
    'hematuria': 'haematuria',
    'hemolysis': 'haemolysis', 'hemolytic': 'haemolytic',
    'hemoptysis': 'haemoptysis',
    'hemoperitoneum': 'haemoperitoneum',
    'hemosiderin': 'haemosiderin',
    'hemodynamic': 'haemodynamic', 'hemodynamics': 'haemodynamics',
    'hemodialysis': 'haemodialysis',
    'hemothorax': 'haemothorax',
    'fecal': 'faecal', 'feces': 'faeces',
    'pediatric': 'paediatric', 'pediatrics': 'paediatrics',
    'orthopedic': 'orthopaedic',
    'gynecologic': 'gynaecologic', 'gynecological': 'gynaecological',
    'maneuver': 'manoeuvre',

    # Other common AU/US
    'artifact': 'artefact', 'artifacts': 'artefacts',
    'analog': 'analogue',
    'gray': 'grey',
    'sulfate': 'sulphate',
    'sulfur': 'sulphur',
    'defense': 'defence',
    'offense': 'offence',
    'license': 'licence',
    'practice': 'practise',  # verb form
    'program': 'programme',
    'aging': 'ageing',

    # -tion spelling variants
    'organizing': 'organising',
    'recognizing': 'recognising',
    'localizing': 'localising',
    'visualizing': 'visualising',
    'mineralizing': 'mineralising',
    'characterizing': 'characterising',
    'stabilizing': 'stabilising',
    'utilizing': 'utilising',
    'mobilizing': 'mobilising',
    'cauterizing': 'cauterising',
    'vascularizing': 'vascularising',
    'catheterizing': 'catheterising',

    # Common Deepgram US outputs
    'pneumatized': 'pneumatised',
    'nonspecific': 'non specific',
    'nontender': 'non tender',
    'nonobstructive': 'non obstructive',
    'nonobstructing': 'non obstructing',
}

# ---------------------------------------------------------------------------
# Layer 3: Report boilerplate removal
# ---------------------------------------------------------------------------
REPORT_BOILERPLATE = [
    r'thank you for referring this patient\.?',
    r'thanks? for (?:the )?referral\.?',
    r'(?:kind|warm|best)?\s*regards,?',
    r'electronically signed by:?\s*',
    r'dr\s+\w+(?:\s+\w+){0,4}\s+(?:mbbs|franzcr|frcr|fracr|bmbs|md)[\w\s,]*(?:consultant|staff)?\s*(?:radiologist|sonographer)?',
    r'(?:mbbs|franzcr|frcr|fracr|bmbs|md)(?:\s*,?\s*(?:mbbs|franzcr|frcr|fracr|bmbs|md))*',
    r'consultant\s+(?:radiologist|sonographer)',
    r'staff\s+specialist',
    r'patient\s+id\s+number:?\s*\S+',
    r'(?:clinical\s+)?(?:history|indication|reason\s+for\s+(?:study|exam|examination)|referral)\s*:',
    r'(?:findings|report|impression|conclusion|comment|opinion|summary)\s*:',
    r'technique\s*:',
    r'comparison\s*:',
    r'addendum\s*:',
]

DEEPGRAM_BOILERPLATE = [
    # Common dictation lead-ins that don't appear in report content
    r'\buse my standard (?:report|template)\b.*',
    r'\bwith all the bells and whistles\b',
    r'\bplease\s*$',
    r'\bsorry\b',
    r'\bactually\b',
    r'\bum+\b',
    r'\buh+\b',
    r'\bah+\b',
    r'\bhmm+\b',
    r'\blet me (?:start|begin)\b',
    r'\boh wait\b',
    r'\bscratch that\b',
    r'\bdelete that\b',
    r'\bgo back\b',
    r'\bstart over\b',
    r'\bstart again\b',
]

# ---------------------------------------------------------------------------
# Layer 4: Number/ordinal normalisation
# ---------------------------------------------------------------------------
ORDINAL_MAP = {
    'first': '1st', 'second': '2nd', 'third': '3rd', 'fourth': '4th',
    'fifth': '5th', 'sixth': '6th', 'seventh': '7th', 'eighth': '8th',
    'ninth': '9th', 'tenth': '10th', 'eleventh': '11th', 'twelfth': '12th',
}

UNIT_EXPANSIONS = {
    'millimeters': 'mm', 'millimetres': 'mm', 'millimeter': 'mm', 'millimetre': 'mm',
    'centimeters': 'cm', 'centimetres': 'cm', 'centimeter': 'cm', 'centimetre': 'cm',
    'kilograms': 'kg', 'kilogram': 'kg',
    'milligrams': 'mg', 'milligram': 'mg',
    'milliliters': 'ml', 'millilitres': 'ml', 'milliliter': 'ml', 'millilitre': 'ml',
}


# ---------------------------------------------------------------------------
# Layer 5: Plural/singular normalisation
# ---------------------------------------------------------------------------
def strip_trailing_s(text: str) -> str:
    """Crude plural normalisation — strip trailing 's' from words > 4 chars.
    Not perfect, but enough to measure the impact."""
    words = text.split()
    out = []
    for w in words:
        if len(w) > 4 and w.endswith('s') and not w.endswith('ss') and not w.endswith('us'):
            out.append(w[:-1])
        else:
            out.append(w)
    return ' '.join(out)


# ---------------------------------------------------------------------------
# Normalisation pipeline
# ---------------------------------------------------------------------------

def normalise(text: str, layers: set[int], is_deepgram: bool = True) -> str:
    """Apply normalisation layers incrementally."""
    t = text.lower().strip()

    # Always: strip markup
    t = re.sub(r'<\\n\\n>', ' ', t)
    t = re.sub(r'<[^>]+>', ' ', t)

    # Layer 1: verbal punctuation (Deepgram side only)
    if 1 in layers and is_deepgram:
        for pattern, _ in VERBAL_COMMANDS:
            t = re.sub(pattern, ' ', t, flags=re.IGNORECASE)

    # Layer 2: US -> AU spelling (Deepgram side only)
    if 2 in layers and is_deepgram:
        words = t.split()
        words = [US_TO_AU.get(w, w) for w in words]
        t = ' '.join(words)

    # Layer 3: boilerplate
    if 3 in layers:
        patterns = REPORT_BOILERPLATE if not is_deepgram else DEEPGRAM_BOILERPLATE
        for pattern in patterns:
            t = re.sub(pattern, ' ', t, flags=re.IGNORECASE)
        # Also strip report boilerplate from report side
        if not is_deepgram:
            for pattern in REPORT_BOILERPLATE:
                t = re.sub(pattern, ' ', t, flags=re.IGNORECASE)

    # Layer 4: number/ordinal normalisation (both sides)
    if 4 in layers:
        words = t.split()
        words = [ORDINAL_MAP.get(w, w) for w in words]
        words = [UNIT_EXPANSIONS.get(w, w) for w in words]
        t = ' '.join(words)

    # Layer 5: plural normalisation (both sides)
    if 5 in layers:
        t = strip_trailing_s(t)

    # Always: strip punctuation, collapse whitespace
    t = re.sub(r'[^\w\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def run_pass(rows, layers: set[int]) -> dict:
    wer_scores = []
    sim_scores = []
    by_doctor = defaultdict(list)
    by_facility = defaultdict(list)
    per_row = []  # (id, wer) for per-record delta analysis

    for row in rows:
        dg = normalise(row['transcript_text'], layers, is_deepgram=True)
        rpt = normalise(row['final_report_text'], layers, is_deepgram=False)

        if not dg or not rpt:
            continue

        try:
            w = compute_wer(rpt, dg)
        except Exception:
            continue

        sim = fuzz.token_sort_ratio(dg, rpt) / 100.0
        wer_scores.append(w)
        sim_scores.append(sim)
        per_row.append((row['id'], w))

        doctor = row['doctor_family_name'] or 'Unknown'
        facility = row['facility_name'] or 'Unknown'
        by_doctor[doctor].append(w)
        by_facility[facility].append(w)

    n = len(wer_scores)
    s = sorted(wer_scores)
    return {
        'n': n,
        'wer_scores': wer_scores,
        'mean_wer': statistics.mean(wer_scores),
        'median_wer': statistics.median(wer_scores),
        'std_wer': statistics.stdev(wer_scores),
        'p10': s[n // 10],
        'p25': s[n // 4],
        'p75': s[3 * n // 4],
        'p90': s[9 * n // 10],
        'mean_sim': statistics.mean(sim_scores),
        'by_doctor': by_doctor,
        'by_facility': by_facility,
        'per_row': dict(per_row),
    }


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT
            id, accession_number, patient_family_name,
            modality_code, doctor_family_name, facility_name,
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

    layer_names = {
        0: 'Baseline (punctuation-only strip)',
        1: '+ Strip verbal punctuation',
        2: '+ US -> AU spelling',
        3: '+ Remove boilerplate/headers',
        4: '+ Ordinal/number normalisation',
        5: '+ Plural/singular normalisation',
    }

    results = {}
    for layer_num in range(6):
        layers = set(range(layer_num + 1)) if layer_num > 0 else set()
        label = layer_names[layer_num]
        print(f"  Running layer {layer_num}: {label}...")
        results[layer_num] = run_pass(rows, layers)

    # ======================================================================
    # Summary table
    # ======================================================================
    print("\n" + "=" * 100)
    print("LAYERED NORMALISATION — CUMULATIVE WER IMPROVEMENT")
    print("=" * 100)
    print(f"\n  {'Layer':<45s}  {'Mean WER':>9s}  {'Median':>8s}  {'P25':>8s}  {'P75':>8s}  {'Sim':>6s}  {'Delta':>8s}")
    print(f"  {'-' * 45}  {'-' * 9}  {'-' * 8}  {'-' * 8}  {'-' * 8}  {'-' * 6}  {'-' * 8}")

    prev_mean = None
    for layer_num in range(6):
        r = results[layer_num]
        d = f"{r['mean_wer'] - prev_mean:+.2%}" if prev_mean is not None else "   ---"
        prev_mean = r['mean_wer']
        print(f"  {layer_names[layer_num]:<45s}  {r['mean_wer']:>8.2%}  {r['median_wer']:>7.2%}  "
              f"{r['p25']:>7.2%}  {r['p75']:>7.2%}  {r['mean_sim']:>5.1%}  {d:>8s}")

    baseline = results[0]
    final = results[5]
    total_improvement = baseline['mean_wer'] - final['mean_wer']
    pct_improvement = total_improvement / baseline['mean_wer'] * 100
    print(f"\n  TOTAL IMPROVEMENT: {baseline['mean_wer']:.2%} -> {final['mean_wer']:.2%} "
          f"= {total_improvement:+.2%} pts ({pct_improvement:.1f}% relative)")

    # ======================================================================
    # WER distribution comparison: baseline vs fully normalised
    # ======================================================================
    print("\n" + "=" * 100)
    print("WER DISTRIBUTION — BASELINE vs FULLY NORMALISED")
    print("=" * 100)
    buckets = [(0, 0.1), (0.1, 0.2), (0.2, 0.3), (0.3, 0.4), (0.4, 0.5),
               (0.5, 0.6), (0.6, 0.75), (0.75, 1.0), (1.0, float('inf'))]
    n = baseline['n']
    print(f"\n  {'Bucket':>12s}  {'BASELINE':>14s}  {'NORMALISED':>14s}  {'SHIFT':>8s}  {'Visual'}")
    print(f"  {'-' * 12}  {'-' * 14}  {'-' * 14}  {'-' * 8}  {'-' * 30}")
    for lo, hi in buckets:
        b = sum(1 for w in baseline['wer_scores'] if lo <= w < hi)
        a = sum(1 for w in final['wer_scores'] if lo <= w < hi)
        label = f"{lo:.0%}-{hi:.0%}" if hi != float('inf') else f"{lo:.0%}+"
        shift = a - b
        sign = '+' if shift > 0 else ''
        bar_b = 'B' * max(1, b * 40 // n)
        bar_a = 'A' * max(1, a * 40 // n)
        print(f"  {label:>12s}  {b:>6,} ({b / n:>5.1%})  {a:>6,} ({a / n:>5.1%})  {sign}{shift:>+6,}  {bar_a}")

    # ======================================================================
    # Doctor breakdown: baseline vs final
    # ======================================================================
    print("\n" + "=" * 100)
    print("DOCTOR COMPARISON — BASELINE vs FULLY NORMALISED (n >= 20)")
    print("=" * 100)
    print(f"\n  {'Doctor':<30s}  {'n':>5s}  {'BASE':>8s}  {'NORM':>8s}  {'DELTA':>8s}  {'Visual'}")
    print(f"  {'-' * 30}  {'-' * 5}  {'-' * 8}  {'-' * 8}  {'-' * 8}  {'-' * 20}")

    doc_rows = []
    for doctor in baseline['by_doctor']:
        scores_b = baseline['by_doctor'][doctor]
        scores_a = final['by_doctor'].get(doctor, scores_b)
        if len(scores_b) < 20:
            continue
        b_mean = statistics.mean(scores_b)
        a_mean = statistics.mean(scores_a)
        doc_rows.append((doctor, len(scores_b), b_mean, a_mean, b_mean - a_mean))

    for doctor, count, b_mean, a_mean, improvement in sorted(doc_rows, key=lambda x: -x[4]):
        bar = '+' * min(30, int(improvement * 50))
        print(f"  {doctor:<30s}  {count:>5,}  {b_mean:>7.2%}  {a_mean:>7.2%}  {improvement:>+7.2%}  {bar}")

    # ======================================================================
    # Facility breakdown: baseline vs final
    # ======================================================================
    print("\n" + "=" * 100)
    print("FACILITY COMPARISON — BASELINE vs FULLY NORMALISED (n >= 50)")
    print("=" * 100)
    print(f"\n  {'Facility':<50s}  {'n':>5s}  {'BASE':>8s}  {'NORM':>8s}  {'DELTA':>8s}")
    print(f"  {'-' * 50}  {'-' * 5}  {'-' * 8}  {'-' * 8}  {'-' * 8}")

    fac_rows = []
    for fac in baseline['by_facility']:
        scores_b = baseline['by_facility'][fac]
        scores_a = final['by_facility'].get(fac, scores_b)
        if len(scores_b) < 50:
            continue
        b_mean = statistics.mean(scores_b)
        a_mean = statistics.mean(scores_a)
        fac_rows.append((fac, len(scores_b), b_mean, a_mean, b_mean - a_mean))

    for fac, count, b_mean, a_mean, improvement in sorted(fac_rows, key=lambda x: -x[4]):
        print(f"  {fac:<50s}  {count:>5,}  {b_mean:>7.2%}  {a_mean:>7.2%}  {improvement:>+7.2%}")

    # ======================================================================
    # Residual error analysis — what's left after all normalisations?
    # ======================================================================
    print("\n" + "=" * 100)
    print("RESIDUAL ERROR ANALYSIS — WHAT STILL DRIVES THE REMAINING WER?")
    print("=" * 100)

    # Sample 200 records and categorise the remaining differences
    import random
    random.seed(42)
    sample = random.sample(rows, min(200, len(rows)))

    categories = Counter()
    category_examples = defaultdict(list)

    for row in sample:
        dg = normalise(row['transcript_text'], set(range(1, 6)), is_deepgram=True)
        rpt = normalise(row['final_report_text'], set(range(1, 6)), is_deepgram=False)
        if not dg or not rpt:
            continue

        dg_words = set(dg.split())
        rpt_words = set(rpt.split())

        # Words in report but not in Deepgram (insertions by typist)
        report_only = rpt_words - dg_words
        # Words in Deepgram but not in report (Deepgram errors or dictator asides)
        deepgram_only = dg_words - rpt_words

        # Categorise report-only words
        for w in report_only:
            if re.match(r'^(dr|mr|mrs|ms)$', w):
                categories['titles_added'] += 1
            elif re.match(r'^\d+mm$', w):
                categories['measurement_formatting'] += 1
            elif w in ('clinical', 'history', 'findings', 'report', 'indication',
                       'impression', 'conclusion', 'technique', 'comparison', 'comment', 'opinion'):
                categories['section_headers'] += 1
            elif w in ('regards', 'kind', 'thank', 'referring', 'patient',
                       'electronically', 'signed', 'consultant', 'radiologist'):
                categories['sign_off_residual'] += 1
            else:
                categories['other_report_only'] += 1

        for w in deepgram_only:
            if w in ('um', 'uh', 'ah', 'hmm', 'sorry', 'actually', 'please',
                     'wait', 'scratch', 'delete', 'back', 'start', 'over', 'again'):
                categories['dictator_asides_filler'] += 1
                if len(category_examples['dictator_asides_filler']) < 5:
                    category_examples['dictator_asides_filler'].append(
                        f"  \"{w}\" (ID={row['id']}, Dr {row['doctor_family_name']})")
            elif w in ('e', 'a', 'i', 'o', 'n', 's', 'the', 'and', 'of', 'is', 'in', 'to', 'for'):
                categories['common_word_mismatch'] += 1
            else:
                categories['deepgram_misrecognition'] += 1

    total_cat = sum(categories.values())
    print(f"\n  Sampled 200 records. Word-level category breakdown:\n")
    print(f"  {'Category':<35s}  {'Count':>6s}  {'%':>6s}")
    print(f"  {'-' * 35}  {'-' * 6}  {'-' * 6}")
    for cat, count in categories.most_common():
        print(f"  {cat:<35s}  {count:>6,}  {count / total_cat:>5.1%}")

    if category_examples['dictator_asides_filler']:
        print(f"\n  Examples of dictator asides/filler:")
        for ex in category_examples['dictator_asides_filler']:
            print(f"    {ex}")

    # ======================================================================
    # Final summary
    # ======================================================================
    print("\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)
    layer_impacts = []
    prev = baseline['mean_wer']
    for i in range(1, 6):
        cur = results[i]['mean_wer']
        impact = prev - cur
        layer_impacts.append((layer_names[i], impact))
        prev = cur

    remaining = results[5]['mean_wer']
    print(f"\n  Starting WER:                     {baseline['mean_wer']:.2%}")
    for name, impact in layer_impacts:
        print(f"  {name:<45s}  saves {impact:>+.2%} pts")
    print(f"  {'─' * 55}")
    print(f"  Remaining WER after all normalisations:  {remaining:.2%}")
    print(f"  Total saved:                             {baseline['mean_wer'] - remaining:>+.2%} pts")
    print(f"\n  The remaining {remaining:.1%} WER is the 'true' gap between")
    print(f"  what Deepgram hears and what the typist produces — driven by:")
    print(f"    - Typist editorial rewording and formatting")
    print(f"    - Template expansion (short dictation -> full procedural text)")
    print(f"    - Medical term misrecognition by Deepgram")
    print(f"    - Dictator corrections/asides ('sorry', 'go back', etc.)")


if __name__ == '__main__':
    main()
