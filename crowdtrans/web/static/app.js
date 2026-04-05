/* ── Settings page helpers ── */

function togglePassword(id) {
    var input = document.getElementById(id);
    if (!input) return;
    input.type = input.type === 'password' ? 'text' : 'password';
}

function toggleNewSiteForm() {
    var form = document.getElementById('new-site-form');
    if (!form) return;
    form.classList.toggle('hidden');
}

function toggleSiteEdit(siteId) {
    var summary = document.getElementById('site-summary-' + siteId);
    var edit = document.getElementById('site-edit-' + siteId);
    if (!summary || !edit) return;
    summary.classList.toggle('hidden');
    edit.classList.toggle('hidden');
}

function updateAudioFields(selectEl, siteId) {
    var isKarisma = selectEl.value === 'karisma';
    var prefix = siteId === 'new' ? 'new' : siteId;

    // Update audio source value
    var audioFields = document.getElementById(prefix + '-audio-fields');
    if (audioFields) {
        var sourceInput = audioFields.querySelector('input[name="audio_source"]');
        if (sourceInput) {
            sourceInput.value = isKarisma ? 'sql_blob' : 'nfs';
        }
    }

    // Show/hide mount path
    var mountPath = document.getElementById(prefix + '-mount-path');
    if (mountPath) {
        if (isKarisma) {
            mountPath.classList.add('hidden');
        } else {
            mountPath.classList.remove('hidden');
        }
    }

    // Update default port
    var form = selectEl.closest('form');
    if (form) {
        var portInput = form.querySelector('input[name="db_port"]');
        if (portInput && (!portInput.value || portInput.value === '5432' || portInput.value === '1433')) {
            portInput.value = isKarisma ? '1433' : '5432';
        }
    }
}

/* ── Report view toggle ── */

var _tabIds = ['llm', 'formatted', 'raw'];

function showTab(tabName) {
    var activeClasses = ['bg-indigo-600', 'text-white', 'border-indigo-600'];
    var inactiveClasses = ['bg-white', 'text-gray-700', 'border-gray-300'];

    _tabIds.forEach(function (id) {
        var panel = document.getElementById('report-' + id);
        var btn = document.getElementById('btn-' + id);
        if (panel) {
            if (id === tabName) {
                panel.classList.remove('hidden');
            } else {
                panel.classList.add('hidden');
            }
        }
        if (btn) {
            if (id === tabName) {
                activeClasses.forEach(function (c) { btn.classList.add(c); });
                inactiveClasses.forEach(function (c) { btn.classList.remove(c); });
            } else {
                inactiveClasses.forEach(function (c) { btn.classList.add(c); });
                activeClasses.forEach(function (c) { btn.classList.remove(c); });
            }
        }
    });
}

// Backwards compat
function showFormatted() { showTab('formatted'); }
function showRaw() { showTab('raw'); }

/* ── Report copy ── */

function copyReport() {
    var llm = document.getElementById('report-llm');
    var formatted = document.getElementById('report-formatted');
    var raw = document.getElementById('report-raw');
    // Copy whichever view is currently visible
    var text = (llm && !llm.classList.contains('hidden')) ? llm :
               (formatted && !formatted.classList.contains('hidden')) ? formatted :
               (raw && !raw.classList.contains('hidden')) ? raw : null;
    if (!text) return;

    navigator.clipboard.writeText(text.textContent).then(function () {
        var label = document.getElementById('copy-label');
        label.textContent = 'Copied!';
        setTimeout(function () {
            label.textContent = 'Copy Report';
        }, 2000);
    }).catch(function () {
        // Fallback for older browsers / non-HTTPS
        var range = document.createRange();
        range.selectNodeContents(text);
        var sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
        document.execCommand('copy');
        sel.removeAllRanges();

        var label = document.getElementById('copy-label');
        label.textContent = 'Copied!';
        setTimeout(function () {
            label.textContent = 'Copy Report';
        }, 2000);
    });
}
