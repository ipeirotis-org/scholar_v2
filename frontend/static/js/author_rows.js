// Author tables (templates/_author_table.html): the whole row links to the
// results page, with a loading overlay while the next page is fetched.
// showLoadingOverlay is also used by the homepage search.
(function() {
    function showLoadingOverlay() {
        if (document.getElementById('loadingOverlay')) return;
        var overlay = document.createElement('div');
        overlay.id = 'loadingOverlay';
        overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(255,255,255,0.9);z-index:9999;display:flex;align-items:center;justify-content:center;flex-direction:column;';
        overlay.innerHTML = '<div class="spinner-border text-primary" role="status" style="width:3rem;height:3rem;"><span class="sr-only">Loading...</span></div><p class="mt-3" style="font-size:1.1rem;color:#333;">Loading author statistics...</p>';
        document.body.appendChild(overlay);
    }
    window.showLoadingOverlay = showLoadingOverlay;

    document.querySelectorAll('.author-table tbody tr[data-href]').forEach(function(row) {
        row.addEventListener('click', function(e) {
            // Modified clicks on the name link open a new tab; leave those alone.
            var link = e.target.closest('a');
            if (link && (e.ctrlKey || e.metaKey || e.shiftKey || e.button !== 0)) return;
            showLoadingOverlay();
            if (!link) window.location.href = row.dataset.href;
        });
    });

    // "See more" buttons show the rows a table hides at first, and hide them again.
    document.querySelectorAll('.author-table-toggle').forEach(function(button) {
        var table = document.getElementById(button.getAttribute('aria-controls'));
        if (!table) return;
        var moreLabel = button.textContent;
        button.addEventListener('click', function() {
            var expand = button.getAttribute('aria-expanded') !== 'true';
            table.querySelectorAll('tr[data-extra]').forEach(function(row) { row.hidden = !expand; });
            button.setAttribute('aria-expanded', String(expand));
            button.textContent = expand ? 'Show fewer' : moreLabel;
        });
    });

    // Remove the overlay when returning via the browser's back/forward buttons.
    window.addEventListener('pageshow', function() {
        var overlay = document.getElementById('loadingOverlay');
        if (overlay) overlay.remove();
    });
})();
