// Global object to track sort states of each column
var sortStates = {};
function sortTable(columnIndex, tableId, isNumeric) {
    var table = document.getElementById(tableId);
    var rowsArray = Array.from(table.getElementsByTagName("TR")).slice(1); // Exclude header row

    // Initialize or toggle the sort state
    if (!sortStates[tableId]) {
        sortStates[tableId] = {};
    }
    if (sortStates[tableId][columnIndex] === 'asc') {
        sortStates[tableId][columnIndex] = 'desc';
    } else {
        sortStates[tableId][columnIndex] = 'asc';
    }

    // Perform the sort
    rowsArray.sort(function(rowA, rowB) {
        var cellA = rowA.getElementsByTagName("TD")[columnIndex];
        var cellB = rowB.getElementsByTagName("TD")[columnIndex];

        var valueA = isNumeric ? parseFloat(cellA.innerHTML) : cellA.innerHTML.toLowerCase();
        var valueB = isNumeric ? parseFloat(cellB.innerHTML) : cellB.innerHTML.toLowerCase();

        if (sortStates[tableId][columnIndex] === 'asc') {
            return valueA > valueB ? 1 : -1;
        } else {
            return valueA < valueB ? 1 : -1;
        }
    });

    // Re-append sorted rows to the table
    rowsArray.forEach(function(row) {
        table.appendChild(row);
    });
}