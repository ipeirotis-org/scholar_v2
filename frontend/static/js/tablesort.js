var sortStates = {};
function sortTable(columnIndex, tableId, isNumeric) {
    var table = document.getElementById(tableId);
    var rowsArray = Array.from(table.getElementsByTagName("TR")).slice(1);

    if (!sortStates[tableId]) {
        sortStates[tableId] = {};
    }
    if (sortStates[tableId][columnIndex] === "asc") {
        sortStates[tableId][columnIndex] = "desc";
    } else {
        sortStates[tableId][columnIndex] = "asc";
    }

    rowsArray.sort(function(rowA, rowB) {
        var cellA = rowA.getElementsByTagName("TD")[columnIndex];
        var cellB = rowB.getElementsByTagName("TD")[columnIndex];

        var valueA = isNumeric ? parseFloat(cellA.textContent) : cellA.textContent.toLowerCase();
        var valueB = isNumeric ? parseFloat(cellB.textContent) : cellB.textContent.toLowerCase();

        if (sortStates[tableId][columnIndex] === "asc") {
            return valueA > valueB ? 1 : -1;
        } else {
            return valueA < valueB ? 1 : -1;
        }
    });

    var tbody = table.querySelector("tbody") || table;
    rowsArray.forEach(function(row) {
        tbody.appendChild(row);
    });
}
