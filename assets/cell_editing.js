document.addEventListener('DOMContentLoaded', function () {
    const table = document.querySelector('#editable-table');

    table.addEventListener('click', function (event) {
        const cell = event.target.closest('td');
        if (cell && !cell.querySelector('input')) {
            cell.click();
            setTimeout(() => {
                const input = cell.querySelector('input');
                if (input) {
                    const clickPosition = event.offsetX;
                    input.focus();
                    input.setSelectionRange(clickPosition, clickPosition);
                }
            }, 0);
        }
    });
});
