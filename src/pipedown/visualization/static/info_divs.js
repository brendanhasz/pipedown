function initialize_info_divs() {
    hide_all_info_divs()
    show_info_div('dag-info')
}

function show_info_div(id) {
    document.getElementById(id).style.display = "block";
}

function hide_all_info_divs() {
    var divs = document.getElementsByClassName("info-div");
    for (var i = 0; i < divs.length; i++) {
        divs.item(i).style.display = "none";
    }
}
