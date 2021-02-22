var img_ele = null;
var x_cursor = 0;
var y_cursor = 0;
var x_img_ele = 0;
var y_img_ele = 0;

function start_drag() {
    img_ele = document.getElementById('dag-svg');
    x_img_ele = window.event.clientX - img_ele.offsetLeft;
    y_img_ele = window.event.clientY - img_ele.offsetTop;
}

function stop_drag() {
    img_ele = null;
}

function while_drag() {
    var x_cursor = window.event.clientX;
    var y_cursor = window.event.clientY;
    if (img_ele !== null) {
        img_ele.style.left = (x_cursor - x_img_ele) + 'px';
        img_ele.style.top = ( window.event.clientY - y_img_ele) + 'px';
    }
}

function zoom(scroll_event) {
    var zoom_scaler = 1.0;
    if (scroll_event.wheelDelta > 0) {
        zoom_scaler = 0.9;
    } else {
        zoom_scaler = 1.1;
    }
    img_ele = document.getElementById('dag-svg');
    var pre_width = img_ele.getBoundingClientRect().width;
    var pre_height = img_ele.getBoundingClientRect().height;
    img_ele.style.width = (pre_width * zoom_scaler) + 'px';
    img_ele.style.height = (pre_height * zoom_scaler) + 'px';
    img_ele = null;
}

/* Assign functions to event listenters */
document.getElementById('dag-viewer').addEventListener('mousedown', start_drag);
document.getElementById('dag-viewer').addEventListener('mousemove', while_drag);
document.getElementById('dag-viewer').addEventListener('mouseup', stop_drag);
document.getElementById('dag-viewer').addEventListener('wheel', zoom);
