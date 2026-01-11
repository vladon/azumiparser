'use strict';

document.onreadystatechange = function () {
    let state = document.readyState;

    // if (state == 'interactive') {
    //     $('#page-loader').removeClass('d-none');
    // } else if (state == 'complete') {
    //     setTimeout(function () {
    //         $('#page-loader').addClass('d-none');
    //     }, 1000);
    // }
};

$(document).ready(function () {
    $('#catalogue-part-form').on('submit', function () {
        $('#page-loader').removeClass('d-none');
    });

    $('#contact-form').on('submit', function () {
        $('#page-loader').removeClass('d-none');
    });

    $('#by-size-form').on('submit', function () {
        $('#page-loader').removeClass('d-none');
    });
});

function lockScreen () {
    $('#page-loader').removeClass('d-none');
}

function unLockScreen () {
    $('#page-loader').addClass('d-none');
}
