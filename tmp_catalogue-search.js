'use strict';

let allFilterValue;
let anyElementChanged = false;

$(document).ready(function () {
    disableDropdown();

    let sizeValue = $('#ss_type_id').val();
    
    $("select").selectpicker({
        dropupAuto: false,
        doneButtonText: "Search"
    });

    $('#ss_type_id').selectpicker('val', sizeValue);
    
    allFilterValue = $('#application-filter').val();
});

$(document).on('change', '#application-filter', function (e) {
    e.preventDefault();

    let currentSelectedValue = $(this).val();
    let unselectedFilter = allFilterValue.filter(function (a) {
        return currentSelectedValue.indexOf(a) === -1;
    });

    unselectedFilter.forEach(function (item) {
        $('.type_' + item).addClass('d-none');
    });

    currentSelectedValue.forEach(function (item) {
        $('.type_' + item).removeClass('d-none');
    });
});

// $(document).on('changed.bs.select', '#selClass, #selBody, #selYear, #selEngine, #selEnginevolume', function (e) {
$(document).
    on('changed.bs.select',
        '#selClass, #selBody, #selYear, #selEngine, #selEnginevolume',
        function (e, clickedIndex, isSelected, previousValue) {
            anyElementChanged = true;
        });

$(document).
    on('hidden.bs.select',
        '#selClass, #selBody, #selYear, #selEngine, #selEnginevolume',
        function (e) {
            e.preventDefault();

            let changedElement = e.target; // This is the element that triggered the event
            let previousValue = $(changedElement).data('previousValue'); // Get the previously selected value
            let newValue = $(changedElement).val(); // Get the new value of the changed element

            if (newValue.length != 0 && anyElementChanged) {
                // Update the previous value to the new value for the next change event
                // $(changedElement).data('previousValue', newValue);
                lockScreen();
                $('#catalogueSearchForm').submit();
            }
        });

function prepareSelectPicker (selector) {
    let value = $(selector).val();
    $(selector).selectpicker('destroy');
    $(selector).selectpicker({
        dropupAuto: false,
        doneButtonText: 'Search',
    });
    $(selector).selectpicker('val', value);
}

$(document).on('submit', '#catalogueSearchForm', function (event) {
    let brandValue = $('#selBrand').val();
    if (brandValue == '') {
        $('.js-error').removeClass('d-none');
        $('.js-validation-error').text('Select brand and model');
        unLockScreen();
        return false;
    } else if (brandValue != '') {
        let modelValue = $('#selClass').val();
        if (modelValue == '') {
            $('.js-error').removeClass('d-none');
            $('.js-validation-error').text('Select at least 1 class & model');
            $('#selBody').selectpicker('val', []);
            unLockScreen();
            return false;
        }
    }

    return true;
});

function initializeAllDropdowns () {

    disableDropdown();

    prepareSelectPicker('#selBrand');

    prepareSelectPicker('#selClass');

    prepareSelectPicker('#selBody');
    
    prepareSelectPicker('#selYear');
    
    prepareSelectPicker('#selEngine');
    
    prepareSelectPicker('#selEnginevolume');

    prepareSelectPicker('#ss_type_id');
}

function disableDropdown(){
    let totalClass = $('#selClass option').length;
    if (totalClass == 0) {
        $('#selClass, #selBody, #selYear, #selEngine, #selEnginevolume').
            attr('disabled', true);
    } else {
        $('#selBody, #selYear, #selEngine, #selEnginevolume').
            attr('disabled', true);
    }

    let classValue = $('#selClass').val();
    // let totalBody = $('#selBody option').length;
    if (classValue && classValue.length > 0) {
        $('#selBody, #selYear, #selEngine, #selEnginevolume').
            attr('disabled', false);
    }

    let totalYear = $('#selYear option').length;
    if (totalYear && totalYear == 0) {
        $('#selYear, #selEngine, #selEnginevolume').attr('disabled', true);
    }
}

$(document).on('click', '.product-image', function (e) {
    e.preventDefault();
    $('.modal-img-source').attr('src', '');
    let imgUrl = $(this).data('img');
    $('.modal-img-source').attr('src', imgUrl);
});
