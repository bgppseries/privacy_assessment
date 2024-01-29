function submitForm() {
var form = document.getElementById('upload'),
formData = new FormData(form);
$.ajax({
    url:"http://127.0.0.1:5000/api/file/upload",
    type:"post",
    data:formData,
    processData:false,
    contentType:false,
    done: function (res) {
        uxAlert('finish:' + res);
    },
    success:function(res){
        if(res){
            uxAlert("上传成功！");
        }
        console.log(res);
    },
    error:function(err){
        uxAlert("网络连接失败,稍后重试",err);
    }
});

    return false;
};
function uxAlert(message) {
    $('<div></div>').html(message).dialog({
        title: 'Alert',
        modal: true,
        buttons: {
            Ok: function () {
                $(this).dialog('close');
            }
        }
    });
}


