function show(){
    // 基于准备好的dom，初始化echarts实例
        var myChart = echarts.init(document.getElementById('range'));

        window.addEventListener('resize', function() {
            myChart.resize();
        });
        // 指定图表的配置项和数据
        var option = {
  title: {
    text: '',
    subtext: '',
    left: 'center'
  },
  tooltip: {
    trigger: 'item'
  },
  legend: {
    orient: 'vertical',
    left: 'left'
  },
  series: [
    {
      name: '隐私场景',
      type: 'pie',
      radius: '50%',
      data: [
        { value: 1048, name: '电信业务类' },
        { value: 735, name: '地图导航类' },
        { value: 580, name: '网上购物类' },
        { value: 484, name: '远程诊疗类' },
        { value: 300, name: '酒店服务类' }
      ],
      emphasis: {
        itemStyle: {
          shadowBlur: 10,
          shadowOffsetX: 0,
          shadowColor: 'rgba(0, 0, 0, 0.5)'
        }
      }
    }
  ]
};
    myChart.resize();
        // 使用刚指定的配置项和数据显示图表。
        myChart.setOption(option);
}
function showl1(){
                var Chart = echarts.init(document.getElementById('first'));
            Chart.resize();
            window.addEventListener('resize', function() {
            Chart.resize();
        });
            var option = {
  xAxis: {
    type: 'category',
    name:'日期',
    data: ['2023/\n11/15', '2023/\n11/19', '2023/\n11/23', '2023/\n11/26', '2023/\n12/01', '2023/\n12/03', '2023/\n12/05'],
     axisLabel: {
            //x轴文字的配置
            show: true,
            interval: 0,//使x轴文字显示全
           },
     formatter: function(params) {
              var newParamsName = "";
              var paramsNameNumber = params.length;
              var provideNumber = 3; //一行显示几个字
              var rowNumber = Math.ceil(paramsNameNumber / provideNumber);
              console.log(rowNumber);
              if (paramsNameNumber > provideNumber) {
                for (var p = 0; p < rowNumber; p++) {
                  var tempStr = "";
                  var start = p * provideNumber;
                  var end = start + provideNumber;
                  if (p == rowNumber - 1) {
                    tempStr = params.substring(start, paramsNameNumber);
                  } else {
                    tempStr = params.substring(start, end) + "\n";
                  }
                  newParamsName += tempStr;
                }
              } else {
                newParamsName = params;
              }
              return newParamsName;
            }
  },
  yAxis: {
    type: 'value',
    name:'评估任务数'
  },
  series: [
    {
      data: [1, 2, 5, 4, 7, 11, 13],
      type: 'bar'
    }
  ]
};
            Chart.setOption(option);

}
function show2(){
    var dom = document.getElementById("CPU");
    var myChart = echarts.init(dom);
    myChart.resize();
            window.addEventListener('resize', function() {
            myChart.resize();
        });
    var option = {
        title: {    // 标题组件
            //text: '系统性能运行状况'    // 标题文本
        },
        legend:{
            data:['CPU运行情况','内存使用情况']
        },
        tooltip: {    // 提示框组件
            trigger: 'axis',    // 触发类型（axis： 坐标轴触发）
//            formatter: function (params) {    // 提示框浮层内容格式器
//                params = params[0];
//                return params.axisValueLabel + '<br />' + params.marker
//                    + ' ' + params.seriesName
//                    + '<span style="font-weight: bold;float: right;">' + params.value[1] + '%</span>';
//            },
            axisPointer: {    // 坐标轴指示器配置项
                animation: false     // 是否开启动画
            }
        },
        toolbox: {    // 工具栏
            feature: {    // 各工具配置项
                //saveAsImage: {},    // 保存为图片
                dataView: {    // 数据视图工具，可以展现当前图表所用的数据，编辑后可以动态更新
                    show: true,    // 是否显示该工具
                    optionToContent: function(opt){    // 自定义 dataView 展现函数，用以取代默认的 textarea 使用更丰富的数据编辑。
                        var data = opt.series[0].data;
                        var table = '<table border=1 cellspacing=0 cellpadding=5><tbody><tr>'
                                + '<td align="center">时间</td>'
                                + '<td align="center">' + opt.series[0].name + '</td>'
                                + '</tr>';
                        for (var i = 0, l = data.length; i < l; i++) {
                            table += '<tr>'
                                + '<td align="center">' + data[i].value[0] + '</td>'
                                + '<td align="center">' + data[i].value[1] + '%</td>'
                                + '</tr>';
                        }
                        table += '</tbody></table>';
                        return table;
                    }
                }
            }
        },
        xAxis: {    // 直角坐标系 grid 中的 x 轴
            type: 'time',    // 坐标轴类型（time: 时间轴，适用于连续的时序数据，与数值轴相比时间轴带有时间的格式化，在刻度计算上也有所不同，例如会根据跨度的范围来决定使用月，星期，日还是小时范围的刻度。）
            splitLine: {    // 坐标轴在 grid 区域中的分隔线
                show: false // 是否显示分隔线。默认数值轴显示，类目轴不显示。
            }
        },
        yAxis: {    // 直角坐标系 grid 中的 y 轴
            type: 'value',    // 坐标轴类型（value: 数值轴，适用于连续数据。）
            boundaryGap: [0, '100%'],    // 坐标轴两边留白策略
            min: 0,        // 坐标轴刻度最小值
            max: 100,    // 坐标轴刻度最大值
            splitLine: {    // 坐标轴在 grid 区域中的分隔线
                show: false // 是否显示分隔线。默认数值轴显示，类目轴不显示。
            }
        },
        series: [
            {
                name: 'CPU利用率',    // 系列名称，用于tooltip的显示，legend 的图例筛选，在 setOption 更新数据和配置项时用于指定对应的系列。
                type: 'line',
                showSymbol: false,    // 是否显示 symbol, 如果 false 则只有在 tooltip hover 的时候显示。
                data: []    // 系列中的数据内容数组
            },
            {
                name:'内存利用率',
                type:'line',
                showSymbol: false,    // 是否显示 symbol, 如果 false 则只有在 tooltip hover 的时候显示。
                data: []    // 系列中的数据内容数组
            }
        ]
    };

    var data = [];
    var date=[];
    var iMax = 100;
    var timeUnit = 1000;
    var base = +new Date() - iMax * timeUnit;
    for (var i = 0; i < iMax; i++) {
        var now = new Date(base += timeUnit);
        var value1 = Math.round(Math.random() * 100, 2);
        var value2 = Math.round(Math.random() * 100, 2);
        data.push({
            name: now.toString(),
            value: [getTime(now), value1]
        });
        date.push({
            name:now.toString(),
            value:[getTime(now),value2]
        })
    }
    option.series[0].data = data;
    option.series[1].data = date;
    myChart.setOption(option, true); // 初始化

    var inter = setInterval(function(){
        var now = new Date(base += timeUnit);
        var value1 = Math.round(Math.random() * 100, 2);
        var value2=Math.round(Math.random() * 100, 2);
        data.shift();
        data.push({
            name: now.toString(),
            value: [getTime(now), value1]
        });
        date.shift();
        date.push({
            name:now.toString(),
            value:[getTime(now),value2]
        })
        option.series[0].data = data;
        option.series[1].data = date;
        myChart.setOption(option, true);
    }, timeUnit);

    function getTime(date){
        var ymd = [date.getFullYear(), date.getMonth() + 1, date.getDate()].join('/');
        var hour = date.getHours() < 10 ? ('0' + date.getHours()) : date.getHours();
        var minute = date.getMinutes() < 10 ? ('0' + date.getMinutes()) : date.getMinutes();
        var second = date.getSeconds() < 10 ? ('0' + date.getSeconds()) : date.getSeconds();
        var his = [hour, minute, second].join(':');
        return ymd + ' ' + his;
    }
}
