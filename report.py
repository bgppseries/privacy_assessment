from reportlab.graphics.shapes import Drawing, Polygon, PolyLine, Line
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.platypus import Image
from reportlab.lib.pagesizes import A4, letter
##  A4[0]是页宽；A4[1]是页高
from reportlab.lib.units import mm
from reportlab.lib.colors import Color, HexColor, CMYKColor
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import math

##全局变量
# 左边距
LEFT = 60
# 行高
LINE_HEIGHT = 24
# 起始高度
START_HIGH = 810
##注册字体
pdfmetrics.registerFont(TTFont('微软雅黑', 'msyh.ttc'))
"""
    注册字体，下载或者在C:\Windows\Fonts或其它途径找到字体文件，比如“微软雅黑”，然后将其复制到你的Python环境中的reportlab包里的fonts目录下，比如我的是C:\\Users\\13374\PycharmProjects\pythonProject\\venv\Lib\site-packages\\reportlab\\fonts
"""


def draw_page_header(c, reportid):
    """"""
    c.setFont("微软雅黑", 12)
    c_high = START_HIGH
    ##c.setFillColor(Color(0, 0, 0, alpha=0.5))  ##文字颜色
    c.drawImage('./report/images/logo.png', LEFT, c_high, width=40, height=11)
    c.drawString(LEFT + 42, c_high + 1, "大数据隐私脱敏与效果评估系统")
    c.drawString(LEFT + 300, c_high + 1, "评估任务编号：%d" % reportid)
    c_high -= 3.5
    c.setStrokeColor(Color(0, 0, 0, alpha=0.5))  ##设置边框颜色
    # c.line(LEFT, c_high, LEFT + 500, c_high)  ##draw line from (x1,y1) to (x2,y2)
    c.line(10 * mm, c_high, A4[0] - 10 * mm, c_high)  ##绘制线条


def draw_page_number(c, page, count):
    """
    画页脚
    :param c:canvas画布
    :param page: 当前页面是第几页
    :param count: 总页数数
    :return:
    """
    c.setFont("微软雅黑", 9)
    c.setStrokeColor(Color(0, 0, 0, alpha=0.5))  ##设置边框颜色
    c.line(10 * mm, 15 * mm, A4[0] - 10 * mm, 15 * mm)  ##绘制线条
    c.setFillColor(Color(0, 0, 0, alpha=0.5))  ##文字颜色
    c.drawCentredString(A4[0] / 2, 10 * mm, "Page %d of %d" % (page, count))


def draw_head(c):
    c_high = START_HIGH
    c_high -= 53.5
    c.setFont('微软雅黑', 16)
    c.drawString(LEFT + 150, c_high, '隐私保护效果评估报告')


def draw_info(c):
    c_high = START_HIGH
    c_high -= 55
    # 1.先绘制模块背景--矩形框
    ## 理解,质疑,成为，贴片
    c.drawImage("./report/images/info.png", 45, c_high - 290, width=505, height=272)

    # c.setFillOverprint(True)
    # c.setFillColor(Color(0, 241, 14, alpha=0.06))##文字颜色
    # c.roundRect(15,c_high-300,width=565,height=272,stroke=0,fill=1,radius=1)#绘制一个左下角位于 (x,y) 且宽度和高度为给定的矩形。
    # c.setFillColor(Color(0,0,0,alpha=0.07))
    # c.roundRect(40,c_high-38,width=75,height=22,stroke=0,fill=1,radius=5)
    # 2.加上文字
    c_high -= 100
    c.setFont("微软雅黑", 8)
    c.drawString(95, c_high + 1, "评估数据 : ")
    # todo 后端获取内容
    c_high -= 25
    c.drawString(95, c_high + 1, "隐私场景 : ")
    c_high -= 25
    c.drawString(95, c_high + 1, "评估任务提交时间 : ")
    c_high -= 25
    c.drawString(95, c_high + 1, "数据量 : ")
    c_high -= 25
    c.drawString(95, c_high + 1, "评估耗时 : ")
    c_high -= 25
    c.drawString(95, c_high + 1, "报告编号 : ")
    # 3.加上个六边形
    c.setFillOverprint(False)
    draw_info_hexagon(c, 425, c_high + 70, 60)
    draw_hexagon(c, 425, c_high + 70, 48, colors.whitesmoke)
    draw_hexagon(c, 425, c_high + 70, 36, colors.white)
    draw_hexagon(c, 425, c_high + 70, 24, colors.whitesmoke)
    draw_hexagon(c, 425, c_high + 70, 12, colors.white)
    #### l列表根据后端获取
    """"
        从左至右依次为：
        "可逆性","延伸控制性","复杂性","偏差性","信息损失性","合规性"
    """
    l = [55, 40, 35, 23, 44, 57]
    draw_Gradient_hexagon(c, 425, c_high + 70, l,60)
def draw_risk(c):
    c_high=START_HIGH
    c_high-=355##模块顶高度


def start(filename):
    c = canvas.Canvas(filename)
    c.bookmarkPage("title")
    c.addOutlineEntry("my book", "title", level=0)
    # 绘制页眉
    draw_page_header(c, 7454561654)
    # 绘制标题
    draw_head(c)
    ## 绘制概述模块
    draw_info(c)
    ## 绘制隐私风险报告
    draw_risk(c)
    # 绘制页脚
    draw_page_number(c, 1, 1)

    c.showPage()  # 保存当前画布页面
    c.save()  # 保存文件并关闭画布


def draw_info_hexagon(c, centre_x, centre_y, l):
    d = Drawing(400, 400)
    a = hexagon_vertices(l, centre_x, centre_y)
    p = Polygon(a, strokeColor=colors.whitesmoke, fillColor=colors.white)
    list = ["可逆性", "延伸控制性", "复杂性", "偏差性", "信息损失性", "合规性"]
    x = a[0] + 10
    y = a[1] - 3  ##可逆性，右下
    c.drawString(x, y, list[0])
    x = a[2] + 10
    y = a[3] - 3  ##延伸控制性，右上
    c.drawString(x, y, list[1])
    x = a[4]
    y = a[5] + 11  ##复杂性，上顶点
    c.drawCentredString(x, y, list[2])
    x = a[6] - 35
    y = a[7] - 3  ##偏差性，左上
    c.drawString(x, y, list[3])
    x = a[8] - 50
    y = a[9] - 3  ##信息损失性，左下
    c.drawString(x, y, list[4])
    x = a[10]
    y = a[11] - 17  ##合规性，下顶点
    c.drawCentredString(x, y, list[5])
    # c.line(a[0], a[1], a[6], a[7])
    # d.add(Line(a[0], a[1], a[6], a[7], strokeColor=colors.black))
    d.add(p)
    d.drawOn(c, 0, 0)


def draw_hexagon(c, centre_x, centre_y, l, col):
    d = Drawing(400, 400)
    a = hexagon_vertices(l, centre_x, centre_y)
    p = Polygon(a, strokeColor=colors.whitesmoke, fillColor=col, alpha=0.5)
    d.add(p)
    d.drawOn(c, 0, 0)


def draw_Gradient_hexagon(c, centre_x, centre_y, l,len):
    d = Drawing(400, 400)
    a = v_hexagon_vertices(l, centre_x, centre_y)
    e=hexagon_vertices(len,centre_x,centre_y)
    a.append(a[0])
    a.append(a[1])
    p = PolyLine(a, strokeColor=colors.aquamarine)
    d.add(PolyLine([e[0],e[1],e[6],e[7]],strokeColor=colors.whitesmoke,strokeWidth=1))
    d.add(PolyLine([e[2],e[3],e[8],e[9]],strokeColor=colors.whitesmoke,strokeWidth=1))
    d.add(PolyLine([e[4],e[5],e[10],e[11]],strokeColor=colors.whitesmoke,strokeWidth=1))
    d.add(p)
    d.drawOn(c, 0, 0)


def hexagon_vertices(length, centre_point_x, centre_point_y):
    """
    Calculate the vertices of a regular hexagon given the length of its sides and
    the coordinates of the centre point.

    :param length: Length of the sides of the hexagon
    :param top_point: Coordinates (x, y) of the centre point of the hexagon
    :return: List of tuples representing the vertices of the hexagon
    """
    x0 = centre_point_x
    y0 = centre_point_y
    angle = math.radians(60)
    vertices = []
    for i in range(1, 7):
        x = x0 + length * math.sin(i * angle)
        y = y0 - length * math.cos(i * angle)
        vertices.append(int(x))
        vertices.append(int(y))
    return vertices


def v_hexagon_vertices(length, centre_point_x, centre_point_y):
    """
    Calculate the vertices of a regular hexagon given the list of length and
    the coordinates of the centre point.
    :param length: Length of the sides of the hexagon
    :param top_point: Coordinates (x, y) of the centre point of the hexagon
    :return: List of tuples representing the vertices of the hexagon
    """
    x0 = centre_point_x
    y0 = centre_point_y
    angle = math.radians(60)
    vertices = []
    for i in range(1, 7):
        x = x0 + length[i - 1] * math.sin(i * angle)
        y = y0 - length[i - 1] * math.cos(i * angle)
        vertices.append(int(x))
        vertices.append(int(y))
    return vertices


if __name__ == '__main__':
    print("A4纸高度：", A4[1], "A4纸宽度：", A4[0])
    start("test.pdf")
