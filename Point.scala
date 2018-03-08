class Point( val xc: Int, val yc: Int) {
  var x: Int = xc
  var y: Int = yc

  def move(dx: Int, dy: Int) {
    x = x + dx
    y = y + dy
    println("Point x location : " + x)
    println("Point y location : " + y)
  }

  // 1.规范化写法,scala 函数的返回值是最后一行代码；Unit，是Scala语言中数据类型的一种，表示无值,用作不返回任何结果的方法；
  def addInt0(a:Int,b:Int) : Int = {
    val total = a + b
    total
  }

  //2 ，不写明返回值的类型喝等于号，程序会自行判断，最后一行代码的执行结果为返回值，没有等于号必须加大括号
  def addInt1(a: Int, b: Int){
    a + b
  }

  //3 最简单写法：def ,{ },返回值都可以省略，此方法在spark编程中经常使用。
  //val addInt2 = (x: Int,y: Int) => x + y 此写法不符合新规范
  val addInt2: (Int, Int) => Int = (x: Int, y: Int) => x + y

  //  如果使用return返回r的值，那么需要明确指定函数返回类型
  def sum(n:Int):Int = {
    var r: Int = 0
    for (i <- 1 to 10)
      r = r*i
    r
  }

}

object Hello {
  def main(args: Array[String]) {
    val pt = new Point(0, 20)
    // Move to a new location
    pt.move(10, 10)
    val r = pt.addInt0(1,2)
    val r1: Unit = pt.addInt1(1,2)
    val r2 = pt.addInt2(5,6)
    println(r)
    println(r1)
    println(r2)
  }
}
