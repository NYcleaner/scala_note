package com.linewell.modeldesgin.singleton
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

//1.单列对象》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》

//2.scala apply 》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》》

/**
  * 伴生类和伴生对象的 apply
  * class Student是伴生对象Object Student的伴生类
  * object Student是伴生类class Student的伴生对象
  */


//伴生类参数var是定义了成员属性
class Student0(var name:String,var address:String){

  private var phone="110"
  //直接访问伴生对象的私有成员
  def infoCompObj():Unit = println("伴生类中访问伴生对象：" + Student0.sno)
}

object Student0 {

  private var sno:Int = 100 //成员被下面的方法调用
  def incrementSno(): Int= {  // 需要加上: Int type annotation require for public member scala

    sno += 1 //加1
    sno  //返回sno
  }

  //定义apply方法,实例化伴生类
  def apply(name1:String,address1:String)= new Student0(name1,address1)

//  main方法在单列对象中，并且可以调用单列对象
  def main(args: Array[String]): Unit = {

    println("单例对象：" + Student0.incrementSno()) //单例对象被调用，此时sno成员变为101

    //实例化伴生类
    val obj = new Student0("yy","bj")
    obj.infoCompObj()
    //另一种实例化伴生类
    new Student0("yy1","bj2").infoCompObj()

    println("通过伴生对象的apply方法访问伴生类成员，实际是通过apply方法进行了对象实例化，避免了手动new对象:")
    val obj2 = Student0("yy_apply","bj_apply")
    println(obj2.name)
    println(obj2.address)

    println(obj.name)
    println(obj.address)
  }
}



//3.构造函数>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
/**在scala中，如果不指定主构造函数，编译器将创建一个主构造函数的构造函数。
所有类的主体的声明都被视为构造函数的一部分。它也被称为默认构造函数。
Scala默认主构造函数示例
class body里所有除去字段和方法声明的语句，剩下的一切都是主构造函数的，它们在class实例化时一定会被执行
如果类名之后没有参数，则该类具备一个无参主构造函数，这样一个构造函数仅仅是简单地执行类体中的所有语句而已。
  */
class Student1{ // 每个类都有主构造函数， 和类的定义交织在一起。
  println("Hello from default constructor") //主体的声明都被视为主构造函数的一部分
}


/**3.1
Scala主要构造函数
Scala提供了一个类的主构造函数的概念。如果代码只有一个构造函数，则可以不需要定义明确的构造函数。
它有助于优化代码，可以创建具有零个或多个参数的主构造函数。
Scala主构造函数showDetails示例
*/
class Student2(id:Int, name:String){
  def showDetails2(){ //辅助构造函数showDetails2
    println(id + " " + name)
  }
}

object Demo2{
  def main(args:Array[String]){
    val s = new Student2(1010, "Maxsu")
    s.showDetails2()
  }
}

/**3.2
Scala次要(辅助)构造器
可以在类中创建任意数量的辅助构造函数，必须要从辅助构造函数内部调用主构造函数。
this关键字用于从其他构造函数调用构造函数。
辅助构造函数的名称为this，这主要是考虑到在C++和Java中，构造函数名与类名同名，
当更改类名时需要同时修改构造函数名，因此使用this为构造函数名使程序可靠性更强
*/

class Student3(id:Int, name:String){ //id, name构造函数参数
  var age:Int = 0
  def showDetails3(){ //辅助构造函数showDetails3
    println(id +" "+name+" "+ age)
  }
  def this(id:Int, name:String, age:Int){
    this(id, name)       // 每一个辅助构造函数都必须以一个对先前已定义的其他辅助构造函数或主构造函数的调用开始。
    this.age = age
  }
}

object Demo3{
  def main(args:Array[String]){
    val s = new Student3(1010, "Maxsu", 25); //三个参数，借助this辅助构建函数来实例化
    s.showDetails3()
  }
}

//3.2.1 每一个辅助构造函数都必须以一个对先前已定义的其他辅助构造函数或主构造函数的调用开始。如：
class Time {
  private var hr = 0  //主构造函数体
  private var min = 0 //主构造函数体

  def this (hr : Int) {
    this ()    //调用主构造函数
    this.hr = hr //更改类的成员属性
  }
  def this (hr : Int, min : Int) {
    this (hr)    //调用辅助构造函数
    this.min = min
    println(min) //更改类的成员属性
  }
}

object Time extends App {
  val t1 = new Time
  val t2 = new Time (16)
  val t3 = new Time (16, 27)
}

//3.3 在scala中，可以重载构造函数。下面我们来看一个例子。

class Student4(id:Int){
  def this(id:Int, name:String)={
    this(id) //重载构造函数
    println(id + " " + name)
  }
  println(id)
}

object Demo4{
  def main(args:Array[String]){
    new Student4(100,"Minsu")
    new Student4(101) //重载构造函数
  }
}
