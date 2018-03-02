# scala_note
### 1.

> def sum(x: Int, y: Int) = x + y

Scala引入不带括号的函数定义方式有什么用意呢？ 我想主要是方便把一个函数当作变量来处理. 在Scala中， 子类可以通过override val 的方式覆盖父类或特质
中同名的def , 
例如：

>class Father{
   def name = "Archer"
}
class Child{
   override val name = "Jack"
}
Scala作者建议，如果一个函数在逻辑上表达一种属性的返回值，那么在定义函数时尽量使用不带括号的写法，因为这样看上去更像一个类的属性，而不像一个方法。
由于不带括号的函数比带括号的函数在使用上更严格，因此将来要把一个带括号的函数定义改为不带括号的函数定义就比较麻烦——需要先将所有带括号的函数调用,
比如name(), 统统改为不带括号的。

### 2.
