package date_7_2_2022

class DemoClass(val name: String, val roll: Int) {

  var this.name: String = name
  var this.roll: Int = roll

  def printDetail() {
    println ("Name: " + name);
    println ("Roll No. : " + roll);
  }
}

object Demo {
  def main(args: Array[String]) {

    val obj1 = new DemoClass("bhargav",4)

    //calling method
    obj1.printDetail()
  }
}