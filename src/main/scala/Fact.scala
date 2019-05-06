object Fact extends App{

  def factorial(n: Int):Int = if (n == 0) 1 else n * factorial(n - 1)

  print(factorial(5))
}