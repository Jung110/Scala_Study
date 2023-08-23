def upperString(value: String): Option[String] = {
  val _value = Option.apply(value)
  if(_value.nonEmpty) {
    if (_value.get.trim.isEmpty) None
    else Some(_value.get.toUpperCase)
  } else None
}
upperString(null)
upperString("").isEmpty
upperString("").isDefined
// NoSuchElementException upperString("").get
upperString("").getOrElse("default")
upperString("val").isEmpty
upperString("val").isDefined
println(upperString("val").get)