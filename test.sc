val c = 12
val p =4
val limit = Math.ceil((c.toFloat/p)).toInt

for (offset <- 0.to(c).by(limit) if offset!=c) {
  println(offset);
}




