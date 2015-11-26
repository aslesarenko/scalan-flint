package scalan.meta

object ScalanFlintBoilerplateTool extends BoilerplateTool {
  lazy val scalanFlintConfig = CodegenConfig(
    name = "scalan-flint",
    srcPath = "scalan-flint-core/src/main/scala",
    entityFiles = List(
      "scalan/flint/DataFrames.scala"),
    baseContextTrait = "Scalan",
    seqContextTrait = "ScalanSeq",
    stagedContextTrait = "ScalanExp",
    extraImports = List(
        "scala.reflect.runtime.universe._",
        "scalan.common.Default"),
    entityTypeSynonyms = Map()
  )

  override def getConfigs(args: Array[String]) = Seq(scalanFlintConfig)

  override def main(args: Array[String]) = {
    new EntityManagement(scalanFlintConfig).generateAll()
  }
}
