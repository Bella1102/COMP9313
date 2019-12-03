
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io.File
import scala.xml.XML
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set
import scalaj.http._
import play.api.libs.json._


object CaseIndex {

  // get all files from input dir
  def getFiles(dir: String): List[File] = {
    val path: File = new File(dir)
    if (!path.exists || !path.isDirectory) {
      return List[File]()
    }
    val file: List[File] = path.listFiles().toList
    return file
  }


  def main(args: Array[String]) {
    // get input dir from argument
    val inputDir = args(0)
    // get list of case files
    val all_files = getFiles(inputDir)

    // create an index
    val new_index = Http("http://localhost:9200/legal_idx")
                      .method("PUT").header("Content-Type", "application/json")
                      .option(HttpOptions.readTimeout(10000)).asString
    // create a new mapping
    val new_mapping = Http("http://localhost:9200/legal_idx/cases/_mapping?pretty")
                        .postData("""{"cases":{"properties":
                          {"id":{"type":"text"},
                          "name":{"type":"text"},
                          "url":{"type":"text"},
                          "catchphrase":{"type":"text"},
                          "sentence":{"type":"text"},
                          "person":{"type":"text"},
                          "location":{"type":"text"},
                          "organization":{"type":"text"}}}}""")
                        .method("PUT").header("Content-Type", "application/json")
                        .option(HttpOptions.readTimeout(10000)).asString

    // processing each file
    for (file <- all_files) {
      // load XML
      val xml = XML.loadFile(file)
      println(file)
      // get filename: . | * must be used with \\ together
      val filename = file.getName().split("\\.")(0)

      // get all information from XML
      val name = (xml \ "name").text.filter(_ >= ' ')
      val url = (xml \ "AustLII").text.filter(_ >= ' ')

      val catchphrases = (xml \ "catchphrases" \ "catchphrase")
      val catchphrase_all = new StringBuilder("")
      for (catchphrase <- catchphrases) {
        catchphrase_all ++= catchphrase.text + " "
      }

      val sentences = (xml \ "sentences" \ "sentence")
      val sentence_list = new ListBuffer[String]()
      for (sentence <- sentences) {
        sentence_list += sentence.text.replace("\"", "\\\"")
      }


      // Create set to store name entities
      val people: Set[String] = Set()
      val locations: Set[String] = Set()
      val organizations: Set[String] = Set()

      // process all sentences
      for (s <- sentence_list) {
        var NLP_sentence = Http("""http://localhost:9000/?properties=%7B'annotators':'ner','outputFormat':'json'%7D""")
                            .postData(s).method("POST").header("Content-Type", "application/json")
                            .option(HttpOptions.readTimeout(60000)).asString.body
        // parse reponse to JSON object
        val NLP_json: JsValue = Json.parse(NLP_sentence)
        // get all entities
        val tokens = NLP_json \\ "tokens"

        for (token <- tokens) {
          val text = token \\ "originalText"
          val ner = token \\ "ner"
          var n = 0
          for (n <- 0 until text.length) {
            if (ner(n).toString == "\"PERSON\"") {
              people.add(text(n).toString)
            } else if (ner(n).toString == "\"LOCATION\"") {
              locations.add(text(n).toString)
            } else if (ner(n).toString == "\"ORGANIZATION\"") {
              organizations.add(text(n).toString)
            }
          }
        }
      }

      // convert to String
      val people_list = "[" + people.toList.mkString(",")+"]"
      val locations_list = "[" + locations.toList.mkString(",")+"]"
      val organizations_list = "[" + organizations.toList.mkString(",")+"]"
      val sentence_str = sentence_list.map(x=>"\"" + x.filter(_ >= ' ') + "\"").mkString(",")
      val new_sentence_list = "[" + sentence_str + "]"

      // create a new document
      val Data = s"""{"id":"${filename}","name":"${name}","url":"${url}",
                  "catchphrase":"${catchphrase_all.toString.filter(_ >= ' ')}",
                  "sentence":${new_sentence_list},
                  "person":${people_list},"location":${locations_list},"organization":${organizations_list}}"""

      val update_document = Http("http://localhost:9200/legal_idx/cases/"+filename+"?pretty")
                          .postData(Data).method("PUT").header("Content-Type", "application/json")
                          .option(HttpOptions.readTimeout(10000)).asString

    }
  }
}


