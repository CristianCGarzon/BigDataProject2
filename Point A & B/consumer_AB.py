from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
from operator import add
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
from pyspark.sql import Row, SparkSession
try:
    import json
except ImportError:
    import simplejson as json
import os 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11.jar pyspark-shell'

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def consumer():
    context = StreamingContext(sc, 30)
    dStream = KafkaUtils.createDirectStream(context, ["pointab"], {"metadata.broker.list": "localhost:9092"})
    dStream.foreachRDD(p1)
    context.start()
    context.awaitTermination()

def insertHashtags(htgs, spark, time):
    if htgs:
        rddHashs = sc.parallelize(htgs)
        rddHashs = rddHashs.filter(lambda x: len(x) > 3)
        # Convert RDD[String] to RDD[Row] to DataFrame
        hashtagsDataFrame = spark.createDataFrame(rddHashs.map(lambda x: Row(hashtag=x, hora=time)))
        hashtagsDataFrame.createOrReplaceTempView("hashtags")
        hashtagsDataFrame = spark.sql("use bigdata")
        hashtagsDataFrame = spark.sql("select hashtag, hora, count(*) as total from hashtags group by hashtag, hora order by total desc limit 50")
        hashtagsDataFrame.write.mode("append").saveAsTable("hashtag")
        print("Inserted hashtags FINISH")
    else:
        print("Is not hashtags avaliables to insert in hive")

def insertText(keys, spark, time):
    if keys:
        rddKeys = sc.parallelize(keys)
        rddKeys = rddKeys.map(lambda x: x.split()).flatMap(lambda x: x).map(lambda x: x.lower())
        rddKeys = rddKeys.filter(lambda x: "x" and x != "a" and x != "actualmente" and x != "acuerdo" and x != "adelante" and x != "ademas" and x != "además" and x != "adrede" and x != "afirmó" and x != "agregó" and x != "ahi" and x != "ahora" and x != "ahí" and x != "al" and x != "algo" and x != "alguna" and x != "algunas" and x != "alguno" and x != "algunos" and x != "algún" and x != "alli" and x != "allí" and x != "alrededor" and x != "ambos" and x != "ampleamos" and x != "antano" and x != "antaño" and x != "ante" and x != "anterior" and x != "antes" and x != "apenas" and x != "aproximadamente" and x != "aquel" and x != "aquella" and x != "aquellas" and x != "aquello" and x != "aquellos" and x != "aqui" and x != "aquél" and x != "aquélla" and x != "aquéllas" and x != "aquéllos" and x != "aquí" and x != "arriba" and x != "arribaabajo" and x != "aseguró" and x != "asi" and x != "así" and x != "atras" and x != "aun" and x != "aunque" and x != "ayer" and x != "añadió" and x != "aún" and x != "b" and x != "bajo" and x != "bastante" and x != "bien" and x != "breve" and x != "buen" and x != "buena" and x != "buenas" and x != "bueno" and x != "buenos" and x != "c" and x != "cada" and x != "casi" and x != "cerca" and x != "cierta" and x != "ciertas" and x != "cierto" and x != "ciertos" and x != "cinco" and x != "claro" and x != "comentó" and x != "como" and x != "con" and x != "conmigo" and x != "conocer" and x != "conseguimos" and x != "conseguir" and x != "considera" and x != "consideró" and x != "consigo" and x != "consigue" and x != "consiguen" and x != "consigues" and x != "contigo" and x != "contra" and x != "cosas" and x != "creo" and x != "cual" and x != "cuales" and x != "cualquier" and x != "cuando" and x != "cuanta" and x != "cuantas" and x != "cuanto" and x != "cuantos" and x != "cuatro" and x != "cuenta" and x != "cuál" and x != "cuáles" and x != "cuándo" and x != "cuánta" and x != "cuántas" and x != "cuánto" and x != "cuántos" and x != "cómo" and x != "d" and x != "da" and x != "dado" and x != "dan" and x != "dar" and x != "de" and x != "debajo" and x != "debe" and x != "deben" and x != "debido" and x != "decir" and x != "dejó" and x != "del" and x != "delante" and x != "demasiado" and x != "demás" and x != "dentro" and x != "deprisa" and x != "desde" and x != "despacio" and x != "despues" and x != "después" and x != "detras" and x != "detrás" and x != "dia" and x != "dias" and x != "dice" and x != "dicen" and x != "dicho" and x != "dieron" and x != "diferente" and x != "diferentes" and x != "dijeron" and x != "dijo" and x != "dio" and x != "donde" and x != "dos" and x != "durante" and x != "día" and x != "días" and x != "dónde" and x != "e" and x != "ejemplo" and x != "el" and x != "ella" and x != "ellas" and x != "ello" and x != "ellos" and x != "embargo" and x != "empleais" and x != "emplean" and x != "emplear" and x != "empleas" and x != "empleo" and x != "en" and x != "encima" and x != "encuentra" and x != "enfrente" and x != "enseguida" and x != "entonces" and x != "entre" and x != "era" and x != "eramos" and x != "eran" and x != "eras" and x != "eres" and x != "es" and x != "esa" and x != "esas" and x != "ese" and x != "eso" and x != "esos" and x != "esta" and x != "estaba" and x != "estaban" and x != "estado" and x != "estados" and x != "estais" and x != "estamos" and x != "estan" and x != "estar" and x != "estará" and x != "estas" and x != "este" and x != "esto" and x != "estos" and x != "estoy" and x != "estuvo" and x != "está" and x != "están" and x != "ex" and x != "excepto" and x != "existe" and x != "existen" and x != "explicó" and x != "expresó" and x != "f" and x != "fin" and x != "final" and x != "fue" and x != "fuera" and x != "fueron" and x != "fui" and x != "fuimos" and x != "g" and x != "general" and x != "gran" and x != "grandes" and x != "gueno" and x != "h" and x != "ha" and x != "haber" and x != "habia" and x != "habla" and x != "hablan" and x != "habrá" and x != "había" and x != "habían" and x != "hace" and x != "haceis" and x != "hacemos" and x != "hacen" and x != "hacer" and x != "hacerlo" and x != "haces" and x != "hacia" and x != "haciendo" and x != "hago" and x != "han" and x != "hasta" and x != "hay" and x != "haya" and x != "he" and x != "hecho" and x != "hemos" and x != "hicieron" and x != "hizo" and x != "horas" and x != "hoy" and x != "hubo" and x != "i" and x != "igual" and x != "incluso" and x != "indicó" and x != "informo" and x != "informó" and x != "intenta" and x != "intentais" and x != "intentamos" and x != "intentan" and x != "intentar" and x != "intentas" and x != "intento" and x != "ir" and x != "j" and x != "junto" and x != "k" and x != "l" and x != "la" and x != "lado" and x != "largo" and x != "las" and x != "le" and x != "lejos" and x != "les" and x != "llegó" and x != "lleva" and x != "llevar" and x != "lo" and x != "los" and x != "luego" and x != "lugar" and x != "m" and x != "mal" and x != "manera" and x != "manifestó" and x != "mas" and x != "mayor" and x != "me" and x != "mediante" and x != "medio" and x != "mejor" and x != "mencionó" and x != "menos" and x != "menudo" and x != "mi" and x != "mia" and x != "mias" and x != "mientras" and x != "mio" and x != "mios" and x != "mis" and x != "misma" and x != "mismas" and x != "mismo" and x != "mismos" and x != "modo" and x != "momento" and x != "mucha" and x != "muchas" and x != "mucho" and x != "muchos" and x != "muy" and x != "más" and x != "mí" and x != "mía" and x != "mías" and x != "mío" and x != "míos" and x != "n" and x != "nada" and x != "nadie" and x != "ni" and x != "ninguna" and x != "ningunas" and x != "ninguno" and x != "ningunos" and x != "ningún" and x != "no" and x != "nos" and x != "nosotras" and x != "nosotros" and x != "nuestra" and x != "nuestras" and x != "nuestro" and x != "nuestros" and x != "nueva" and x != "nuevas" and x != "nuevo" and x != "nuevos" and x != "nunca" and x != "o" and x != "ocho" and x != "os" and x != "otra" and x != "otras" and x != "otro" and x != "otros" and x != "p" and x != "pais" and x != "para" and x != "parece" and x != "parte" and x != "partir" and x != "pasada" and x != "pasado" and x != "paìs" and x != "peor" and x != "pero" and x != "pesar" and x != "poca" and x != "pocas" and x != "poco" and x != "pocos" and x != "podeis" and x != "podemos" and x != "poder" and x != "podria" and x != "podriais" and x != "podriamos" and x != "podrian" and x != "podrias" and x != "podrá" and x != "podrán" and x != "podría" and x != "podrían" and x != "poner" and x != "por" and x != "porque" and x != "posible" and x != "primer" and x != "primera" and x != "primero" and x != "primeros" and x != "principalmente" and x != "pronto" and x != "propia" and x != "propias" and x != "propio" and x != "propios" and x != "proximo" and x != "próximo" and x != "próximos" and x != "pudo" and x != "pueda" and x != "puede" and x != "pueden" and x != "puedo" and x != "pues" and x != "q" and x != "qeu" and x != "que" and x != "quedó" and x != "queremos" and x != "quien" and x != "quienes" and x != "quiere" and x != "quiza" and x != "quizas" and x != "quizá" and x != "quizás" and x != "quién" and x != "quiénes" and x != "qué" and x != "r" and x != "raras" and x != "realizado" and x != "realizar" and x != "realizó" and x != "repente" and x != "respecto" and x != "s" and x != "sabe" and x != "sabeis" and x != "sabemos" and x != "saben" and x != "saber" and x != "sabes" and x != "salvo" and x != "se" and x != "sea" and x != "sean" and x != "segun" and x != "segunda" and x != "segundo" and x != "según" and x != "seis" and x != "ser" and x != "sera" and x != "será" and x != "serán" and x != "sería" and x != "señaló" and x != "si" and x != "sido" and x != "siempre" and x != "siendo" and x != "siete" and x != "sigue" and x != "siguiente" and x != "sin" and x != "sino" and x != "sobre" and x != "sois" and x != "sola" and x != "solamente" and x != "solas" and x != "solo" and x != "solos" and x != "somos" and x != "son" and x != "soy" and x != "soyos" and x != "su" and x != "supuesto" and x != "sus" and x != "suya" and x != "suyas" and x != "suyo" and x != "sé" and x != "sí" and x != "sólo" and x != "t" and x != "tal" and x != "tambien" and x != "también" and x != "tampoco" and x != "tan" and x != "tanto" and x != "tarde" and x != "te" and x != "temprano" and x != "tendrá" and x != "tendrán" and x != "teneis" and x != "tenemos" and x != "tener" and x != "tenga" and x != "tengo" and x != "tenido" and x != "tenía" and x != "tercera" and x != "ti" and x != "tiempo" and x != "tiene" and x != "tienen" and x != "toda" and x != "todas" and x != "todavia" and x != "todavía" and x != "todo" and x != "todos" and x != "total" and x != "trabaja" and x != "trabajais" and x != "trabajamos" and x != "trabajan" and x != "trabajar" and x != "trabajas" and x != "trabajo" and x != "tras" and x != "trata" and x != "través" and x != "tres" and x != "tu" and x != "tus" and x != "tuvo" and x != "tuya" and x != "tuyas" and x != "tuyo" and x != "tuyos" and x != "tú" and x != "u" and x != "ultimo" and x != "un" and x != "una" and x != "unas" and x != "uno" and x != "unos" and x != "usa" and x != "usais" and x != "usamos" and x != "usan" and x != "usar" and x != "usas" and x != "uso" and x != "usted" and x != "ustedes" and x != "v" and x != "va" and x != "vais" and x != "valor" and x != "vamos" and x != "van" and x != "varias" and x != "varios" and x != "vaya" and x != "veces" and x != "ver" and x != "verdad" and x != "verdadera" and x != "verdadero" and x != "vez" and x != "vosotras" and x != "vosotros" and x != "voy" and x != "vuestra" and x != "vuestras" and x != "vuestro" and x != "vuestros" and x != "w" and x != "x" and x != "y" and x != "ya" and x != "yo" and x != "z" and x != "él" and x != "ésa" and x != "ésas" and x != "ése" and x != "ésos" and x != "ésta" and x != "éstas" and x != "éste" and x != "éstos" and x != "última" and x != "últimas" and x != "último" and x != "últimos" \
                                           and x != "rt" and x != "a's" and x != "able" and x != "about" and x != "above" and x != "according" and x != "accordingly" and x != "across" and x != "actually" and x != "after" and x != "afterwards" and x != "again" and x != "against" and x != "ain't" and x != "all" and x != "allow" and x != "allows" and x != "almost" and x != "alone" and x != "along" and x != "already" and x != "also" and x != "although" and x != "always" and x != "am" and x != "among" and x != "amongst" and x != "an" and x != "and" and x != "another" and x != "any" and x != "anybody" and x != "anyhow" and x != "anyone" and x != "anything" and x != "anyway" and x != "anyways" and x != "anywhere" and x != "apart" and x != "appear" and x != "appreciate" and x != "appropriate" and x != "are" and x != "aren't" and x != "around" and x != "as" and x != "aside" and x != "ask" and x != "asking" and x != "associated" and x != "at" and x != "available" and x != "away" and x != "awfully" and x != "b" and x != "be" and x != "became" and x != "because" and x != "become" and x != "becomes" and x != "becoming" and x != "been" and x != "before" and x != "beforehand" and x != "behind" and x != "being" and x != "believe" and x != "below" and x != "beside" and x != "besides" and x != "best" and x != "better" and x != "between" and x != "beyond" and x != "both" and x != "brief" and x != "but" and x != "by" and x != "c" and x != "c'mon" and x != "c's" and x != "came" and x != "can" and x != "can't" and x != "cannot" and x != "cant" and x != "cause" and x != "causes" and x != "certain" and x != "certainly" and x != "changes" and x != "clearly" and x != "co" and x != "com" and x != "come" and x != "comes" and x != "concerning" and x != "consequently" and x != "consider" and x != "considering" and x != "contain" and x != "containing" and x != "contains" and x != "corresponding" and x != "could" and x != "couldn't" and x != "course" and x != "currently" and x != "d" and x != "definitely" and x != "described" and x != "despite" and x != "did" and x != "didn't" and x != "different" and x != "do" and x != "does" and x != "doesn't" and x != "doing" and x != "don't" and x != "done" and x != "down" and x != "downwards" and x != "during" and x != "e" and x != "each" and x != "edu" and x != "eg" and x != "eight" and x != "either" and x != "else" and x != "elsewhere" and x != "enough" and x != "entirely" and x != "especially" and x != "et" and x != "etc" and x != "even" and x != "ever" and x != "every" and x != "everybody" and x != "everyone" and x != "everything" and x != "everywhere" and x != "ex" and x != "exactly" and x != "example" and x != "except" and x != "f" and x != "far" and x != "few" and x != "fifth" and x != "first" and x != "five" and x != "followed" and x != "following" and x != "follows" and x != "for" and x != "former" and x != "formerly" and x != "forth" and x != "four" and x != "from" and x != "further" and x != "furthermore" and x != "g" and x != "get" and x != "gets" and x != "getting" and x != "given" and x != "gives" and x != "go" and x != "goes" and x != "going" and x != "gone" and x != "got" and x != "gotten" and x != "greetings" and x != "h" and x != "had" and x != "hadn't" and x != "happens" and x != "hardly" and x != "has" and x != "hasn't" and x != "have" and x != "haven't" and x != "having" and x != "he" and x != "he's" and x != "hello" and x != "help" and x != "hence" and x != "her" and x != "here" and x != "here's" and x != "hereafter" and x != "hereby" and x != "herein" and x != "hereupon" and x != "hers" and x != "herself" and x != "hi" and x != "him" and x != "himself" and x != "his" and x != "hither" and x != "hopefully" and x != "how" and x != "howbeit" and x != "however" and x != "i" and x != "i'd" and x != "i'll" and x != "i'm" and x != "i've" and x != "ie" and x != "if" and x != "ignored" and x != "immediate" and x != "in" and x != "inasmuch" and x != "inc" and x != "indeed" and x != "indicate" and x != "indicated" and x != "indicates" and x != "inner" and x != "insofar" and x != "instead" and x != "into" and x != "inward" and x != "is" and x != "isn't" and x != "it" and x != "it'd" and x != "it'll" and x != "it's" and x != "its" and x != "itself" and x != "j" and x != "just" and x != "k" and x != "keep" and x != "keeps" and x != "kept" and x != "know" and x != "known" and x != "knows" and x != "l" and x != "last" and x != "lately" and x != "later" and x != "latter" and x != "latterly" and x != "least" and x != "less" and x != "lest" and x != "let" and x != "let's" and x != "like" and x != "liked" and x != "likely" and x != "little" and x != "look" and x != "looking" and x != "looks" and x != "ltd" and x != "m" and x != "mainly" and x != "many" and x != "may" and x != "maybe" and x != "me" and x != "mean" and x != "meanwhile" and x != "merely" and x != "might" and x != "more" and x != "moreover" and x != "most" and x != "mostly" and x != "much" and x != "must" and x != "my" and x != "myself" and x != "n" and x != "name" and x != "namely" and x != "nd" and x != "near" and x != "nearly" and x != "necessary" and x != "need" and x != "needs" and x != "neither" and x != "never" and x != "nevertheless" and x != "new" and x != "next" and x != "nine" and x != "no" and x != "nobody" and x != "non" and x != "none" and x != "noone" and x != "nor" and x != "normally" and x != "not" and x != "nothing" and x != "novel" and x != "now" and x != "nowhere" and x != "o" and x != "obviously" and x != "of" and x != "off" and x != "often" and x != "oh" and x != "ok" and x != "okay" and x != "old" and x != "on" and x != "once" and x != "one" and x != "ones" and x != "only" and x != "onto" and x != "or" and x != "other" and x != "others" and x != "otherwise" and x != "ought" and x != "our" and x != "ours" and x != "ourselves" and x != "out" and x != "outside" and x != "over" and x != "overall" and x != "own" and x != "p" and x != "particular" and x != "particularly" and x != "per" and x != "perhaps" and x != "placed" and x != "please" and x != "plus" and x != "possible" and x != "presumably" and x != "probably" and x != "provides" and x != "q" and x != "que" and x != "quite" and x != "qv" and x != "r" and x != "rather" and x != "rd" and x != "re" and x != "really" and x != "reasonably" and x != "regarding" and x != "regardless" and x != "regards" and x != "relatively" and x != "respectively" and x != "right" and x != "s" and x != "said" and x != "same" and x != "saw" and x != "say" and x != "saying" and x != "says" and x != "second" and x != "secondly" and x != "see" and x != "seeing" and x != "seem" and x != "seemed" and x != "seeming" and x != "seems" and x != "seen" and x != "self" and x != "selves" and x != "sensible" and x != "sent" and x != "serious" and x != "seriously" and x != "seven" and x != "several" and x != "shall" and x != "she" and x != "should" and x != "shouldn't" and x != "since" and x != "six" and x != "so" and x != "some" and x != "somebody" and x != "somehow" and x != "someone" and x != "something" and x != "sometime" and x != "sometimes" and x != "somewhat" and x != "somewhere" and x != "soon" and x != "sorry" and x != "specified" and x != "specify" and x != "specifying" and x != "still" and x != "sub" and x != "such" and x != "sup" and x != "sure" and x != "t" and x != "t's" and x != "take" and x != "taken" and x != "tell" and x != "tends" and x != "th" and x != "than" and x != "thank" and x != "thanks" and x != "thanx" and x != "that" and x != "that's" and x != "thats" and x != "the" and x != "their" and x != "theirs" and x != "them" and x != "themselves" and x != "then" and x != "thence" and x != "there" and x != "there's" and x != "thereafter" and x != "thereby" and x != "therefore" and x != "therein" and x != "theres" and x != "thereupon" and x != "these" and x != "they" and x != "they'd" and x != "they'll" and x != "they're" and x != "they've" and x != "think" and x != "third" and x != "this" and x != "thorough" and x != "thoroughly" and x != "those" and x != "though" and x != "three" and x != "through" and x != "throughout" and x != "thru" and x != "thus" and x != "to" and x != "together" and x != "too" and x != "took" and x != "toward" and x != "towards" and x != "tried" and x != "tries" and x != "truly" and x != "try" and x != "trying" and x != "twice" and x != "two" and x != "u" and x != "un" and x != "under" and x != "unfortunately" and x != "unless" and x != "unlikely" and x != "until" and x != "unto" and x != "up" and x != "upon" and x != "us" and x != "use" and x != "used" and x != "useful" and x != "uses" and x != "using" and x != "usually" and x != "uucp" and x != "v" and x != "value" and x != "various" and x != "very" and x != "via" and x != "viz" and x != "vs" and x != "w" and x != "want" and x != "wants" and x != "was" and x != "wasn't" and x != "way" and x != "we" and x != "we'd" and x != "we'll" and x != "we're" and x != "we've" and x != "welcome" and x != "well" and x != "went" and x != "were" and x != "weren't" and x != "what" and x != "what's" and x != "whatever" and x != "when" and x != "whence" and x != "whenever" and x != "where" and x != "where's" and x != "whereafter" and x != "whereas" and x != "whereby" and x != "wherein" and x != "whereupon" and x != "wherever" and x != "whether" and x != "which" and x != "while" and x != "whither" and x != "who" and x != "who's" and x != "whoever" and x != "whole" and x != "whom" and x != "whose" and x != "why" and x != "will" and x != "willing" and x != "wish" and x != "with" and x != "within" and x != "without" and x != "won't" and x != "wonder" and x != "would" and x != "wouldn't" and x != "x" and x != "y" and x != "yes" and x != "yet" and x != "you" and x != "you'd" and x != "you'll" and x != "you're" and x != "you've" and x != "your" and x != "yours" and x != "yourself" and x != "yourselves" and x != "z" and x != "zero")
        # Convert RDD[String] to RDD[Row] to DataFrame
        keywordsDataFrame = spark.createDataFrame(rddKeys.map(lambda x: Row(keywords=x, hora=time)))
        keywordsDataFrame.createOrReplaceTempView("keywords")
        keywordsDataFrame = spark.sql("use bigdata")
        keywordsDataFrame = spark.sql("select keywords, hora, count(*) as total from keywords group by keywords, hora order by total desc limit 50")
        keywordsDataFrame.write.mode("append").saveAsTable("keywords")
        print("Inserted keywords FINISH")
    else:
        print("Is not keywords avaliables to insert in hive")

def p1(time,rdd):
    rdd = rdd.map(lambda x: json.loads(x[1]))
    records = rdd.collect() #Return a list with tweets
    spark = getSparkSessionInstance(rdd.context.getConf())
    
    #########################################Punto A#########################################
    htgs = [element["entities"]["hashtags"] for element in records if "entities" in element]
    htgs = [x for x in htgs if x] #remove empty hashtags
    htgs = [element[0]["text"] for element in htgs]
    insertHashtags(htgs, spark, time)
    #########################################Punto A#########################################

    #########################################Punto B#########################################
    keys = [element["text"] for element in records if "text" in element]
    insertText(keys, spark, time)
    #########################################Punto B#########################################

if __name__ == "__main__":
    print("Starting to read tweets")
    sc = SparkContext(appName="ConsumerAB")
    consumer()
    