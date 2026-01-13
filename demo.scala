// demo.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._ 
import org.apache.log4j.{Level, Logger}

object BankingFraudAnalysis {
  def main(args: Array[String]): Unit = {
    
    // On masque les logs techniques
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("Banking Fraud Detection")
      .master("local[*]") 
      .getOrCreate()

    println("\n=== PARTIE 1 : Chargement et Prise en main ===")

    // 1. Chargement des CSV
    val rawTransactionsDf = spark.read.option("header", "true").option("inferSchema", "true").csv("data/transactions_data.csv")
    val cardsDf = spark.read.option("header", "true").option("inferSchema", "true").csv("data/cards_data.csv")
    val usersDf = spark.read.option("header", "true").option("inferSchema", "true").csv("data/users_data.csv")
    
    // Chargement JSON MCC
    val rawJsonMcc = spark.read.option("wholetext", true).text("data/mcc_codes.json")
    val mccDf = rawJsonMcc
      .select(from_json(col("value"), MapType(StringType, StringType)).alias("data"))
      .select(explode(col("data")).as(Seq("mcc_str", "edited_description")))

    // --- 1.1 Inspection des Schémas ---
    val datasets = Seq(
      ("Transactions", rawTransactionsDf), 
      ("Cards", cardsDf), 
      ("Users", usersDf),
      ("MCC Codes", mccDf)
    )

    println("\n--- Inspection des fichiers ---")
    datasets.foreach { case (name, df) =>
      println(s"\n*** Dataset: $name ***")
      println(s"Nombre de colonnes : ${df.columns.length}")
      df.printSchema()
      df.show(10, truncate = false)
    }

    println("\n>>> Question 1 : Types suspects ?")
    println("Réponse : La colonne 'amount' est vue comme STRING (à cause du symbole $). Elle doit être convertie en DOUBLE.")
    println("Réponse : La colonne 'zip' est vue comme DOUBLE. C'est incorrect (un code postal est une catégorie/chaîne), mais pas bloquant ici.")

    // --- 1.2 Nettoyage Préliminaire ---
    val transactionsDf = rawTransactionsDf
        .withColumn("amount_clean", regexp_replace(col("amount"), "\\$", "")) 
        .withColumn("amount", 
            when(col("amount_clean") === "" || col("amount_clean").isNull, lit(null)) 
            .otherwise(col("amount_clean").cast("Double")) 
        )
        .withColumn("ts", to_timestamp(col("date")))
        .drop("amount_clean")

    // --- 1.3 Analyse de Volumétrie ---
    println("\n--- Analyse de Volumétrie ---")
    
    val totalTransactions = transactionsDf.count()
    val totalClients = usersDf.count() 
    val totalCards = cardsDf.count()   
    val totalMerchants = transactionsDf.select("merchant_id").distinct().count()

    println(f"""
      |Résultats :
      | - Transactions totales : $totalTransactions
      | - Clients uniques (base): $totalClients
      | - Cartes uniques (base) : $totalCards
      | - Commerçants uniques   : $totalMerchants
      |
      | Interprétation : Ce sont les Transactions qui génèrent le plus de volume.
      """.stripMargin)

    // --- 1.4 Qualité des données ---
    println("\n--- Qualité des Données ---")

    val colsCheck = Seq("amount", "mcc", "merchant_city", "errors")
    val schema = transactionsDf.schema 

    println(f"${"Indicateur"}%-30s | ${"Nombre"}%-10s | ${"%"}%-10s")
    println("-" * 55)

    // Check Nulls avec gestion des types
    colsCheck.foreach { colName =>
      val dtype = schema(colName).dataType
      val condition = if (dtype == StringType) {
         col(colName).isNull || col(colName) === "NaN" || col(colName) === ""
      } else if (dtype == DoubleType || dtype == FloatType) {
         col(colName).isNull || isnan(col(colName)) 
      } else {
         col(colName).isNull
      }

      val missingCount = transactionsDf.filter(condition).count()
      val pct = (missingCount.toDouble / totalTransactions) * 100
      println(f"Valeurs manquantes ($colName)  | $missingCount%-10d | $pct%2.2f %%")
    }

    // Règles métiers
    val negativeAmount = transactionsDf.filter(col("amount") <= 0).count()
    val noMcc = transactionsDf.filter(col("mcc").isNull).count()
    val transErrors = transactionsDf.filter(col("errors").isNotNull && length(trim(col("errors"))) > 0).count()

    println(f"Montant <= 0                   | $negativeAmount%-10d | ${(negativeAmount.toDouble/totalTransactions)*100}%2.2f %%")
    println(f"Sans code MCC                  | $noMcc%-10d | ${(noMcc.toDouble/totalTransactions)*100}%2.2f %%")
    println(f"Transactions en Erreur         | $transErrors%-10d | ${(transErrors.toDouble/totalTransactions)*100}%2.2f %%")

    println("\n=== PARTIE 2 : Analyse des Montants & Temps ===")
    
    // 4. Analyse des montants (Moyenne, Min, Max, Médiane, Tranches)
    println("--- Statistiques des Montants ---")
    transactionsDf.select(
        round(avg("amount"), 2).alias("Moyenne"),
        min("amount").alias("Min"),
        max("amount").alias("Max")
    ).show()

    // Médiane (Approximate quantile pour performance)
    val medianAmount = transactionsDf.stat.approxQuantile("amount", Array(0.5), 0.0)(0)
    println(s"Médiane des montants : $medianAmount")

    // Distribution par tranche
    println("--- Distribution par Tranche ---")
    val bucketedDf = transactionsDf.na.drop(Seq("amount")).withColumn("tranche",
      when(col("amount") < 10, "< 10")
      .when(col("amount") >= 10 && col("amount") < 50, "10-50")
      .when(col("amount") >= 50 && col("amount") <= 200, "50-200")
      .otherwise("> 200")
    )
    bucketedDf.groupBy("tranche").count().orderBy("tranche").show()

    // 5. Analyse Temporelle (Heure, Jour, Mois)
    println("--- Analyse Temporelle ---")
    
    // Extraction Heure, Jour, Mois
    val timeDf = transactionsDf
        .withColumn("heure", hour(col("ts")))
        .withColumn("jour_semaine", date_format(col("ts"), "E"))
        .withColumn("mois", month(col("ts")))

    println("Top 5 Heures les plus actives :")
    timeDf.groupBy("heure").count().orderBy(desc("count")).show(5)

    println("Transactions par Jour de la semaine :")
    timeDf.groupBy("jour_semaine").count().orderBy(desc("count")).show()

    println("Transactions par Mois (Saisonnalité ?) :")
    timeDf.groupBy("mois").count().orderBy("mois").show()

    println("\n=== PARTIE 3 : Enrichissement & Erreurs ===")
    
    // 6. Jointure MCC & Analyse Catégories
    val transWithMcc = transactionsDf.withColumn("mcc_str", col("mcc").cast("String"))
    val joinedDf = transWithMcc.join(mccDf, Seq("mcc_str"), "left")

    println("--- Top 10 Catégories par Volume et Montant Moyen ---")
    joinedDf.groupBy("edited_description")
      .agg(
        count("*").alias("volume"),
        round(avg("amount"), 2).alias("montant_moyen")
      )
      .orderBy(desc("volume"))
      .show(10, truncate = false)

    // 7. Analyse des erreurs (Consigne : Types fréquents + Taux par Carte/Client)
    val dfWithErrors = joinedDf.withColumn("has_error", 
      when(col("errors").isNotNull && length(trim(col("errors"))) > 0 && col("errors") =!= "NULL", 1).otherwise(0)
    )

    println("--- Types d'erreurs les plus fréquents ---")
    dfWithErrors.filter(col("has_error") === 1)
      .groupBy("errors")
      .count()
      .orderBy(desc("count"))
      .show(5, truncate = false)

    println("--- Taux d'erreur par Client (Top 5 suspects) ---")
    dfWithErrors.groupBy("client_id")
      .agg(
        count("*").alias("total_tx"),
        sum("has_error").alias("total_errors")
      )
      .withColumn("error_rate_pct", round((col("total_errors") / col("total_tx")) * 100, 2))
      .filter(col("total_errors") > 0)
      .orderBy(desc("error_rate_pct"))
      .show(5)

    println("--- Taux d'erreur par Carte (Top 5 suspectes) ---")
    dfWithErrors.groupBy("card_id")
      .agg(
        count("*").alias("total_tx"),
        sum("has_error").alias("total_errors")
      )
      .withColumn("error_rate_pct", round((col("total_errors") / col("total_tx")) * 100, 2))
      .filter(col("total_errors") > 0)
      .orderBy(desc("error_rate_pct"))
      .show(5)

    println("\n=== PARTIE 4 : Détection de Fraude (Calculs) ===")

    // 8. Création d'indicateurs (Agrégations par carte et jour)
    val dailyStats = dfWithErrors
      .withColumn("day", to_date(col("ts")))
      .groupBy("card_id", "day")
      .agg(
        count("*").alias("nb_tx"),                          // Nb tx par jour
        sum("amount").alias("total_amount"),                // Montant total jour
        countDistinct("merchant_city").alias("nb_cities"),  // Nb villes diff
        sum("has_error").alias("nb_errors")                 // Nb erreurs
      )
      .withColumn("error_ratio", round(col("nb_errors") / col("nb_tx"), 4)) // Ratio erreurs

    println("--- Exemple d'indicateurs calculés (Top 5 montants) ---")
    dailyStats.orderBy(desc("total_amount")).show(5)

    // 9. Détection de comportements suspects
    println("--- ALERTE : Cartes Suspectes (DataFrame suspicious_cards) ---")
    val suspicious_cards = dailyStats.filter(
        col("nb_tx") > 20 || 
        col("nb_cities") >= 3 || 
        col("total_amount") > 2000
    )
    
    suspicious_cards.select("card_id", "day", "nb_tx", "total_amount", "nb_cities", "error_ratio")
      .orderBy(desc("nb_cities"))
      .show(10)
      
    println(s"Nombre de cas suspects détectés : ${suspicious_cards.count()}")

    println("\n=== BONUS : Score de Risque ===")
    val scoredDf = dailyStats.withColumn("risk_score", 
        (col("total_amount") / 1000) + (col("nb_cities") * 5) + (col("error_ratio") * 20)
    )
    println("--- Top 5 Cartes les plus Risquées (Score) ---")
    scoredDf.select("card_id", "day", "risk_score", "total_amount", "nb_cities")
      .orderBy(desc("risk_score"))
      .show(5)
      
    spark.stop()
  }
}

// Lancement direct
BankingFraudAnalysis.main(Array())