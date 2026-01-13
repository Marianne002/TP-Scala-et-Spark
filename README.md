# TP Scala et Spark - Marianne Grandjean 2026
Ce projet vise à effectuer une analyse exploratoire de données (EDA) et une détection de comportements frauduleux sur des transactions bancaires réelles anonymisées.

Le code est écrit entièrement en **Scala** en utilisant le framework **Apache Spark** (API DataFrame/Dataset), sans utilisation de **SQL** pur ni de librairies externes comme Pandas.

## Table des Matières

1. [Contexte du Projet](#-contexte-du-projet)
2. [Structure du Projet](#-structure-du-projet)
3. [Prérequis Techniques](#-prérequis-techniques)
4. [Installation et Lancement](#-installation-et-lancement)


## Contexte du Projet

En tant que membre de l'équipe **Risk & Fraud**, l'objectif est d'analyser un jeu de données "sale" et hétérogène **(CSV et JSON)** pour :
* Comprendre la volumétrie et la qualité des données.
* Identifier des patterns de consommation.
* Détecter des signaux faibles de fraude (transactions multiples, géographie incohérente, montants élevés).
* Calculer un score de risque par carte bancaire.


## Structure du Projet

L'arborescence du projet est organisée comme suit :

```text
/
|-- .bsp/                # Configuration Build Server Protocol
|-- .scala-build/        # Cache et build Scala
|-- data/                # Dossier contenant les sources de données
|   |-- transactions_data.csv
|   |-- cards_data.csv
|   |-- users_data.csv
|   |-- mcc_codes.json
|-- .gitattributes       # Configuration Git
|-- .gitignore           # Fichiers ignorés par Git
|-- demo.scala           # Script principal (Source Code)
|-- README.md            # Documentation du projet
|-- terminal.txt         # Logs ou sorties console
```


## Prérequis Techniques
Pour exécuter ce projet localement, vous avez besoin de :

**Java (JDK) :** Version 8, 11 ou 17 installée.

**Apache Spark :** Version 3.x installée.

**Scala :** Inclus avec Spark.

Pour les utilisateurs **Windows** :
- Vous devez avoir les binaires Hadoop (**winutils.exe**) configurés.
- Le fichier **winutils.exe** doit être placé dans %HADOOP_HOME%\bin.

## Installation et Lancement
Suivez ces étapes scrupuleusement pour éviter les erreurs de mémoire (OOM) ou de configuration Windows.

**1. Préparation de l'environnement (Windows)**

Assurez-vous que votre variable d'environnement Hadoop est définie dans votre terminal (PowerShell) avant de lancer Spark. Si vous avez installé **winutils.exe** dans **C:\hadoop\bin**, lancez :


```PowerShell
$env:HADOOP_HOME = "C:\hadoop"
```

**2. Données**
Assurez-vous que les 4 fichiers de données (transactions_data.csv, cards_data.csv, users_data.csv, mcc_codes.json) sont bien présents dans le dossier data/ à la racine du projet.

**3. Exécution du script**
Le volume de données étant conséquent (13 millions de transactions), il est nécessaire d'allouer plus de mémoire RAM à Spark pour éviter l'erreur java.lang.OutOfMemoryError: Java heap space.

Lancez la commande suivante dans le terminal à la racine du projet :

```PowerShell
C:\spark\bin\spark-shell.cmd --driver-memory 4g -i demo.scala
```

**Note :** Si votre chemin d'installation Spark est différent, adaptez le début de la commande (ex: **spark-shell --driver-memory 4g -i demo.scala** si Spark est dans votre PATH).