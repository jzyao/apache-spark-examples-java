Apache Spark Examples - Wordcount in Scala
==========================================

Aplicación que cuenta el número de ocurrencias de cada palabra en un corpus y posteriormente
cuente el número de ocurrencias de cada caracter en aquellas palabras que se consideran
más populares.

El ejemplo se ha implementado en Scala.

Comandos para la construcción del Jar:

    mvn clean
    mvn package

Para ejecutar el proyecto en nuestro entorno de pruebas local, simplemente tenemos
que ejecutar el script submit.sh que acompaña al proyecto en el directorio raíz del
mismo.

    $ ./submit.sh

El script ejecuta la aplicación en un solo nodo en modo local.