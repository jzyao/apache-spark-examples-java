Apache Spark Examples - Simple Sql App
======================================

Ejemplo sencillo que muestra los pasos necesarios para poder ejecutar una
consulta SQL sobre un cluster con soporte para Apache Spark.

Comandos para la construcción del Jar:

    mvn clean
    mvn package

Para ejecutar el proyecto en nuestro entorno de pruebas local, simplemente tenemos
que ejecutar el script submit.sh que acompaña al proyecto en el directorio raíz del
mismo.

    $ ./submit.sh

El script ejecuta la aplicación en un solo nodo en modo local.