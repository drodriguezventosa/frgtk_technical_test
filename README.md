**Instrucciones para la ejecución**

**El proceso está planteado para tener conexión a internet para poder bajarse los ficheros directamente de la url que 
correponda. La ejecución debe ser desde la ruta del misma del proyecto. Los ficheros dentro del proyecto .parquet solo son utilizados en los test**

El programa consta de varias partes como son la importación de los datos, la limpieza de los mismos, 
la creación de dataset complementarios para no tener que trabajar directamente sobre el conjunto entero. 
Después se procesa la información aplicando filtrados optimizados y aplicando índices para su posterior exportación 
tanto a CSV como a EXCEL de los requerimientos pedidos.

```
Importing data ...
Cleaning data ...
Generating months ...
Generating weeks ...
Generating week metrics ...
Generating month metrics ...
Formatting results ...
Exporting results ...
Execution time: 38.032226708 seconds
```
***

*1 - Comandos a ejecutar para lanzar el proceso* 
```. venv/bin/activate``` --> ```pip install -r requirements.txt``` --> ```python main.py```
***
*2 - Para lanzar los test ejecutar ```pytest```*
***
*3 - Los test son un pequeño ejemplo para que se vea la utilización de pytest*

