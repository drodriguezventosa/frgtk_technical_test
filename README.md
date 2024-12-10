**Instrucciones para la ejecución**

**El proceso está planteado para tener conexión a internet para poder bajarse los ficheros directamente de la url que 
correponda. La ejecución debe ser desde la ruta del misma del proyecto. Los ficheros dentro del proyecto .parquet solo son utilizados en los test**

El programa consta de varias partes como son la importación de los datos, la limpieza de los mismos, 
la creación de dataset complementarios para no tener que trabajar directamente sobre el conjunto entero. 
Después se procesa la información aplicando filtrados optimizados y aplicando índices para su posterior exportación 
tanto a CSV como a EXCEL de los requerimientos pedidos.

```
Init objects ...
*** 0.00547895899999995 seconds ***
Importing data ...
*** 7.456109084 seconds ***
Cleaning data ...
*** 5.637706417 seconds ***
Adding more columns ...
*** 30.625991959 seconds ***
Generating week metrics ...
*** 1.227824708 seconds ***
Generating month metrics ...
*** 7.938639625 seconds ***
Formatting results ...
*** 0.0005992090000006556 seconds ***
Exporting results ...
*** 0.09505266699999737 seconds ***
Execution time: 52.987694084 seconds
```
***
*1 - Crear entorno virtual de python3*

*2 - Comandos a ejecutar para lanzar el proceso* 
```. venv/bin/activate``` --> ```pip install -r requirements.txt``` --> ```python main.py```
***
*3 - Para lanzar los test ejecutar ```pytest```*
***
*4 - Los test son un pequeño ejemplo para que se vea la utilización de pytest*

