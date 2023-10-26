# incubadora-aplicaciones
Solución de ML para medir el estado de salud de las aplicaciones.

## Descripción
Este proyecto es una solución de Machine Learning que permite medir el estado de salud de las aplicaciones. Para esto tiene en cuenta diferentes fuentes de datos que aportan información sobre diferentes aspectos de una aplicación desplegada: logs, métricas, eventos, recomendaciones, etc.

## Estructura de carpetas y archivos
- [data](data): Contiene los archivos csv que se utilizan para entrenar el modelo.
- [iac](iac): Infraestructura como código desplegada para la solución.
- [Runbook](Runbook.docx): Documento donde se van condensando los hallazgos evidenciados en el proceso de construcción.
- [Analysis_Preprocessing.ipynb](Analysis_Preprocessing.ipynb): Notebook donde se realiza el análisis exploratorio de los datos y el preprocesamiento de los mismos.
- [Model_Implementation.ipynb](Model_Implementation.ipynb): Notebook donde se implementa el modelo de Machine Learning.
- El resto de notebooks son generados por el equipo de desarrollo para pruebas, análisis y demás.

## Para contribuir
Cree una rama feature con la nueva funcionalidad o mejora que se va a implementar. A través de un pull request se revisará el código y se aprobará para hacer merge con la rama main.