# Generar-Serie-WRF
Genera una serie de una variable para un lat lon

### Parametros
| Variable     | Descripción      | Ejemplo |
|--------------|:----------------:|--------:|
| variable     | Variable de WRF  | T2      |
| fecha inicio | Del set de datos | 2018-06-31 |
| fecha fin    | Del set de datos | 2020-07-01 |
| latitud      | Del punto a extraer | -31.485245|
| longitud     | Del punto a extraer | -64.254851 |
| tag          | que identifique al punto | Córdoba |
| run          | corrida del modelo WRF | 06 o 18 |


### Ejemplo de uso:
```Bash
python generarSerie.py 'T2' '2019-06-01' '2020-07-31' '-31.420703' '-64.198286' 'Observatorio' '06'
```
